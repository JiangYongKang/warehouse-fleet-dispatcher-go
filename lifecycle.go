package dispatcher

import (
	"context"
	"sort"
	"sync"
)

// Option 配置引擎。
type Option func(*Engine)

// WithStore 设置持久化存储（默认纯内存、不落盘）。
func WithStore(st Store) Option {
	return func(e *Engine) { e.store = st }
}

// WithLogger 设置日志输出（默认丢弃；测试中可桥接 testing.T）。
func WithLogger(l Logger) Option {
	return func(e *Engine) { e.log = l }
}

// WithoutBackground 禁用后台分派循环，内核只在显式调用
// AssignOnce/AdvanceClock/提交/上报时同步推进。测试使用它以获得完全确定的时序，
// 避免测试代码与后台 goroutine 在同一 tick 上竞态。
func WithoutBackground() Option {
	return func(e *Engine) { e.noBackground = true }
}

// Logger 是最小日志接口。
type Logger interface {
	Printf(format string, args ...any)
}

// discardLogger 默认静默。
type discardLogger struct{}

func (discardLogger) Printf(string, ...any) {}

// Engine 是调度内核。
type Engine struct {
	mu    sync.Mutex
	wg    sync.WaitGroup
	graph *Graph
	rt    *ReservationTable
	clock *Clock
	store Store
	log   Logger

	robots map[string]*Robot
	tasks  map[string]*TaskState
	seq    int64

	// exec 是在线执行进度（机器人 -> 任务执行状态）；
	// lastTick 记录上一次运动推进的 tick，同 tick 重复调用不重复推进。
	exec     map[string]*execution
	lastTick int64

	closed bool

	// resultLog 记录已到终态任务的结果指纹，保证重复上报幂等。
	resultLog map[string]resultRecord

	assignCh chan struct{}
	cancel   context.CancelFunc

	noBackground bool // 测试用：不启动后台 goroutine，纯显式驱动
}

// resultRecord 是结果上报的去重指纹。
type resultRecord struct {
	taskID  string
	robotID string
	ok      bool
	attempt int
	seq     int64
}

// NewEngine 创建调度内核；传入 store 时尝试从快照恢复。
func NewEngine(graph *Graph, opts ...Option) (*Engine, error) {
	if graph == nil {
		return nil, fail("NewEngine", ErrInvalidArgument, "graph is nil")
	}
	e := &Engine{
		graph:     graph,
		rt:        NewReservationTable(),
		clock:     NewClock(),
		log:       discardLogger{},
		robots:    map[string]*Robot{},
		tasks:     map[string]*TaskState{},
		exec:      map[string]*execution{},
		lastTick:  -1,
		resultLog: map[string]resultRecord{},
		assignCh:  make(chan struct{}, 1),
	}
	for _, o := range opts {
		o(e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	e.cancel = cancel
	if !e.noBackground {
		e.wg.Add(1)
		go e.bgLoop(ctx)
	}
	return e, nil
}

// Close 停止后台循环并做最后一次持久化；可安全重复调用。
func (e *Engine) Close() error {
	e.mu.Lock()
	if e.closed {
		e.mu.Unlock()
		return nil
	}
	e.closed = true
	cancel := e.cancel
	perr := e.persistLocked()
	e.mu.Unlock()
	cancel()
	e.wg.Wait()
	return perr
}

// bgLoop 是后台分派循环：被 kick 唤醒后执行一轮分派与死锁检测。
// 它不持有任何锁等待，与手动 AssignOnce 共用同一段加锁逻辑。
func (e *Engine) bgLoop(ctx context.Context) {
	defer e.wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case <-e.assignCh:
			e.mu.Lock()
			now := e.clock.Now()
			assigned, err := e.assignLocked(now)
			if err != nil {
				e.logf("bg assign error at tick %d: %v", now, err)
			} else if len(assigned) > 0 {
				e.logf("bg assigned tasks %v at tick %d", assigned, now)
			}
			if err := e.detectAndBreakLocked(now); err != nil {
				e.logf("bg deadlock check error: %v", err)
			}
			if perr := e.persistLocked(); perr != nil {
				e.logf("bg persist error: %v", perr)
			}
			e.mu.Unlock()
		}
	}
}

// kick 非阻塞地通知后台分派循环（信号合并：容量 1）。
func (e *Engine) kick() {
	select {
	case e.assignCh <- struct{}{}:
	default:
	}
}

// logf 输出一条带 tick 前缀的判定日志。
func (e *Engine) logf(format string, args ...any) {
	if e.log == nil {
		return
	}
	e.log.Printf("[tick %d] %s", e.clock.Now(), fmtSprintf(format, args...))
}

// Now 返回逻辑时钟当前 tick。
func (e *Engine) Now() int64 { return e.clock.Now() }

// AdvanceClock 推进逻辑时钟，随后同步执行一轮分派与死锁检测并落盘。
func (e *Engine) AdvanceClock(t int64) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return fail("Engine.AdvanceClock", ErrInvalidState, "engine closed")
	}
	cur := e.clock.Now()
	if t < cur {
		return fail("Engine.AdvanceClock", ErrInvalidArgument,
			"cannot move clock backwards: %d < %d", t, cur)
	}
	e.clock.Advance(t)
	e.logf("clock advanced %d -> %d", cur, t)
	if _, err := e.assignLocked(t); err != nil {
		return err
	}
	if err := e.detectAndBreakLocked(t); err != nil {
		return err
	}
	return e.persistLocked()
}

// ---- 只读查询 ----

// Robots 返回全部机器人状态（按 ID 升序）。
func (e *Engine) Robots() []RobotState {
	e.mu.Lock()
	defer e.mu.Unlock()
	ids := make([]string, 0, len(e.robots))
	for id := range e.robots {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]RobotState, 0, len(ids))
	for _, id := range ids {
		out = append(out, e.robots[id].Snapshot())
	}
	return out
}

// RobotSnapshot 返回单个机器人状态。
func (e *Engine) RobotSnapshot(id string) (RobotState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.robots[id]
	if !ok {
		return RobotState{}, fail("Engine.RobotSnapshot", ErrNotFound, "robot %q not found", id)
	}
	return r.Snapshot(), nil
}

// Tasks 返回全部任务状态（按 Seq 升序，Seq 相同按 ID）。
func (e *Engine) Tasks() []TaskState {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]TaskState, 0, len(e.tasks))
	for _, t := range e.tasks {
		out = append(out, t.TaskSnapshot())
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Seq != out[j].Seq {
			return out[i].Seq < out[j].Seq
		}
		return out[i].ID < out[j].ID
	})
	return out
}

// TaskSnapshot 返回单个任务状态。
func (e *Engine) TaskSnapshot(id string) (TaskState, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	t, ok := e.tasks[id]
	if !ok {
		return TaskState{}, fail("Engine.TaskSnapshot", ErrNotFound, "task %q not found", id)
	}
	return t.TaskSnapshot(), nil
}

// Reservations 返回当前全部预约（顺序确定）。
func (e *Engine) Reservations() []Reservation {
	return e.rt.AllReservations()
}

// IsAwaitingResult 判断机器人执行的任务是否已走完路径、正在等待结果上报。
// 供调用方决定何时可以上报，避免"刚重派仍停在旧终点"的误判。
func (e *Engine) IsAwaitingResult(robotID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	ex := e.exec[robotID]
	return ex != nil && ex.arrived
}

// AssignOnce 对外的单轮分派入口。
func (e *Engine) AssignOnce(now int64) ([]string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed {
		return nil, fail("Engine.AssignOnce", ErrInvalidState, "engine closed")
	}
	if now < e.clock.Now() {
		return nil, fail("Engine.AssignOnce", ErrInvalidArgument,
			"tick %d is in the past (now %d)", now, e.clock.Now())
	}
	e.clock.Advance(now)
	assigned, err := e.assignLocked(now)
	if err != nil {
		return nil, err
	}
	if err := e.detectAndBreakLocked(now); err != nil {
		return nil, err
	}
	if err := e.persistLocked(); err != nil {
		return nil, err
	}
	return assigned, nil
}
