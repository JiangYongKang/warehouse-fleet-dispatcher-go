package dispatchtest

import (
	"strings"

	"context"
	"fmt"
	"sync"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
)

// TestConcurrentSubmitNoDuplicateExecution 多 goroutine 用同一幂等键并发提交：
// 只能产生一个任务，并且只被执行一次。
func TestConcurrentSubmitNoDuplicateExecution(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")

	const n = 20
	var wg sync.WaitGroup
	errs := make(chan error, n)
	tasks := make(chan string, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tk, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
				IdemKey: "race-key", Priority: 3, From: "A", To: "F",
			})
			errs <- err
			tasks <- tk.ID
		}()
	}
	wg.Wait()
	close(errs)
	close(tasks)

	dup := 0
	ok := 0
	ids := map[string]int{}
	for e := range errs {
		if e == nil {
			ok++
		} else if dispatcher.IsCode(e, domain.CodeDuplicateSubmit) {
			dup++
		} else {
			t.Fatalf("unexpected submit error: %v", e)
		}
	}
	for id := range tasks {
		ids[id]++
	}
	if ok != 1 || dup != n-1 {
		t.Fatalf("submit: accepted=%d dup=%d, want 1/%d", ok, dup, n-1)
	}
	if len(ids) != 1 {
		t.Fatalf("distinct task ids=%v, want exactly 1", ids)
	}
	t.Logf("result: %d concurrent submits -> 1 task id accepted, %d duplicate_submit", ok, dup)

	tickAll(t, k, []string{"r1"}, 8)
	var completed int
	for _, tk := range k.Snapshot().Tasks {
		if tk.Status == domain.TaskCompleted {
			completed++
			if tk.Attempts != 1 {
				t.Fatalf("task %s attempts=%d, want 1", tk.ID, tk.Attempts)
			}
		}
	}
	if completed != 1 {
		t.Fatalf("completed tasks=%d, want exactly 1", completed)
	}
	t.Log("result: task executed exactly once under concurrent submission")
}

// TestConcurrentMixedOperations 混合并发：提交、心跳、上报、读快照与时钟推进
// 交替进行。要求无数据竞争、无 panic，所有任务最终进入合法终态且完成任务尝试次数>=1。
// 时钟推进由主 goroutine 独占（保证确定性），其余 goroutine 只并发发起请求。
func TestConcurrentMixedOperations(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r2", "B")
	addRobot(t, k, "r3", "D")
	addRobot(t, k, "r4", "E")

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 心跳器
	for _, r := range []string{"r1", "r2", "r3", "r4"} {
		r := r
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_ = k.Heartbeat(ctx, r)
				}
			}
		}()
	}
	// 上报器：只有到达终点才成功，其余被安全拒绝。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				st := k.Snapshot()
				for id, r := range st.Robots {
					if r.Status == domain.RobotExecuting && r.CurrentTask != "" {
						_, _ = k.ReportResult(ctx, r.CurrentTask, id, domain.TaskResult{Success: true})
					}
				}
			}
		}
	}()
	// 读取器
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
				_ = k.Snapshot()
				_ = k.Decisions(10)
			}
		}
	}()

	// 提交器与主推进循环在同一 goroutine 顺序交错（提交是请求方语义，
	// 但为保证终态确定性由主循环驱动）。
	pairs := [][2]string{
		{"A", "F"}, {"F", "A"}, {"D", "C"}, {"C", "D"},
		{"A", "E"}, {"E", "A"}, {"D", "F"}, {"F", "D"},
	}
	submitted := 0
	for tick := 0; tick < 400; tick++ {
		if submitted < 32 {
			pr := pairs[submitted%len(pairs)]
			_, _ = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
				IdemKey: fmt.Sprintf("mix-%d", submitted), Priority: submitted % 5,
				From: pr[0], To: pr[1],
			})
			submitted++
		}
		if err := k.Advance(ctx, 1); err != nil {
			t.Fatal(err)
		}
		st := k.Snapshot()
		terminal := 0
		for _, tk := range st.Tasks {
			if tk.Status.IsTerminal() {
				terminal++
			}
		}
		if submitted == 32 && terminal == 32 {
			break
		}
	}
	close(stop)
	wg.Wait()

	st := k.Snapshot()
	if len(st.Tasks) != 32 {
		t.Fatalf("tasks=%d want 32", len(st.Tasks))
	}
	completed, failed := 0, 0
	for id, tk := range st.Tasks {
		switch tk.Status {
		case domain.TaskCompleted:
			completed++
			if tk.Attempts < 1 {
				t.Fatalf("task %s completed with attempts=%d", id, tk.Attempts)
			}
		case domain.TaskFailed:
			failed++
		default:
			pl := st.Plans[id]
			rb := st.Robots[tk.AssignedTo]
			last := ""
			for _, dd := range k.Decisions(0) {
				if strings.Contains(dd.Detail, id) {
					last = dd.Kind + ": " + dd.Detail
				}
			}
			t.Fatalf("task %s non-terminal status=%s assigned=%s plan=%+v robot=%+v last=%s",
				id, tk.Status, tk.AssignedTo, pl, rb, last)
		}
	}
	if completed == 0 {
		t.Fatal("no task completed")
	}
	t.Logf("result: mixed concurrency finished: tasks=%d completed=%d failed=%d clock=%d",
		len(st.Tasks), completed, failed, st.Clock)
}

// TestConcurrentIdempotentReports 同一任务被多个 goroutine 同时上报成功：
// 只有一个生效，其余得到 invalid_state。
func TestConcurrentIdempotentReports(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")
	tk, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "rep", From: "A", To: "F"})
	must2(t, err)
	// 手动推进（不自动上报），使 r1 到达终点停在 executing。
	for i := 0; i < 6; i++ {
		must2(t, k.Heartbeat(ctx, "r1"))
		must2(t, k.Advance(ctx, 1))
	}
	if rb, _ := k.Robot("r1"); rb.Status != domain.RobotExecuting {
		t.Fatalf("precondition: r1 status=%s node=%s", rb.Status, rb.Node)
	}

	const n = 16
	var wg sync.WaitGroup
	var mu sync.Mutex
	success, rejected := 0, 0
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := k.ReportResult(ctx, tk.ID, "r1", domain.TaskResult{Success: true, Note: "x"})
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				success++
			} else if dispatcher.IsCode(err, domain.CodeInvalidState) {
				rejected++
			}
		}()
	}
	wg.Wait()
	if success != 1 || rejected != n-1 {
		t.Fatalf("reports success=%d rejected=%d, want 1/%d", success, rejected, n-1)
	}
	t.Logf("result: %d concurrent reports -> 1 success, %d invalid_state", success, rejected)
}

func containsStr(s, sub string) bool {
	return strings.Contains(s, sub)
}
