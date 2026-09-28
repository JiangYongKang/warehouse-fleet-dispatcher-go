package dispatcher

import (
	"sort"
	"sync"
)

// Window 是半开时间窗 [Start, End)，单位为逻辑 tick。
// 同一资源上两个窗口半开重叠即视为冲突。
type Window struct {
	Start int64
	End   int64
}

// overlaps 判断两个半开窗口是否重叠。
func (w Window) overlaps(o Window) bool {
	return w.Start < o.End && o.Start < w.End
}

// ResourceKind 区分节点占用与边（车道）占用。
type ResourceKind string

const (
	ResNode ResourceKind = "node"
	ResEdge ResourceKind = "edge"
)

// Resource 标识路网中被预约的资源。
type Resource struct {
	Kind ResourceKind
	ID   string
}

func nodeRes(id string) Resource { return Resource{Kind: ResNode, ID: id} }

// edgeRes 把有向边映射到"物理车道"资源：
// 仓库巷道是双向单车道，a->b 与 b->a 是同一段路，必须互斥占用，
// 因此资源 ID 用两端点的规范化形式（较小 ID 在前），保证对向冲突可被检测。
func edgeRes(e *Edge) Resource {
	a, b := e.From, e.To
	if b < a {
		a, b = b, a
	}
	return Resource{Kind: ResEdge, ID: a + "~" + b}
}

// Reservation 是单个机器人对单个资源在单个时间窗的占用。
// 执行模型不变量：每台机器人任意时刻至多持有一条"当前占用"。
type Reservation struct {
	RobotID string
	TaskID  string
	Res     Resource
	Win     Window
}

// ReservationTable 保存全部当前占用，提供冲突查询、写入与回收。
type ReservationTable struct {
	mu      sync.Mutex
	byRes   map[Resource][]Reservation // 按 Start 升序
	byRobot map[string][]Reservation
}

// NewReservationTable 创建空预约表。
func NewReservationTable() *ReservationTable {
	return &ReservationTable{
		byRes:   map[Resource][]Reservation{},
		byRobot: map[string][]Reservation{},
	}
}

// insertSorted 向按 Start 升序的切片插入一条预约。
func insertSorted(list []Reservation, r Reservation) []Reservation {
	idx := sort.Search(len(list), func(i int) bool { return list[i].Win.Start >= r.Win.Start })
	list = append(list, Reservation{})
	copy(list[idx+1:], list[idx:])
	list[idx] = r
	return list
}

// putLocked 写入一条预约（调用方持锁）。
func (t *ReservationTable) putLocked(r Reservation) {
	t.byRes[r.Res] = insertSorted(t.byRes[r.Res], r)
	t.byRobot[r.RobotID] = append(t.byRobot[r.RobotID], r)
}

// deleteFromResLocked 从某资源列表中删除一条占用（调用方持锁）。
func (t *ReservationTable) deleteFromResLocked(target Reservation) {
	list := t.byRes[target.Res]
	out := list[:0]
	for _, r := range list {
		if r.RobotID == target.RobotID && r.Win == target.Win && r.TaskID == target.TaskID {
			continue
		}
		out = append(out, r)
	}
	if len(out) == 0 {
		delete(t.byRes, target.Res)
	} else {
		t.byRes[target.Res] = out
	}
}

// ForcePut 写入"当前占用"。逐拍模型下每机器人至多占用一个资源，
// 因此先清掉该机器人的既有占用再写入，维护不变量。
func (t *ReservationTable) ForcePut(r Reservation) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.removeIfLocked(r.RobotID, "")
	t.putLocked(r)
}

// forcePut 是包内别名。
func (t *ReservationTable) forcePut(r Reservation) { t.ForcePut(r) }

// removeIfLocked 删除某机器人（可限定任务）的占用，返回删除条数（调用方持锁）。
func (t *ReservationTable) removeIfLocked(robotID, taskID string) int {
	removed := 0
	rs := t.byRobot[robotID]
	if taskID != "" {
		kept := rs[:0]
		for _, r := range rs {
			if r.TaskID == taskID {
				removed++
				t.deleteFromResLocked(r)
			} else {
				kept = append(kept, r)
			}
		}
		t.byRobot[robotID] = kept
	} else {
		for _, r := range rs {
			t.deleteFromResLocked(r)
		}
		removed = len(rs)
		t.byRobot[robotID] = nil
	}
	return removed
}

// ReleaseRobot 释放某机器人（可限定任务）的全部占用，返回条数。
func (t *ReservationTable) ReleaseRobot(robotID, taskID string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.removeIfLocked(robotID, taskID)
}

// ReleaseResource 释放某机器人对指定资源的占用。
func (t *ReservationTable) ReleaseResource(robotID, taskID string, res Resource) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.releaseResourceLocked(robotID, res)
}

func (t *ReservationTable) releaseResourceLocked(robotID string, res Resource) {
	list := t.byRes[res]
	out := list[:0]
	for _, r := range list {
		if r.RobotID != robotID {
			out = append(out, r)
		}
	}
	if len(out) == 0 {
		delete(t.byRes, res)
	} else {
		t.byRes[res] = out
	}
	rs := t.byRobot[robotID]
	kept := rs[:0]
	for _, r := range rs {
		if r.Res != res {
			kept = append(kept, r)
		}
	}
	t.byRobot[robotID] = kept
}

// ExtendHolds 把某机器人当前占用窗口的结束时间顺延到 end（受阻时继续占住资源）。
func (t *ReservationTable) ExtendHolds(robotID string, end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.extendHoldsLocked(robotID, end)
}

func (t *ReservationTable) extendHoldsLocked(robotID string, end int64) {
	for i := range t.byRobot[robotID] {
		if t.byRobot[robotID][i].Win.End < end {
			t.byRobot[robotID][i].Win.End = end
		}
	}
	for res, list := range t.byRes {
		for i := range list {
			if list[i].RobotID == robotID && list[i].Win.End < end {
				list[i].Win.End = end
			}
		}
		_ = res
	}
}

// HolderAt 返回在 now 时刻占用资源 res 的"其他"机器人；无人占用返回空串。
// 由于同一资源冲突的预约不可能共存，最多只有一个持有者。
func (t *ReservationTable) HolderAt(res Resource, now int64, excludeRobot string) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.holderAtLocked(res, now, excludeRobot)
}

func (t *ReservationTable) holderAtLocked(res Resource, now int64, excludeRobot string) string {
	for _, r := range t.byRes[res] {
		if r.RobotID == excludeRobot {
			continue
		}
		if r.Win.Start <= now && now < r.Win.End {
			return r.RobotID
		}
	}
	return ""
}

// AllReservations 返回全部当前占用（顺序确定：资源键升序、再按开始时间、机器人）。
func (t *ReservationTable) AllReservations() []Reservation {
	t.mu.Lock()
	defer t.mu.Unlock()
	var out []Reservation
	for _, list := range t.byRes {
		out = append(out, list...)
	}
	sort.Slice(out, func(i, j int) bool {
		ki, kj := resKey(out[i].Res), resKey(out[j].Res)
		if ki != kj {
			return ki < kj
		}
		if out[i].Win.Start != out[j].Win.Start {
			return out[i].Win.Start < out[j].Win.Start
		}
		return out[i].RobotID < out[j].RobotID
	})
	return out
}

// resKey 给出资源的可排序字符串。
func resKey(r Resource) string { return string(r.Kind) + ":" + r.ID }
