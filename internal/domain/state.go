package domain

import "sort"

// CurrentStateVersion 当前快照格式版本。
const CurrentStateVersion = 3

// NewState 构造空状态。
func NewState() *State {
	return &State{
		Version:   CurrentStateVersion,
		Robots:    map[string]*Robot{},
		Tasks:     map[string]*Task{},
		Plans:     map[string]*Plan{},
		IdemIndex: map[string]string{},
		Extra:     map[string]interface{}{},
	}
}

// Clone 深拷贝状态（供只读快照使用）。
func (s *State) Clone() *State {
	cp := *s
	cp.Robots = make(map[string]*Robot, len(s.Robots))
	for k, v := range s.Robots {
		rv := *v
		rv.Caps = append([]string(nil), v.Caps...)
		cp.Robots[k] = &rv
	}
	cp.Tasks = make(map[string]*Task, len(s.Tasks))
	for k, v := range s.Tasks {
		tv := *v
		tv.Deps = append([]string(nil), v.Deps...)
		tv.Requires = append([]string(nil), v.Requires...)
		if v.Result != nil {
			r := *v.Result
			tv.Result = &r
		}
		cp.Tasks[k] = &tv
	}
	cp.Plans = make(map[string]*Plan, len(s.Plans))
	for k, v := range s.Plans {
		pv := *v
		pv.Path = append([]string(nil), v.Path...)
		pv.Victims = append([]string(nil), v.Victims...)
		cp.Plans[k] = &pv
	}
	cp.Reservations = append([]Reservation(nil), s.Reservations...)
	cp.Decisions = append([]Decision(nil), s.Decisions...)
	cp.IdemIndex = map[string]string{}
	for k, v := range s.IdemIndex {
		cp.IdemIndex[k] = v
	}
	return &cp
}

// AddDecision 追加一条审计判定。
func (s *State) AddDecision(kind, detail string) {
	s.Decisions = append(s.Decisions, Decision{At: s.Clock, Kind: kind, Detail: detail})
}

// ActiveReservations 过滤掉已完全过期的时间窗（仅用于清理，判定时仍直接查全量）。
func (s *State) PrunePast(tick int64) {
	out := s.Reservations[:0]
	for _, r := range s.Reservations {
		if r.End != SentinelForever && r.End <= tick {
			continue
		}
		out = append(out, r)
	}
	s.Reservations = out
}

// Overlapping 返回与资源 resource 的窗口 [start,end) 重叠的预约；
// excludeRobot 指定的机器人自身预约被忽略。
func (s *State) Overlapping(resource string, start, end int64, excludeRobot string) []Reservation {
	var hit []Reservation
	for _, r := range s.Reservations {
		if r.Resource != resource || r.RobotID == excludeRobot {
			continue
		}
		if WindowsOverlap(start, end, r.Start, r.End) {
			hit = append(hit, r)
		}
	}
	return hit
}

// WindowsOverlap 半开时间窗重叠判定（端点相接不算重叠）。
func WindowsOverlap(a0, a1, b0, b1 int64) bool {
	return a0 < b1 && b0 < a1
}

// HolderAt 返回某资源在 tick 时刻的占用者（确定性：按 RobotID 排序取第一个）。
func (s *State) HolderAt(resource string, tick int64, excludeRobot string) (Reservation, bool) {
	rs := s.Overlapping(resource, tick, tick+1, excludeRobot)
	if len(rs) == 0 {
		return Reservation{}, false
	}
	sort.Slice(rs, func(i, j int) bool { return rs[i].RobotID < rs[j].RobotID })
	return rs[0], true
}

// RobotReservations 返回某机器人的全部有效（未过期）预约。
func (s *State) RobotReservations(robotID string, tick int64) []Reservation {
	var out []Reservation
	for _, r := range s.Reservations {
		if r.RobotID != robotID {
			continue
		}
		if r.End == SentinelForever || r.End > tick {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Start != out[j].Start {
			return out[i].Start < out[j].Start
		}
		return out[i].Resource < out[j].Resource
	})
	return out
}

// ReleaseReservations 删除某机器人的全部预约（被抢占/掉线时回收）。
func (s *State) ReleaseReservations(robotID string) int {
	out := s.Reservations[:0]
	n := 0
	for _, r := range s.Reservations {
		if r.RobotID == robotID {
			n++
			continue
		}
		out = append(out, r)
	}
	s.Reservations = out
	return n
}

// ReleaseNodeAtDeparture 机器人离开节点时把节点占位窗结束时间设为 tick（半开交接）。
func (s *State) ReleaseNodeAtDeparture(robotID, node string, tick int64) {
	for i := range s.Reservations {
		r := &s.Reservations[i]
		if r.RobotID == robotID && r.Resource == KeyNode(node) && r.End == SentinelForever {
			r.End = tick
		}
	}
}

// PositionAt 依据计划返回机器人在 tick 时刻所在位置（节点 ID；移动中返回当前边起点）。
func (s *State) PositionAt(robotID string, tick int64) string {
	r := s.Robots[robotID]
	if r == nil {
		return ""
	}
	if r.CurrentTask == "" {
		return r.Node
	}
	p := s.Plans[r.CurrentTask]
	if p == nil {
		return r.Node
	}
	if p.State == PlanMoving && tick < p.EdgeUntil {
		return p.Path[p.Idx]
	}
	return r.Node
}
