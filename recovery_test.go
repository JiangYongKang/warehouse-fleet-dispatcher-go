package dispatcher_test

import (
	"os"
	"path/filepath"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
)

// TestOfflineReassign 机器人执行途中掉线：任务安全回收并被其他机器人完成，
// 预约全部释放；重新上线后可继续工作。
func TestOfflineReassign(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterRobot("r2", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "t1", From: "A", To: "C", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	// 推进到 r1 在途中（tick 2 应在边 A~B 上），然后让 r1 掉线。
	if _, err := eng.AssignOnce(1); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.AssignOnce(2); err != nil {
		t.Fatal(err)
	}
	r1, _ := eng.RobotSnapshot("r1")
	if r1.CurrentTask != "t1" {
		t.Fatalf("r1 should be executing t1, got %q", r1.CurrentTask)
	}
	if err := eng.MarkOffline("r1"); err != nil {
		t.Fatal(err)
	}
	ts, _ := eng.TaskSnapshot("t1")
	if ts.Status != dispatcher.TaskPending || ts.Assignee != "" {
		t.Fatalf("task must be requeued after offline, got %s/%s", ts.Status, ts.Assignee)
	}
	if n := len(eng.Reservations()); false {
		_ = n
	}
	// r1 的预约应已释放（只剩 r2 对 A 的占用）。
	for _, rv := range eng.Reservations() {
		if rv.RobotID == "r1" {
			t.Fatalf("offline robot r1 still holds %s %s", rv.Res.Kind, rv.Res.ID)
		}
	}
	t.Log("decision: r1 offline mid-route -> t1 pending, r1 reservations released")

	// 后续拍由 r2 接手完成；掉线机器人的迟到上报必须被拒绝（stale）。
	mustCode(t, eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: true}), dispatcher.ErrStaleResult)
	for tk := int64(3); tk <= 30; tk++ {
		if _, err := eng.AssignOnce(tk); err != nil {
			t.Fatal(err)
		}
		autoReport(t, eng, true, "")
	}
	ts, _ = eng.TaskSnapshot("t1")
	if ts.Status != dispatcher.TaskCompleted || ts.Assignee != "r2" {
		t.Fatalf("t1 should be completed by r2, got %s/%s", ts.Status, ts.Assignee)
	}
	if ts.Attempt != 0 {
		t.Fatalf("offline reassign must not count as execution failure, Attempt=%d", ts.Attempt)
	}
	t.Logf("decision: t1 picked up and completed by r2 (dispatch=%d), no failure counted", ts.Dispatch)

	// r1 重新上线，应可再次接活。
	if err := eng.MarkOnline("r1"); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "t2", From: "A", To: "D", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	for tk := int64(31); tk <= 60; tk++ {
		if _, err := eng.AssignOnce(tk); err != nil {
			t.Fatal(err)
		}
		autoReport(t, eng, true, "")
	}
	t2, _ := eng.TaskSnapshot("t2")
	if t2.Status != dispatcher.TaskCompleted {
		t.Fatalf("t2 after r1 online: %s", t2.Status)
	}
	t.Logf("decision: r1 back online, t2 completed by %s", t2.Assignee)

	// 重复下线/上线是非法流转，要给可区分原因。
	if err := eng.MarkOffline("r2"); err != nil {
		t.Fatal(err)
	}
	mustCode(t, eng.MarkOffline("r2"), dispatcher.ErrInvalidState)
	if err := eng.MarkOnline("r2"); err != nil {
		t.Fatal(err)
	}
	mustCode(t, eng.MarkOnline("r2"), dispatcher.ErrInvalidState)
}

// TestRestartRecovery 落盘 -> 关闭 -> 用同文件新引擎恢复 -> assigned 回收、终态保持。
func TestRestartRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "snap.json")

	// 第一轮：完成 t1；在 t2 已分派但仍在途的时刻关闭（模拟宕机在途中）。
	// 时间线：r1 做 t1（A->C，tick1~5 到达并上报）；
	// r2 的 t2（依赖 t1）tick6 从 S 出发，S->A 两拍、A->D 两拍，tick10 到达。
	// 因此 tick8 时 r2 正在边 A~D 上（t2=assigned 在途），此刻关闭。
	func() {
		eng := newEngine(t, gridGraph(t), dispatcher.NewFileStore(path))
		if err := eng.RegisterRobot("r1", "A"); err != nil {
			t.Fatal(err)
		}
		if err := eng.RegisterRobot("r2", "S"); err != nil {
			t.Fatal(err)
		}
		if err := eng.SubmitTask(dispatcher.Task{ID: "t1", From: "A", To: "C", Priority: 1}); err != nil {
			t.Fatal(err)
		}
		if err := eng.SubmitTask(dispatcher.Task{ID: "t2", From: "S", To: "D", Priority: 2, Deps: []string{"t1"}}); err != nil {
			t.Fatal(err)
		}
		for tk := int64(1); tk <= 8; tk++ {
			if _, err := eng.AssignOnce(tk); err != nil {
				t.Fatal(err)
			}
			autoReport(t, eng, true, "")
		}
		t2, _ := eng.TaskSnapshot("t2")
		if t2.Status != dispatcher.TaskAssigned {
			t.Fatalf("test setup: t2 should be in-flight assigned at tick 8, got %s", t2.Status)
		}
		r2, _ := eng.RobotSnapshot("r2")
		t.Logf("input at shutdown: t2=%s assignee=%s r2.node=%s current=%s",
			t2.Status, t2.Assignee, r2.Node, r2.CurrentTask)
		if err := eng.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: snapshot file size=%d bytes (atomic write)", info.Size())

	// 第二轮：重建相同路网并恢复。
	eng2 := newEngine(t, gridGraph(t), dispatcher.NewFileStore(path))
	if err := eng2.Recover(); err != nil {
		t.Fatal(err)
	}
	t1, _ := eng2.TaskSnapshot("t1")
	if t1.Status != dispatcher.TaskCompleted {
		t.Fatalf("completed task must remain completed after restart, got %s", t1.Status)
	}
	t2, _ := eng2.TaskSnapshot("t2")
	if t2.Status != dispatcher.TaskPending {
		t.Fatalf("in-flight assigned task must be requeued to pending on restart, got %s", t2.Status)
	}
	if t2.Assignee != "" {
		t.Fatalf("requeued task must have no assignee, got %q", t2.Assignee)
	}
	// 恢复后时钟从宕机时的 tick 8 继续，t2 回收后重新规划并完成。
	if eng2.Now() != 8 {
		t.Fatalf("clock should restore to 8, got %d", eng2.Now())
	}
	for tk := int64(9); tk <= 60; tk++ {
		if _, err := eng2.AssignOnce(tk); err != nil {
			t.Fatal(err)
		}
		autoReport(t, eng2, true, "")
	}
	t2, _ = eng2.TaskSnapshot("t2")
	if t2.Status != dispatcher.TaskCompleted {
		t.Fatalf("t2 should complete after recovery, got %s", t2.Status)
	}
	t.Logf("decision: t1 stays completed (never re-executed), t2 requeued then completed by %s post-restart", t2.Assignee)
}

// TestReplayDeterminism 同样的路网 + 同样的操作序列，两次运行结果必须逐任务一致。
func TestReplayDeterminism(t *testing.T) {
	run := func(seed int) []string {
		eng := newEngine(t, gridGraph(t), &dispatcher.MemoryStore{})
		for _, id := range []string{"r1", "r2", "r3"} {
			if err := eng.RegisterRobot(id, "A"); err != nil {
				t.Fatal(err)
			}
		}
		// 固定顺序提交 12 个任务（优先级循环），依赖一个公共前驱。
		if err := eng.SubmitTask(dispatcher.Task{ID: "base", From: "A", To: "D", Priority: 3}); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 12; i++ {
			id := "t" + itoa(i)
			if err := eng.SubmitTask(dispatcher.Task{
				ID: id, From: "A", To: "C", Priority: (i + seed) % 4, Deps: []string{"base"},
			}); err != nil {
				t.Fatal(err)
			}
		}
		for tk := int64(1); tk <= 200; tk++ {
			if _, err := eng.AssignOnce(tk); err != nil {
				t.Fatal(err)
			}
			autoReport(t, eng, true, "")
		}
		var out []string
		for _, ts := range eng.Tasks() {
			out = append(out, ts.ID+":"+string(ts.Status)+":"+ts.Assignee+":"+itoa(ts.Dispatch))
		}
		return out
	}
	a := run(0)
	b := run(0)
	if len(a) != len(b) {
		t.Fatalf("length mismatch %d vs %d", len(a), len(b))
	}
	for i := range a {
		if a[i] != b[i] {
			t.Fatalf("non-deterministic replay at index %d:\n run1=%s\n run2=%s", i, a[i], b[i])
		}
	}
	t.Logf("decision: two replays produced identical %d task outcomes (status/assignee/dispatch)", len(a))
}

// TestSnapshotV1Compatibility v1 历史快照（无 format、复合资源串、任务无 seq/wait_tick）
// 必须能被当前内核读取并正确迁移，且 assigned 同样回收。
func TestSnapshotV1Compatibility(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "v1.json")
	v1 := `{
	  "clock": 7,
	  "robots": [
	    {"id":"r1","status":"online","node":"A","task":"done"},
	    {"id":"r2","status":"offline","node":"S"}
	  ],
	  "tasks": [
	    {"id":"done","priority":2,"from":"A","to":"C","status":"completed","assignee":"r1","attempt":1},
	    {"id":"fly","priority":1,"from":"S","to":"A","status":"assigned","assignee":"r2","attempt":1}
	  ],
	  "reservations": [
	    {"robot_id":"r1","task_id":"done","res":"node:A","start":6,"end":7},
	    {"robot_id":"r2","task_id":"fly","res":"edge:S->A","start":6,"end":7}
	  ]
	}`
	if err := os.WriteFile(path, []byte(v1), 0o644); err != nil {
		t.Fatal(err)
	}
	eng := newEngine(t, gridGraph(t), dispatcher.NewFileStore(path))
	if err := eng.Recover(); err != nil {
		t.Fatalf("v1 snapshot should migrate and load: %v", err)
	}
	done, _ := eng.TaskSnapshot("done")
	if done.Status != dispatcher.TaskCompleted || done.Assignee != "r1" {
		t.Fatalf("v1 completed task migrated wrong: %+v", done)
	}
	if done.Seq != 0 {
		t.Fatalf("v1 first task seq should be backfilled to 0, got %d", done.Seq)
	}
	fly, _ := eng.TaskSnapshot("fly")
	if fly.Status != dispatcher.TaskPending || fly.Assignee != "" {
		t.Fatalf("v1 assigned task must be requeued, got %+v", fly)
	}
	r2, _ := eng.RobotSnapshot("r2")
	if r2.Status != dispatcher.RobotOffline {
		t.Fatalf("r2 should remain offline, got %s", r2.Status)
	}
	t.Log("decision: v1 snapshot (compound resource ids, no seq/wait_tick) migrated; completed kept, assigned requeued, offline kept")

	// 恢复后再次落盘应为 v2，且可重复加载（往返一致）。
	for tk := int64(8); tk <= 12; tk++ {
		if err := eng.MarkOnline("r2"); err != nil { // 幂等：只第一次成功
			if dispatcher.ErrorCodeOf(err) != dispatcher.ErrInvalidState {
				t.Fatal(err)
			}
		}
		if err := eng.AdvanceClock(tk); err != nil {
			t.Fatal(err)
		}
		autoReport(t, eng, true, "")
	}
	if err := eng.Close(); err != nil {
		t.Fatal(err)
	}
	eng2 := newEngine(t, gridGraph(t), dispatcher.NewFileStore(path))
	if err := eng2.Recover(); err != nil {
		t.Fatalf("re-saved snapshot must reload: %v", err)
	}
}

// TestUnknownFieldsTolerated 当前版本写出的快照若被未来增加未知字段，仍应可被当前版本读取。
func TestUnknownFieldsTolerated(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "future.json")
	future := `{
	  "format": "fleet-snapshot/v2",
	  "clock": 3,
	  "seq": 1,
	  "future_top_level": 42,
	  "robots": [{"id":"r1","status":"online","node":"A","battery":0.7}],
	  "tasks": [{"id":"t1","priority":1,"from":"A","to":"B","status":"pending","seq":1,"tags":["x"]}],
	  "reservations": []
	}`
	if err := os.WriteFile(path, []byte(future), 0o644); err != nil {
		t.Fatal(err)
	}
	eng := newEngine(t, simpleGraph(t), dispatcher.NewFileStore(path))
	if err := eng.Recover(); err != nil {
		t.Fatalf("unknown future fields must be ignored: %v", err)
	}
	if _, err := eng.TaskSnapshot("t1"); err != nil {
		t.Fatal(err)
	}
	t.Log("decision: unknown fields (battery, tags, future_top_level) ignored, snapshot still loads")
}

func simpleGraph(t *testing.T) *dispatcher.Graph {
	t.Helper()
	g := dispatcher.NewGraph()
	for _, n := range []string{"A", "B"} {
		if err := g.AddNode(dispatcher.Node{ID: n}); err != nil {
			t.Fatal(err)
		}
	}
	if err := g.AddEdge(dispatcher.Edge{From: "A", To: "B", Length: 1}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(dispatcher.Edge{From: "B", To: "A", Length: 1}); err != nil {
		t.Fatal(err)
	}
	return g
}
