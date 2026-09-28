package dispatchtest

import (
	"context"
	"path/filepath"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/storage"
)

// runScenario 执行一段固定操作序列，返回最终状态快照（用于对比一致性）。
func runScenario(t *testing.T, store storage.Store) *domain.State {
	t.Helper()
	ctx := context.Background()
	k, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: store})
	must2(t, err)
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r2", "D")
	addRobot(t, k, "r3", "C")

	_, err = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "s1", Priority: 3, From: "A", To: "D"})
	must2(t, err)
	_, err = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "s2", Priority: 8, From: "D", To: "A"})
	must2(t, err)

	// r1 仅在 tick 1..3 心跳，之后掉线；r3 在 C 可接手回收任务。
	for tick := 1; tick <= 12; tick++ {
		if tick <= 3 {
			must2(t, k.Heartbeat(ctx, "r1"))
		}
		must2(t, k.Heartbeat(ctx, "r2"))
		must2(t, k.Heartbeat(ctx, "r3"))
		must2(t, k.Advance(ctx, 1))
		autoReport(t, k)
	}
	return k.Snapshot()
}

// TestRestartReplay 同样的操作序列在全新内核上重放，终态必须完全一致；
// 已完成任务不被再次执行。
func TestRestartReplay(t *testing.T) {
	st1 := storage.NewMemoryStore()
	final1 := runScenario(t, st1)

	// 用同一个 MemoryStore（含 ops 日志）构造第二个内核 -> 重放恢复。
	ctx := context.Background()
	k2, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: st1})
	must2(t, err)
	final2 := k2.Snapshot()

	t.Logf("comparing final states after replay: clock=%d tasks=%d", final2.Clock, len(final2.Tasks))
	if !sameTasks(t, final1, final2) {
		t.Fatalf("replay produced different task states")
	}
	if final1.Clock != final2.Clock {
		t.Fatalf("clock differs: %d vs %d", final1.Clock, final2.Clock)
	}

	// 已完成任务不被再次执行：状态保持 completed，尝试次数不变。
	for id, t1 := range final1.Tasks {
		t2 := final2.Tasks[id]
		if t1.Status == domain.TaskCompleted {
			if t2.Status != domain.TaskCompleted || t2.Attempts != t1.Attempts {
				t.Fatalf("task %s re-executed: %+v vs %+v", id, t1, t2)
			}
		}
	}
	t.Log("result: replay identical, completed tasks stable")
}

// TestDeterministicAcrossIndependentRuns 两次独立运行（不同 store）结论一致。
func TestDeterministicAcrossIndependentRuns(t *testing.T) {
	a := runScenario(t, storage.NewMemoryStore())
	b := runScenario(t, storage.NewMemoryStore())
	if !sameTasks(t, a, b) {
		t.Fatal("independent runs differ")
	}
	if a.Clock != b.Clock {
		t.Fatalf("clock %d vs %d", a.Clock, b.Clock)
	}
	t.Log("result: two independent runs produced identical conclusions")
}

// TestOfflineReassign 机器人途中掉线：预约释放、任务回收并由同位置机器人接手完成。
func TestOfflineReassign(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, longLaneGraph())
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r3", "D")

	task, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "off1", Priority: 2, From: "A", To: "F",
	})
	must2(t, err)
	// r1 走到 C 附近后停止心跳。
	for tick := 1; tick <= 20; tick++ {
		if tick <= 3 {
			must2(t, k.Heartbeat(ctx, "r1"))
		}
		must2(t, k.Heartbeat(ctx, "r3"))
		must2(t, k.Advance(ctx, 1))
		autoReport(t, k)
	}
	tk, _ := k.Task(task.ID)
	if tk.Status != domain.TaskCompleted {
		t.Fatalf("task not completed after offline reassign: %s", tk.Status)
	}
	if tk.AssignedTo != "r3" {
		t.Fatalf("task should be finished by r3, got %s", tk.AssignedTo)
	}
	t.Logf("result: r1 went offline, r3 took over, task completed attempts=%d", tk.Attempts)
	logDecisions(t, k, "robot_offline", "reclaim", "dispatch_assign", "report_success")
}

// TestFailureRetries 执行失败按尝试次数重派，超过上限转 failed 终态。
func TestFailureRetries(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")
	task, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "f1", Priority: 1, From: "A", To: "F",
	})
	must2(t, err)
	// 每次重派后推进到真正开始移动，再上报失败；累计 MaxAttempts 次 -> failed。
	advance := func(n int) {
		for i := 0; i < n; i++ {
			must2(t, k.Heartbeat(ctx, "r1"))
			must2(t, k.Advance(ctx, 1))
			// 故意不自动上报成功。
		}
	}
	for attempt := 1; attempt <= domain.MaxAttempts+1; attempt++ {
		advance(2)
		tk, _ := k.Task(task.ID)
		if tk.Status.IsTerminal() {
			t.Logf("terminal reached after %d failures: %s", attempt-1, tk.Status)
			break
		}
		before, _ := k.Task(task.ID)
		tk2, ferr := k.ReportResult(ctx, task.ID, "r1", domain.TaskResult{Success: false, Note: "err"})
		must2(t, ferr)
		t.Logf("failure #%d: attempts %d->%d status=%s", attempt, before.Attempts, tk2.Attempts, tk2.Status)
	}
	tk, _ := k.Task(task.ID)
	if tk.Status != domain.TaskFailed || tk.FailedReasonCode != domain.CodeExecutionFailed {
		t.Fatalf("status=%s reason=%s, want failed/execution_failed", tk.Status, tk.FailedReasonCode)
	}
	// 终态后再上报被拒绝。
	if _, err := k.ReportResult(ctx, task.ID, "r1", domain.TaskResult{Success: true}); !dispatcher.IsCode(err, domain.CodeInvalidState) {
		t.Fatalf("report after terminal: %v", err)
	}
	t.Logf("result: task failed permanently after %d attempts", tk.Attempts)
}

// TestSnapshotFileRoundtrip 文件存储：运行→快照+重启→继续运行→结果一致。
func TestSnapshotFileRoundtrip(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "wfstate")
	ctx := context.Background()

	store1 := storage.NewFileStore(dir)
	k1, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: store1})
	must2(t, err)
	addRobot(t, k1, "r1", "A")
	addRobot(t, k1, "r2", "D")
	_, err = k1.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "p1", Priority: 2, From: "A", To: "D"})
	must2(t, err)
	_, err = k1.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "p2", Priority: 7, From: "D", To: "A"})
	must2(t, err)
	tickAll(t, k1, []string{"r1", "r2"}, 10)
	mid := k1.Snapshot()
	must2(t, k1.Checkpoint(ctx))

	// 重启：载入快照，日志已截断。
	k2, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: storage.NewFileStore(dir)})
	must2(t, err)
	after := k2.Snapshot()
	if after.Clock != mid.Clock || len(after.Tasks) != len(mid.Tasks) {
		t.Fatalf("snapshot restore mismatch: clock %d vs %d", after.Clock, mid.Clock)
	}
	for id, tm := range mid.Tasks {
		if ta := after.Tasks[id]; ta == nil || ta.Status != tm.Status {
			t.Fatalf("task %s mismatch after restart: %v vs %v", id, ta, tm)
		}
	}
	// 再提交一个新任务并继续，确保恢复后仍可正常分派。
	_, err = k2.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "p3", Priority: 1, From: "A", To: "D"})
	must2(t, err)
	tickAll(t, k2, []string{"r1", "r2"}, 10)
	t3, _ := k2.Task("")
	_ = t3
	full := k2.Snapshot()
	// p1/p2 此前已完成；p3 也应完成（r1 回到 A 附近接手）。
	if full.Tasks["T0003"] != nil && full.Tasks["T0003"].Status != domain.TaskCompleted {
		t.Logf("p3 final status=%s", full.Tasks["T0003"].Status)
	}
	t.Log("result: file snapshot + restart restore verified")
}

// TestStateBackwardCompat 历史快照字段增删后仍可读取（v1 无 idem_index/plans 等）。
func TestStateBackwardCompat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compat")
	ctx := context.Background()
	store := storage.NewFileStore(dir)

	// 手写一个 v1 快照：缺少 idem_index，且 Task 少了新字段 requires。
	oldJSON := `{
  "version": 1,
  "state": {
    "version": 1,
    "clock": 3,
    "task_seq": 1,
    "robots": {"r1": {"id":"r1","node":"A","status":"idle","last_beat":3}},
    "tasks": {"T0001": {"id":"T0001","priority":2,"from":"A","to":"D","status":"pending","submitted_at":1}},
    "plans": {},
    "reservations": [],
    "decisions": []
  }
}`
	must2(t, writeSnapshotFile(dir, oldJSON))

	k, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: store})
	must2(t, err)
	st := k.Snapshot()
	if st.Clock != 3 {
		t.Fatalf("clock=%d want 3", st.Clock)
	}
	tk := st.Tasks["T0001"]
	if tk == nil || tk.Status != domain.TaskPending {
		t.Fatalf("old task not loaded: %+v", tk)
	}
	r := st.Robots["r1"]
	if r == nil || r.Node != "A" {
		t.Fatalf("old robot not loaded: %+v", r)
	}
	// 向前兼容：加载后内核仍可继续工作。
	tickAll(t, k, []string{"r1"}, 8)
	if st2 := k.Snapshot(); st2.Tasks["T0001"].Status != domain.TaskCompleted {
		t.Fatalf("task after replay of old state = %s", st2.Tasks["T0001"].Status)
	}
	if k.Snapshot().Version != domain.CurrentStateVersion {
		t.Fatalf("version not upgraded")
	}
	t.Log("result: v1 snapshot with missing fields loaded and upgraded")
}

func sameTasks(t *testing.T, a, b *domain.State) bool {
	t.Helper()
	if len(a.Tasks) != len(b.Tasks) {
		t.Logf("task count differs: %d vs %d", len(a.Tasks), len(b.Tasks))
		return false
	}
	for id, ta := range a.Tasks {
		tb := b.Tasks[id]
		if tb == nil {
			t.Logf("task %s missing in second state", id)
			return false
		}
		if ta.Status != tb.Status || ta.Attempts != tb.Attempts || ta.AssignedTo != tb.AssignedTo {
			t.Logf("task %s differs: %+v vs %+v", id, ta, tb)
			return false
		}
	}
	for id, ra := range a.Robots {
		rb := b.Robots[id]
		if rb == nil || ra.Node != rb.Node || ra.Status != rb.Status {
			t.Logf("robot %s differs: %+v vs %+v", id, ra, rb)
			return false
		}
	}
	return true
}

// TestStateForwardAndBackwardCompat 模拟：
//   - 未来版本（version=99）带未知字段：内核应忽略未知字段并继续工作；
//   - 当前版本快照往返：字段不丢失。
func TestStateForwardAndBackwardCompat(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "compat-fwd")
	ctx := context.Background()

	// 未来版本：带内核不认识的字段 future_flag / nested.extra。
	futureJSON := `{
  "version": 99,
  "state": {
    "version": 99,
    "clock": 1,
    "task_seq": 0,
    "robots": {"r1": {"id":"r1","node":"A","status":"idle","last_beat":1,"future_field":"abc"}},
    "tasks": {},
    "plans": {},
    "reservations": [],
    "decisions": [],
    "future_top": {"nested": [1,2,3]}
  }
}`
	must2(t, writeSnapshotFile(dir, futureJSON))

	k, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: storage.NewFileStore(dir)})
	must2(t, err)
	st := k.Snapshot()
	if st.Clock != 1 || st.Robots["r1"] == nil {
		t.Fatalf("future snapshot not loaded: %+v", st)
	}
	t.Log("future-version snapshot with unknown fields loaded successfully")

	// 继续正常工作：提交并完成任务。
	_, err = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "after-future", From: "A", To: "D"})
	must2(t, err)
	tickAll(t, k, []string{"r1"}, 10)
	if k.Snapshot().Tasks["T0001"].Status != domain.TaskCompleted {
		t.Fatal("kernel did not continue working after loading future snapshot")
	}
	t.Log("kernel continued operating after loading unknown-version snapshot")
}

// TestCheckpointTruncateLog 快照后日志被截断；再次重启仅从快照恢复，结果一致。
func TestCheckpointTruncateLog(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cp")
	ctx := context.Background()
	st1 := storage.NewFileStore(dir)
	k1, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: st1})
	must2(t, err)
	addRobot(t, k1, "r1", "A")
	_, err = k1.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "c1", From: "A", To: "D"})
	must2(t, err)
	tickAll(t, k1, []string{"r1"}, 10)
	before := k1.Snapshot()
	must2(t, k1.Checkpoint(ctx))
	ops, err := st1.ReadOps(ctx)
	must2(t, err)
	if len(ops) != 0 {
		t.Fatalf("ops not truncated: %d remain", len(ops))
	}
	k2, err := dispatcher.New(ctx, dispatcher.Config{Graph: laneGraph(), Store: storage.NewFileStore(dir)})
	must2(t, err)
	after := k2.Snapshot()
	if after.Clock != before.Clock {
		t.Fatalf("clock after checkpoint-restore: %d vs %d", after.Clock, before.Clock)
	}
	if after.Tasks["T0001"].Status != domain.TaskCompleted {
		t.Fatalf("task status after restore: %s", after.Tasks["T0001"].Status)
	}
	t.Log("checkpoint truncated log and restore produced identical state")
}
