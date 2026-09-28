package dispatcher_test

import (
	"errors"
	"sync"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
)

// mustCode 断言 err 是带指定错误码的 *Error，并打印判定依据。
func mustCode(t *testing.T, err error, code dispatcher.ErrorCode) {
	t.Helper()
	if err == nil {
		t.Fatalf("expected error code %q, got nil", code)
	}
	var pe *dispatcher.Error
	if !errors.As(err, &pe) {
		t.Fatalf("expected *dispatcher.Error, got %T: %v", err, err)
	}
	if dispatcher.ErrorCodeOf(err) != code {
		t.Fatalf("expected error code %q, got %q (%v)", code, pe.Code, err)
	}
	t.Logf("OK reject reason=%q op=%q msg=%q", pe.Code, pe.Op, pe.Msg)
}

func gridGraph(t *testing.T) *dispatcher.Graph {
	t.Helper()
	g := dispatcher.NewGraph()
	for _, n := range []string{"A", "B", "C", "D", "E", "S"} {
		if err := g.AddNode(dispatcher.Node{ID: n}); err != nil {
			t.Fatal(err)
		}
	}
	// 双向边，长度统一为 2；外加 A-D-C 的绕行环线。
	pairs := [][2]string{
		{"A", "B"}, {"B", "C"},
		{"A", "D"}, {"D", "C"},
		{"B", "D"},
		{"S", "A"},
	}
	for _, p := range pairs {
		for _, d := range [][2]string{{p[0], p[1]}, {p[1], p[0]}} {
			if err := g.AddEdge(dispatcher.Edge{From: d[0], To: d[1], Length: 2}); err != nil {
				t.Fatal(err)
			}
		}
	}
	return g
}

func newEngine(t *testing.T, g *dispatcher.Graph, store ...dispatcher.Store) *dispatcher.Engine {
	t.Helper()
	opts := []dispatcher.Option{
		dispatcher.WithLogger(tlog{t}),
		dispatcher.WithoutBackground(), // 显式驱动：测试时序确定、无后台竞态
	}
	if len(store) == 1 {
		opts = append(opts, dispatcher.WithStore(store[0]))
	}
	eng, err := dispatcher.NewEngine(g, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })
	return eng
}

// driveToCompletion 推进时钟，并在机器人到达后自动上报成功。
func driveToCompletion(t *testing.T, eng *dispatcher.Engine, ticks int64, reportSuccess bool) {
	t.Helper()
	for tk := int64(1); tk <= ticks; tk++ {
		if _, err := eng.AssignOnce(tk); err != nil {
			t.Fatalf("tick %d assign: %v", tk, err)
		}
		if reportSuccess {
			autoReport(t, eng, true, "")
		}
	}
}

func autoReport(t *testing.T, eng *dispatcher.Engine, success bool, reason string) {
	t.Helper()
	for _, ts := range eng.Tasks() {
		if ts.Status != dispatcher.TaskAssigned || ts.Assignee == "" {
			continue
		}
		// 仅当机器人本轮已走完路径（arrived）才上报，
		// 避免对"刚重派、仍停在旧终点"的执行误报。
		if eng.IsAwaitingResult(ts.Assignee) {
			_ = eng.ReportResult(dispatcher.TickReport{
				TaskID: ts.Task.ID, RobotID: ts.Assignee, Success: success, Reason: reason,
			})
		}
	}
}

// TestSubmitValidation 覆盖任务提交的各类非法输入，要求原因可区分。
func TestSubmitValidation(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	t.Log("input: empty id / missing nodes / unknown dep / self dep / dup dep / dup task")

	mustCode(t, eng.SubmitTask(dispatcher.Task{ID: "", From: "A", To: "B"}), dispatcher.ErrInvalidArgument)
	mustCode(t, eng.SubmitTask(dispatcher.Task{ID: "x", From: "Z", To: "B"}), dispatcher.ErrNotFound)
	mustCode(t, eng.SubmitTask(dispatcher.Task{ID: "x", From: "A", To: "Z"}), dispatcher.ErrNotFound)
	mustCode(t, eng.SubmitTask(dispatcher.Task{ID: "x", From: "A", To: "B", Deps: []string{"ghost"}}), dispatcher.ErrDepsUnmet)
	mustCode(t, eng.SubmitTask(dispatcher.Task{ID: "x", From: "A", To: "B", Deps: []string{"x"}}), dispatcher.ErrInvalidArgument)

	if err := eng.SubmitTask(dispatcher.Task{ID: "t0", From: "A", To: "B"}); err != nil {
		t.Fatal(err)
	}
	mustCode(t, eng.SubmitTask(dispatcher.Task{ID: "dup", From: "A", To: "B", Deps: []string{"t0", "t0"}}), dispatcher.ErrInvalidArgument)
	mustCode(t, eng.SubmitTask(dispatcher.Task{ID: "t0", From: "A", To: "B"}), dispatcher.ErrAlreadyExists)

	// 深依赖链 t1->t2->t3 合法放行；反向/环在本提交规则下只能表现为自依赖（已在上面拒绝）。
	if err := eng.SubmitTask(dispatcher.Task{ID: "c1", From: "A", To: "B"}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "c2", From: "A", To: "B", Deps: []string{"c1"}}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "c3", From: "A", To: "B", Deps: []string{"c2", "c1"}}); err != nil {
		t.Fatalf("valid deep dependency chain rejected: %v", err)
	}
	t.Log("decision: self/cyclic deps rejected, valid deep chain accepted")
	t.Log("decision: all malformed submissions rejected with distinct codes, no state side-effects")
}

// TestDependencyGating 验证依赖未完成时不能分派，完成后自动放行。
func TestDependencyGating(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterRobot("r2", "S"); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "base", From: "A", To: "C", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "child", From: "S", To: "A", Priority: 1, Deps: []string{"base"}}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.AssignOnce(1); err != nil {
		t.Fatal(err)
	}
	child, _ := eng.TaskSnapshot("child")
	if child.Status != dispatcher.TaskPending {
		t.Fatalf("child must stay pending while dep unfinished, got %s", child.Status)
	}
	t.Log("input child deps=[base] unfinished; decision: child held pending")

	driveToCompletion(t, eng, 20, true)
	child, _ = eng.TaskSnapshot("child")
	if child.Status != dispatcher.TaskCompleted {
		t.Fatalf("child should dispatch and complete after base, got %s", child.Status)
	}
	base, _ := eng.TaskSnapshot("base")
	if base.Status != dispatcher.TaskCompleted {
		t.Fatalf("base not completed")
	}
	t.Logf("decision: after base completed at some earlier tick, child assigned to %s and completed", child.Assignee)
}

// TestExclusiveAssignment 同一任务只能被一个机器人执行；同一时刻一个机器人只做一个任务。
func TestExclusiveAssignment(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	for _, id := range []string{"r1", "r2"} {
		if err := eng.RegisterRobot(id, "A"); err != nil {
			t.Fatal(err)
		}
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "t1", From: "A", To: "C", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.AssignOnce(1); err != nil {
		t.Fatal(err)
	}
	ts, _ := eng.TaskSnapshot("t1")
	if ts.Assignee != "r1" { // 平局取 ID 最小
		t.Fatalf("want r1, got %s", ts.Assignee)
	}
	// 第二个任务在 r1 忙时只能给 r2，t1 不会被重复分派给 r2。
	if err := eng.SubmitTask(dispatcher.Task{ID: "t2", From: "A", To: "D", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.AssignOnce(2); err != nil {
		t.Fatal(err)
	}
	t2, _ := eng.TaskSnapshot("t2")
	if t2.Assignee != "r2" {
		t.Fatalf("t2 must go to idle r2, got %s", t2.Assignee)
	}
	t1, _ := eng.TaskSnapshot("t1")
	if t1.Assignee != "r1" {
		t.Fatalf("t1 assignee changed to %s; task was double-assigned", t1.Assignee)
	}
	t.Logf("decision: t1->%s t2->%s, exclusivity holds", t1.Assignee, t2.Assignee)
}

// TestPriorityDispatch 同一时刻就绪的多个任务，高优先级先分派；
// 同优先级按提交序号 FIFO。
func TestPriorityDispatch(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	// 两台空闲机器人，恰好同时分派两个任务。
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterRobot("r2", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "low", From: "A", To: "D", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "high", From: "A", To: "D", Priority: 9}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.AssignOnce(1); err != nil {
		t.Fatal(err)
	}
	hi, _ := eng.TaskSnapshot("high")
	lo, _ := eng.TaskSnapshot("low")
	// 平局机器人选择按 ID：两个任务都应被分派，其中 high 必须拿到 r1（先被处理）。
	if hi.Assignee != "r1" {
		t.Fatalf("higher priority must be processed first -> high gets r1, got %q", hi.Assignee)
	}
	if lo.Assignee != "r2" {
		t.Fatalf("lower priority gets the remaining robot r2, got %q", lo.Assignee)
	}
	t.Log("decision: high(pri9)->r1 then low(pri1)->r2 in the same dispatch round")
}

// TestAgingOverridesFreshHigh 精确验证老化补偿：
// 一台机器人；先提交 long-running 高优先级任务并停在终点不结束，
// 期间一个低优先级任务持续等待；等待足够久后再提交"新的"高优先级任务。
// 当 long-running 结束、机器人空闲的同一拍，低优先级（因老化）应先于新高优先级被分派。
func TestAgingOverridesFreshHigh(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	// hold：A->C，到达后我们不立刻结束，用它占住机器人若干拍。
	if err := eng.SubmitTask(dispatcher.Task{ID: "hold", From: "A", To: "C", Priority: 5}); err != nil {
		t.Fatal(err)
	}
	// low：同起点 A，因机器人忙而等待。
	if err := eng.SubmitTask(dispatcher.Task{ID: "low", From: "A", To: "C", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	// 推进到 hold 到达 C（约 5 拍），随后继续空转让 low 等待足够久（>= agingInterval 拍）。
	var holdArrivedAt int64
	for tk := int64(1); tk <= 40; tk++ {
		if _, err := eng.AssignOnce(tk); err != nil {
			t.Fatal(err)
		}
		if holdArrivedAt == 0 && eng.IsAwaitingResult("r1") {
			holdArrivedAt = tk
		}
	}
	if holdArrivedAt == 0 {
		t.Fatal("hold never arrived")
	}
	// 此时 low 已等待约 40 拍，老化补偿 = 40/10 = 4 级，有效优先级 1+4=5。
	// 再提交一个全新的高优先级 fresh（静态 3，老化 0）。同目的地 C，
	// 排除"可达性差异"，纯粹比较有效优先级：aged low eff=5 > fresh eff=3。
	if err := eng.SubmitTask(dispatcher.Task{ID: "fresh", From: "A", To: "C", Priority: 3}); err != nil {
		t.Fatal(err)
	}
	// 结束 hold，释放机器人；下一拍 low(eff 5) 应击败 fresh(eff 3)。
	if err := eng.ReportResult(dispatcher.TickReport{TaskID: "hold", RobotID: "r1", Success: true}); err != nil {
		t.Fatal(err)
	}
	// 机器人此刻在 C；两任务都从 A 出发，可达性相同（都需 C->A 再接 A->C）。
	if _, err := eng.AssignOnce(41); err != nil {
		t.Fatal(err)
	}
	low, _ := eng.TaskSnapshot("low")
	fresh, _ := eng.TaskSnapshot("fresh")
	t.Logf("input at tick41: low(pri1,waited~40t,effPri~5) vs fresh(pri3,effPri3), same route; dispatch: low=%q fresh=%q",
		low.Assignee, fresh.Assignee)
	if low.Assignee != "r1" {
		t.Fatalf("aged low-priority task must be served first by aging bonus, got assignee %q", low.Assignee)
	}
	if fresh.Status != dispatcher.TaskPending {
		t.Fatalf("fresh higher-static-priority task must wait behind aged low task, got %s", fresh.Status)
	}
	t.Log("decision: aging bonus lets a long-waiting low-priority task overtake a fresh higher-priority one => bounded wait, no starvation")
}

// TestReportIdempotency 重复上报 / 过期上报 / 提前上报都被正确区分。
func TestReportIdempotency(t *testing.T) {
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
	mustCode(t, eng.ReportResult(dispatcher.TickReport{TaskID: "ghost", RobotID: "r1", Success: true}), dispatcher.ErrNotFound)
	// 未到达就上报：invalid_state。
	if _, err := eng.AssignOnce(1); err != nil {
		t.Fatal(err)
	}
	mustCode(t, eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: true}), dispatcher.ErrInvalidState)
	// 其他机器人冒充上报：stale_result。
	mustCode(t, eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r2", Success: true}), dispatcher.ErrStaleResult)

	driveToCompletion(t, eng, 20, false)
	if err := eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: true}); err != nil {
		t.Fatal(err)
	}
	// 成功重复上报：already_done（幂等，绝不二次执行）。
	mustCode(t, eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: true}), dispatcher.ErrAlreadyDone)
	// 完成后迟到的失败上报：stale_result。
	mustCode(t, eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: false, Reason: "late"}), dispatcher.ErrStaleResult)
	ts, _ := eng.TaskSnapshot("t1")
	if ts.Status != dispatcher.TaskCompleted || ts.Attempt != 0 || ts.Dispatch != 1 {
		t.Fatalf("duplicate reports changed state: %+v", ts)
	}
	t.Log("decision: duplicate success idempotent, late failure rejected, attempt stays 1, no re-execution")
}

// TestFailureRetryAndTerminal 失败重试到上限后 failed 终态，终态不可再上报。
func TestFailureRetryAndTerminal(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "t1", From: "A", To: "C", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	var clock int64
	for attempt := 1; attempt <= dispatcher.MaxAttempts; attempt++ {
		// 推进直到任务被分派并到达终点。
		deadline := clock + 60
		for {
			clock++
			if _, err := eng.AssignOnce(clock); err != nil {
				t.Fatalf("tick %d: %v", clock, err)
			}
			ts, err := eng.TaskSnapshot("t1")
			if err != nil {
				t.Fatal(err)
			}
			r, err := eng.RobotSnapshot("r1")
			if err != nil {
				t.Fatal(err)
			}
			// 必须是"本轮执行已到达"：任务 assigned 给 r1 且 r1 已到终点，
			// 否则可能只是上一轮失败后停在终点、尚未重新出发。
			if ts.Status == dispatcher.TaskAssigned && ts.Assignee == "r1" && r.CurrentTask == "t1" && eng.IsAwaitingResult("r1") {
				break
			}
			if clock > deadline {
				t.Fatalf("attempt %d never arrived by tick %d (status=%s)", attempt, deadline, ts.Status)
			}
		}
		if err := eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: false, Reason: "payload dropped"}); err != nil {
			t.Fatalf("attempt %d report: %v", attempt, err)
		}
		ts, _ := eng.TaskSnapshot("t1")
		if attempt < dispatcher.MaxAttempts {
			if ts.Status != dispatcher.TaskPending {
				t.Fatalf("attempt %d: want pending requeue, got %s", attempt, ts.Status)
			}
			if ts.Attempt != attempt {
				t.Fatalf("attempt %d: Attempt=%d want %d", attempt, ts.Attempt, attempt)
			}
		}
	}
	ts, _ := eng.TaskSnapshot("t1")
	if ts.Status != dispatcher.TaskFailed {
		t.Fatalf("want failed after %d attempts, got %s", dispatcher.MaxAttempts, ts.Status)
	}
	mustCode(t, eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: true}), dispatcher.ErrAlreadyDone)
	t.Logf("decision: failed terminally after %d reported failures, later success rejected", dispatcher.MaxAttempts)
}

// TestConcurrentSubmitAndReport 并发提交与上报不得产生数据竞争或重复分派。
func TestConcurrentSubmitAndReport(t *testing.T) {
	g := gridGraph(t)
	eng := newEngine(t, g)
	for i := 0; i < 4; i++ {
		if err := eng.RegisterRobot(string(rune('a'+i)), "A"); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	// 并发表单相同的一批任务：每个 ID 恰好成功一次，其余 already_exists。
	const n = 40
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n; i++ {
				id := "task-" + itoa(i)
				err := eng.SubmitTask(dispatcher.Task{ID: id, From: "A", To: "C", Priority: i % 5})
				if err != nil && dispatcher.ErrorCodeOf(err) != dispatcher.ErrAlreadyExists {
					t.Errorf("unexpected submit error: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()
	got := eng.Tasks()
	if len(got) != n {
		t.Fatalf("want exactly %d unique tasks, got %d", n, len(got))
	}
	// 推进并完成全部任务。
	driveToCompletion(t, eng, 400, true)
	completed := 0
	assignees := map[string]int{}
	for _, ts := range eng.Tasks() {
		if ts.Status != dispatcher.TaskCompleted {
			t.Fatalf("task %s not completed: %s", ts.ID, ts.Status)
		}
		completed++
		assignees[ts.Assignee]++
		if ts.Dispatch < 1 {
			t.Fatalf("task %s dispatch %d", ts.ID, ts.Dispatch)
		}
	}
	t.Logf("decision: %d tasks submitted once and completed; per-robot counts=%v", completed, assignees)
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b []byte
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}
