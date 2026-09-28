package dispatcher_test

import (
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
)

type tlog struct{ t *testing.T }

func (l tlog) Printf(format string, args ...any) { l.t.Logf(format, args...) }

func newLineGraph(t *testing.T) *dispatcher.Graph {
	t.Helper()
	g := dispatcher.NewGraph()
	for _, n := range []string{"A", "B", "C"} {
		if err := g.AddNode(dispatcher.Node{ID: n}); err != nil {
			t.Fatal(err)
		}
	}
	for _, e := range [][2]string{{"A", "B"}, {"B", "A"}, {"B", "C"}, {"C", "B"}} {
		if err := g.AddEdge(dispatcher.Edge{From: e[0], To: e[1], Length: 2}); err != nil {
			t.Fatal(err)
		}
	}
	return g
}

// TestSmokeBasicAssign 走通：提交任务 -> 分派 -> 运动到终点 -> 上报完成。
func TestSmokeBasicAssign(t *testing.T) {
	g := newLineGraph(t)
	eng, err := dispatcher.NewEngine(g, dispatcher.WithLogger(tlog{t}), dispatcher.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "t1", From: "A", To: "C", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	// A->B length2：tick1 进入、tick3 到达 B；B->C：tick4 进入、tick5 到达 C。
	for tk := int64(1); tk <= 5; tk++ {
		if _, err := eng.AssignOnce(tk); err != nil {
			t.Fatal(err)
		}
	}
	ts, err := eng.TaskSnapshot("t1")
	if err != nil {
		t.Fatal(err)
	}
	if ts.Status != dispatcher.TaskAssigned || ts.Assignee != "r1" {
		t.Fatalf("unexpected task state: %+v", ts)
	}
	r, _ := eng.RobotSnapshot("r1")
	if r.Node != "C" {
		t.Fatalf("robot at %q, want C", r.Node)
	}
	if err := eng.ReportResult(dispatcher.TickReport{TaskID: "t1", RobotID: "r1", Success: true}); err != nil {
		t.Fatal(err)
	}
	ts, _ = eng.TaskSnapshot("t1")
	if ts.Status != dispatcher.TaskCompleted {
		t.Fatalf("want completed, got %s", ts.Status)
	}
}

// TestSmokeHeadOn 两个机器人在单车道 A~B 对头行驶，必须检测并打破死锁，
// 且最终两个任务都能完成。
func TestSmokeHeadOn(t *testing.T) {
	g := newLineGraph(t)
	eng, err := dispatcher.NewEngine(g, dispatcher.WithLogger(tlog{t}), dispatcher.WithoutBackground())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	if err := eng.RegisterRobot("r2", "B"); err != nil {
		t.Fatal(err)
	}
	// r1: A->B（进入车道），r2: B->A（对头）。
	if err := eng.SubmitTask(dispatcher.Task{ID: "t1", From: "A", To: "B", Priority: 5}); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "t2", From: "B", To: "A", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	completed := map[string]bool{}
	for tk := int64(1); tk <= 40; tk++ {
		if _, err := eng.AssignOnce(tk); err != nil {
			t.Fatal(err)
		}
		for _, id := range []string{"t1", "t2"} {
			ts, _ := eng.TaskSnapshot(id)
			r := ts.Assignee
			if r != "" {
				if rr, _ := eng.RobotSnapshot(r); rr.CurrentTask == id && rr.Node == ts.To {
					_ = eng.ReportResult(dispatcher.TickReport{TaskID: id, RobotID: r, Success: true})
				}
			}
		}
	}
	for _, id := range []string{"t1", "t2"} {
		ts, _ := eng.TaskSnapshot(id)
		if ts.Status != dispatcher.TaskCompleted {
			t.Fatalf("task %s status=%s assignee=%s, want completed", id, ts.Status, ts.Assignee)
		}
		completed[id] = true
	}
	if completed["t1"] && completed["t2"] {
		t.Logf("both head-on tasks completed after deadlock resolution")
	}
}
