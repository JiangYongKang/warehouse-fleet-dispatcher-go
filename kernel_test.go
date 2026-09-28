package dispatcher_test

import (
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
)

// TestNoPath 不连通路网的路径规划必须给出 ErrNoPath，任务也因此无法分派。
func TestNoPath(t *testing.T) {
	g := dispatcher.NewGraph()
	if err := g.AddNode(dispatcher.Node{ID: "X"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddNode(dispatcher.Node{ID: "Y"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(dispatcher.Edge{From: "X", To: "Y", Length: 1}); err != nil {
		t.Fatal(err)
	}
	// Y 无出边 -> X，故 Y->X 不可达。
	_, err := g.ShortestPath("Y", "X")
	mustCode(t, err, dispatcher.ErrNoPath)
	t.Log("decision: disconnected Y->X yields distinct no_path error")

	// 引擎层：机器人在 Y、任务 From=Y To=X，无可行路径 -> 保持 pending。
	// 需要一个能放机器人的孤立起点：再注册节点 Z 放机器人。
	if err := g.AddNode(dispatcher.Node{ID: "Z"}); err != nil {
		t.Fatal(err)
	}
	if err := g.AddEdge(dispatcher.Edge{From: "Z", To: "Y", Length: 1}); err != nil {
		t.Fatal(err)
	}
	eng := newEngine(t, g)
	if err := eng.RegisterRobot("r1", "Z"); err != nil {
		t.Fatal(err)
	}
	if err := eng.SubmitTask(dispatcher.Task{ID: "t1", From: "Y", To: "X", Priority: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := eng.AssignOnce(1); err != nil {
		t.Fatal(err)
	}
	ts, _ := eng.TaskSnapshot("t1")
	if ts.Status != dispatcher.TaskPending {
		t.Fatalf("task with no feasible route must stay pending, got %s", ts.Status)
	}
	t.Log("decision: task whose route is infeasible stays pending rather than being assigned")
}

// TestRegisterValidation 机器人注册的非法输入。
func TestRegisterValidation(t *testing.T) {
	g := simpleGraph(t)
	eng := newEngine(t, g)
	mustCode(t, eng.RegisterRobot("", "A"), dispatcher.ErrInvalidArgument)
	mustCode(t, eng.RegisterRobot("r1", "ZZ"), dispatcher.ErrNotFound)
	if err := eng.RegisterRobot("r1", "A"); err != nil {
		t.Fatal(err)
	}
	mustCode(t, eng.RegisterRobot("r1", "A"), dispatcher.ErrAlreadyExists)
	mustCode(t, eng.MarkOffline("ghost"), dispatcher.ErrNotFound)
	mustCode(t, eng.MarkOnline("ghost"), dispatcher.ErrNotFound)
}

// TestDeadlockCycleAndVictim 纯函数验证：确定性找环 + 牺牲者规则
// （有效优先级最低；平局取提交序号最大=最年轻者；再平局取最大机器人 ID）。
func TestDeadlockCycleAndVictim(t *testing.T) {
	// 4 个节点，边：a->b, b->c, c->a 成环；d 孤立。
	edges := []struct{ from, to string }{
		{"a", "b"}, {"b", "c"}, {"c", "a"},
	}
	nodes := []string{"a", "b", "c", "d"}
	cyc := dispatcher.DetectCycleForTest(toWaitEdges(edges), nodes)
	if len(cyc) != 3 {
		t.Fatalf("expected cycle of length 3, got %v", cyc)
	}
	// 重复调用结果必须一致。
	cyc2 := dispatcher.DetectCycleForTest(toWaitEdges(edges), nodes)
	if !sameSeq(cyc, cyc2) {
		t.Fatalf("cycle detection non-deterministic: %v vs %v", cyc, cyc2)
	}
	t.Logf("input waits-for a->b,b->c,c->a; decision: cycle=%v, repeat=%v", cyc, cyc2)

	if got := dispatcher.DetectCycleForTest(nil, nodes); got != nil {
		t.Fatalf("acyclic graph should return nil, got %v", got)
	}

	// 牺牲者：b 有效优先级最低。
	pri := map[string]int{"a": 5, "b": 1, "c": 5}
	seq := map[string]int64{"a": 1, "b": 2, "c": 3}
	v := dispatcher.BreakDeadlockForTest(cyc,
		func(id string) int { return pri[id] },
		func(id string) int64 { return seq[id] },
	)
	if v != "b" {
		t.Fatalf("victim should be lowest effPri b, got %s", v)
	}

	// 同有效优先级：最年轻（seq 最大）者让路 -> c。
	pri2 := map[string]int{"a": 5, "b": 5, "c": 5}
	v2 := dispatcher.BreakDeadlockForTest(cyc,
		func(id string) int { return pri2[id] },
		func(id string) int64 { return seq[id] },
	)
	if v2 != "c" {
		t.Fatalf("tie: newest seq c must yield, got %s", v2)
	}
	t.Log("decision: victim=lowest effPri; tie => newest seq (FIFO-friendly, starvation-safe)")
}

func toWaitEdges(pairs []struct{ from, to string }) []dispatcher.WaitEdgeForTest {
	out := make([]dispatcher.WaitEdgeForTest, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, dispatcher.WaitEdgeForTest{From: p.from, To: p.to})
	}
	return out
}

func sameSeq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
