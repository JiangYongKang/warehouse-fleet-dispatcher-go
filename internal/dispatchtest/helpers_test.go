package dispatchtest

import (
	"context"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/network"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/storage"
)

func newKernel(t *testing.T, g *network.Graph) (*dispatcher.Kernel, *storage.MemoryStore) {
	t.Helper()
	st := storage.NewMemoryStore()
	k, err := dispatcher.New(context.Background(), dispatcher.Config{Graph: g, Store: st})
	if err != nil {
		t.Fatalf("new kernel: %v", err)
	}
	return k, st
}

// tickAll 心跳全部在线机器人并推进 n 个 tick；到达终点自动上报成功。
func tickAll(t *testing.T, k *dispatcher.Kernel, robots []string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		for _, r := range robots {
			if err := k.Heartbeat(ctx, r); err != nil {
				t.Fatalf("heartbeat %s: %v", r, err)
			}
		}
		if err := k.Advance(ctx, 1); err != nil {
			t.Fatalf("advance: %v", err)
		}
		autoReport(t, k)
	}
}

func autoReport(t *testing.T, k *dispatcher.Kernel) {
	t.Helper()
	ctx := context.Background()
	st := k.Snapshot()
	for id, r := range st.Robots {
		if r.Status == domain.RobotExecuting && r.CurrentTask != "" {
			if _, err := k.ReportResult(ctx, r.CurrentTask, id, domain.TaskResult{Success: true}); err != nil {
				t.Fatalf("auto report: %v", err)
			}
		}
	}
}

func gridGraph() *network.Graph {
	// 双向方格：
	//  A-B-C
	//  | | |
	//  D-E-F
	nodes := []string{"A", "B", "C", "D", "E", "F"}
	var edges []network.Edge
	add := func(u, v string, c int) {
		edges = append(edges, network.Edge{From: u, To: v, Cost: c}, network.Edge{From: v, To: u, Cost: c})
	}
	add("A", "B", 1)
	add("B", "C", 1)
	add("A", "D", 1)
	add("B", "E", 1)
	add("C", "F", 1)
	add("D", "E", 1)
	add("E", "F", 1)
	return network.NewGraph(nodes, edges)
}

// laneGraph 单车道 A-B-C-D：每段都显式声明为双向互斥的单车道。
func laneGraph() *network.Graph {
	line := []string{"A", "B", "C", "D"}
	var edges []network.Edge
	var lanes []network.Lane
	for i := 0; i+1 < len(line); i++ {
		edges = append(edges, network.Edge{From: line[i], To: line[i+1], Cost: 1}, network.Edge{From: line[i+1], To: line[i], Cost: 1})
		lanes = append(lanes, network.Lane{U: line[i], V: line[i+1]})
	}
	return network.NewGraphWithLanes(line, edges, lanes)
}

// longLaneGraph 6 节点单车道，边成本 2，制造足够长的途中时间窗口。
func longLaneGraph() *network.Graph {
	line := []string{"A", "B", "C", "D", "E", "F"}
	var edges []network.Edge
	var lanes []network.Lane
	for i := 0; i+1 < len(line); i++ {
		edges = append(edges, network.Edge{From: line[i], To: line[i+1], Cost: 2}, network.Edge{From: line[i+1], To: line[i], Cost: 2})
		lanes = append(lanes, network.Lane{U: line[i], V: line[i+1]})
	}
	return network.NewGraphWithLanes(line, edges, lanes)
}

func logDecisions(t *testing.T, k *dispatcher.Kernel, kinds ...string) {
	t.Helper()
	want := map[string]bool{}
	for _, k := range kinds {
		want[k] = true
	}
	for _, d := range k.Decisions(0) {
		if len(want) == 0 || want[d.Kind] {
			t.Logf("[decision] tick=%d %s: %s", d.At, d.Kind, d.Detail)
		}
	}
}
