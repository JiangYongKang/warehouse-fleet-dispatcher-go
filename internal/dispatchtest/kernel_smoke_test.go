package dispatchtest

import (
	"context"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/network"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/storage"
)

func lineGraph() *network.Graph {
	return network.NewGraph(
		[]string{"A", "B", "C", "D"},
		[]network.Edge{
			{From: "A", To: "B", Cost: 2}, {From: "B", To: "C", Cost: 2}, {From: "C", To: "D", Cost: 2},
			{From: "B", To: "A", Cost: 2}, {From: "C", To: "B", Cost: 2}, {From: "D", To: "C", Cost: 2},
		},
	)
}

func TestSmokeNormalDispatch(t *testing.T) {
	ctx := context.Background()
	k, err := dispatcher.New(ctx, dispatcher.Config{Graph: lineGraph(), Store: storage.NewMemoryStore()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.AddRobot(ctx, "r1", "A"); err != nil {
		t.Fatal(err)
	}
	task, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "k-1", Priority: 5, From: "A", To: "D",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("submitted task %s", task.ID)

	// A->D 三跳，每跳 cost 2，共 6 tick 到达。
	for i := 0; i < 10; i++ {
		if err := k.Heartbeat(ctx, "r1"); err != nil {
			t.Fatal(err)
		}
		if err := k.Advance(ctx, 1); err != nil {
			t.Fatal(err)
		}
		tk, _ := k.Task(task.ID)
		rb, _ := k.Robot("r1")
		t.Logf("tick=%d task=%s robot=%s node=%s", i+1, tk.Status, rb.Status, rb.Node)
		if rb.Status == domain.RobotExecuting && rb.Node == "D" {
			break
		}
	}
	rb, _ := k.Robot("r1")
	if rb.Node != "D" || rb.Status != domain.RobotExecuting {
		t.Fatalf("robot not arrived: node=%s status=%s", rb.Node, rb.Status)
	}
	if _, err := k.ReportResult(ctx, task.ID, "r1", domain.TaskResult{Success: true}); err != nil {
		t.Fatal(err)
	}
	tk, _ := k.Task(task.ID)
	if tk.Status != domain.TaskCompleted {
		t.Fatalf("task status=%s", tk.Status)
	}
	// 重复上报必须被拒绝。
	if _, err := k.ReportResult(ctx, task.ID, "r1", domain.TaskResult{Success: true}); !dispatcher.IsCode(err, domain.CodeInvalidState) {
		t.Fatalf("duplicate report err=%v, want invalid_state", err)
	}
	// 幂等重复提交。
	if _, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "k-1", From: "A", To: "D"}); !dispatcher.IsCode(err, domain.CodeDuplicateSubmit) {
		t.Fatalf("dup submit err=%v", err)
	}
}
