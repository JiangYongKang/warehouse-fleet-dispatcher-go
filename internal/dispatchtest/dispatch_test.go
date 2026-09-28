package dispatchtest

import (
	"context"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
)

// TestNormalDispatch 正常派单基线：提交→分派→移动→到达→上报完成。
func TestNormalDispatch(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	if _, err := k.AddRobot(ctx, "r1", "A"); err != nil {
		t.Fatal(err)
	}
	task, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "job-1", Priority: 5, From: "A", To: "F",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input: task=%s prio=5 route=A->F, advance 8 ticks", task.ID)
	tickAll(t, k, []string{"r1"}, 8)

	tk, _ := k.Task(task.ID)
	if tk.Status != domain.TaskCompleted {
		t.Fatalf("task status=%s, want completed", tk.Status)
	}
	rb, _ := k.Robot("r1")
	if rb.Node != "F" || rb.Status != domain.RobotIdle {
		t.Fatalf("robot node=%s status=%s, want F/idle", rb.Node, rb.Status)
	}
	t.Logf("result: task=%s completed by r1 at F, attempts=%d", tk.ID, tk.Attempts)
	logDecisions(t, k, "dispatch_assign", "arrival", "report_success")
}

// TestInvalidTransitions 各类非法流转必须给出可区分原因码。
func TestInvalidTransitions(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())

	// 机器人重复注册
	if _, err := k.AddRobot(ctx, "r1", "A"); err != nil {
		t.Fatal(err)
	}
	if _, err := k.AddRobot(ctx, "r1", "A"); !dispatcher.IsCode(err, domain.CodeAlreadyExists) {
		t.Fatalf("dup robot: %v", err)
	}
	// 机器人/节点不存在
	if err := k.Heartbeat(ctx, "ghost"); !dispatcher.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("heartbeat ghost: %v", err)
	}
	if _, err := k.AddRobot(ctx, "rX", "Z"); !dispatcher.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("add at unknown node: %v", err)
	}

	// 任务起点/终点不存在、不可达
	if _, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{From: "A", To: "Z"}); !dispatcher.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("unknown node task: %v", err)
	}

	// 依赖不存在 / 依赖失败
	if _, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "j-missing", From: "A", To: "F", Deps: []string{"nope"},
	}); !dispatcher.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("missing dep: %v", err)
	}

	// 上报不存在的任务 / 未分派任务
	if _, err := k.ReportResult(ctx, "ghost", "r1", domain.TaskResult{Success: true}); !dispatcher.IsCode(err, domain.CodeNotFound) {
		t.Fatalf("report ghost task: %v", err)
	}
	t1, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "j1", From: "A", To: "F",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := k.ReportResult(ctx, t1.ID, "r1", domain.TaskResult{Success: true}); !dispatcher.IsCode(err, domain.CodeNotAssigned) {
		t.Fatalf("report unassigned: %v", err)
	}

	// 推进后由 r1 执行；错误机器人上报被拒绝
	tickAll(t, k, []string{"r1"}, 1)
	if _, err := k.ReportResult(ctx, t1.ID, "ghost-r", domain.TaskResult{Success: true}); !dispatcher.IsCode(err, domain.CodeInvalidState) {
		t.Fatalf("report by wrong robot: %v", err)
	}
	tickAll(t, k, []string{"r1"}, 8)

	// 重复上报：终态任务再次上报
	_, err = k.ReportResult(ctx, t1.ID, "r1", domain.TaskResult{Success: true})
	if !dispatcher.IsCode(err, domain.CodeInvalidState) {
		t.Fatalf("dup report: %v", err)
	}
	t.Logf("duplicate report rejected with code=invalid_state: %v", err)

	// 同幂等键重复提交
	_, err = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "j1", From: "A", To: "F"})
	if !dispatcher.IsCode(err, domain.CodeDuplicateSubmit) {
		t.Fatalf("dup submit: %v", err)
	}
	t.Logf("duplicate submit rejected with code=duplicate_submit: %v", err)

	// 成功上报时机器人不在终点 -> at_wrong_node（构造：r1 在 A 提交 A->F，
	// 先不推进到终点是无法成功的；这里用失败上报验证不改变终态语义）。
	t2, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "j2", From: "F", To: "A"})
	if err != nil {
		t.Fatal(err)
	}
	// r1 在 F（刚完成 j1），推进 j2 但中途失败上报：合法失败流转。
	tickAll(t, k, []string{"r1"}, 1)
	tk2, err := k.ReportResult(ctx, t2.ID, "r1", domain.TaskResult{Success: false, Note: "boom"})
	if err != nil {
		t.Fatalf("failure report: %v", err)
	}
	t.Logf("failure accepted: task=%s attempts=%d status=%s", tk2.ID, tk2.Attempts, tk2.Status)
	if tk2.Attempts != 1 {
		t.Fatalf("attempts=%d want 1", tk2.Attempts)
	}
}
