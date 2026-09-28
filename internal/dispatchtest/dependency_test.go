package dispatchtest

import (
	"context"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
)

// TestDependencyChain 任务依赖：后继任务只能在前驱完成后分派。
func TestDependencyChain(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r2", "F")

	t1, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "dep-1", Priority: 1, From: "A", To: "F",
	})
	must2(t, err)
	t2, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "dep-2", Priority: 1, From: "F", To: "A", Deps: []string{t1.ID},
	})
	must2(t, err)
	t3, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "dep-3", Priority: 1, From: "A", To: "E", Deps: []string{t2.ID},
	})
	must2(t, err)
	t.Logf("input: chain %s -> %s -> %s", t1.ID, t2.ID, t3.ID)

	for tick := 0; tick < 40; tick++ {
		tickAll(t, k, []string{"r1", "r2"}, 1)
		s1, _ := k.Task(t1.ID)
		s2, _ := k.Task(t2.ID)
		// t2 不得在 t1 完成前开始（assigned/running/completed 之外）。
		if s1.Status != domain.TaskCompleted {
			if s2.Status != domain.TaskPending {
				t.Fatalf("t2 started before t1 completed: t1=%s t2=%s", s1.Status, s2.Status)
			}
		}
		if s2.Status != domain.TaskCompleted {
			s3, _ := k.Task(t3.ID)
			if s3.Status != domain.TaskPending {
				t.Fatalf("t3 started before t2 completed: t2=%s t3=%s", s2.Status, s3.Status)
			}
		}
	}

	for _, id := range []string{t1.ID, t2.ID, t3.ID} {
		tk, _ := k.Task(id)
		if tk.Status != domain.TaskCompleted {
			t.Fatalf("%s status=%s want completed", id, tk.Status)
		}
	}
	t.Log("result: dependency chain executed strictly in order")
}

// TestDependencyFailedBlocks 前驱失败时后继任务被拒绝/转失败，给出可区分原因。
func TestDependencyFailedBlocks(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")

	// 先构造一个已失败的前驱。
	t1, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "df1", From: "A", To: "F"})
	must2(t, err)
	for a := 0; a < domain.MaxAttempts; a++ {
		advanceManual(t, k, "r1", 2)
		tk, _ := k.Task(t1.ID)
		if tk.Status.IsTerminal() {
			break
		}
		_, err := k.ReportResult(ctx, t1.ID, "r1", domain.TaskResult{Success: false})
		must2(t, err)
	}
	t1s, _ := k.Task(t1.ID)
	if t1s.Status != domain.TaskFailed {
		t.Fatalf("setup: t1 status=%s", t1s.Status)
	}

	// 提交依赖它的新任务：提交期已存在的失败前驱 => dependency_failed。
	_, err = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "df2", From: "A", To: "E", Deps: []string{t1.ID},
	})
	if !dispatcher.IsCode(err, domain.CodeDependencyFailed) {
		t.Fatalf("submit depending on failed task err=%v want dependency_failed", err)
	}
	t.Logf("rejected with code=dependency_failed: %v", err)
}

// TestCapabilityFilter 能力要求：只有具备能力的机器人才能接单。
func TestCapabilityFilter(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "plain", "A")
	addRobot(t, k, "lifter", "A", "forklift")

	_, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "cap1", Priority: 1, From: "A", To: "F", Requires: []string{"forklift"},
	})
	must2(t, err)
	tickAll(t, k, []string{"plain", "lifter"}, 8)

	st := k.Snapshot()
	var got string
	for _, tk := range st.Tasks {
		got = tk.AssignedTo
		if tk.Status != domain.TaskCompleted {
			t.Fatalf("task status=%s", tk.Status)
		}
	}
	if got != "lifter" {
		t.Fatalf("task assigned to %s, want lifter", got)
	}
	t.Log("result: capability-requiring task assigned only to capable robot")
}

func advanceManual(t *testing.T, k *dispatcher.Kernel, robot string, n int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < n; i++ {
		must2(t, k.Heartbeat(ctx, robot))
		must2(t, k.Advance(ctx, 1))
	}
}
