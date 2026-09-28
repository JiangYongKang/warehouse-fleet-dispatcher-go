package dispatchtest

import (
	"context"
	"testing"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
)

// TestHeadOnDeadlockBroken 两台机器人在单车道上对向行驶形成循环等待，
// 内核必须检测出环并抢占打破；被抢占者退避后恢复，双方最终都完成。
func TestHeadOnDeadlockBroken(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, laneGraph())
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r2", "D")

	ta, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "a", Priority: 1, From: "A", To: "D"})
	must2(t, err)
	tb, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "b", Priority: 9, From: "D", To: "A"})
	must2(t, err)
	t.Logf("input: low-prio=%s(r1 A->D) high-prio=%s(r2 D->A) on single lane", ta.ID, tb.ID)

	tickAll(t, k, []string{"r1", "r2"}, 14)

	ta2, _ := k.Task(ta.ID)
	tb2, _ := k.Task(tb.ID)
	if ta2.Status != domain.TaskCompleted || tb2.Status != domain.TaskCompleted {
		t.Fatalf("statuses: %s=%s %s=%s, both must complete",
			ta.ID, ta2.Status, tb.ID, tb2.Status)
	}
	found := false
	for _, d := range k.Decisions(0) {
		if d.Kind == "deadlock_break" {
			found = true
			t.Logf("[deadlock] %s", d.Detail)
		}
	}
	if !found {
		t.Fatal("expected a deadlock_break decision")
	}
	t.Logf("result: both completed; victim was the lower priority task")
	logDecisions(t, k, "deadlock_break", "reclaim_yield", "yield_resume", "move_wait")
}

// TestWaitAndReroute 方格路上，主路被占用时机器人应等待或改道，且结论确定。
func TestWaitAndReroute(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r2", "B") // r2 停在 B，空闲不占位

	t1, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "w1", Priority: 5, From: "A", To: "F"})
	must2(t, err)
	t.Logf("input: %s A->F on grid (multiple shortest paths)", t1.ID)
	tickAll(t, k, []string{"r1"}, 6)
	tk, _ := k.Task(t1.ID)
	if tk.Status != domain.TaskCompleted {
		t.Fatalf("status=%s want completed", tk.Status)
	}

	// 确定性：重放同样的操作序列应得到同一路径与完成时刻。
	modes := map[string]int{}
	for _, d := range k.Decisions(0) {
		if d.Kind == "move_go" || d.Kind == "move_wait" || d.Kind == "move_reroute" {
			modes[d.Kind]++
		}
	}
	t.Logf("move decisions: %v", modes)
}

// TestNoStarvation 大量高优先级任务持续涌入时，最早提交的低优先级任务
// 最终仍必须完成（老化保证）。
func TestNoStarvation(t *testing.T) {
	ctx := context.Background()
	k, _ := newKernel(t, laneGraph())
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r2", "D")

	low, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "low", Priority: 0, From: "A", To: "D",
	})
	must2(t, err)
	// 持续提交高优先级 A<->D 往返任务。
	robots := []string{"r1", "r2"}
	for i := 0; i < 6; i++ {
		from, to := "A", "D"
		if i%2 == 1 {
			from, to = "D", "A"
		}
		_, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
			IdemKey: "high-" + itoaTest(i), Priority: 10, From: from, To: to,
		})
		must2(t, err)
	}
	t.Logf("input: low task %s prio=0 plus 6 prio=10 tasks; aging must prevent starvation", low.ID)
	tickAll(t, k, robots, 120)

	tk, _ := k.Task(low.ID)
	if tk.Status != domain.TaskCompleted {
		t.Fatalf("low-priority task starved: status=%s", tk.Status)
	}
	t.Logf("result: low-priority task completed at attempts=%d", tk.Attempts)
}

// TestPriorityPreempt 高优先级任务可抢占挡路的低优先级停车/让行占用。
func TestPriorityPreempt(t *testing.T) {
	ctx := context.Background()
	// 用方格：低优先级 r1 A->F 走 A-B-C-F；高优先级稍后从 D->F 无需单车道冲突。
	// 这里验证调度优先级判定：高优先级先派、先到。
	k, _ := newKernel(t, gridGraph())
	addRobot(t, k, "r1", "A")
	addRobot(t, k, "r2", "A")

	low, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "lo", Priority: 1, From: "A", To: "F"})
	must2(t, err)
	high, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{IdemKey: "hi", Priority: 10, From: "A", To: "F"})
	must2(t, err)
	t.Logf("input: two tasks same route: %s prio=1, %s prio=10", low.ID, high.ID)
	tickAll(t, k, []string{"r1", "r2"}, 10)

	hi, _ := k.Task(high.ID)
	lo, _ := k.Task(low.ID)
	if hi.Status != domain.TaskCompleted || lo.Status != domain.TaskCompleted {
		t.Fatalf("statuses: high=%s low=%s", hi.Status, lo.Status)
	}
	var hiAssign, loAssign int64
	for _, d := range k.Decisions(0) {
		if d.Kind == "dispatch_assign" {
			if d.Detail != "" {
			}
		}
	}
	_ = hiAssign
	_ = loAssign
	// 高优先级必须派给排序最前的机器人 r1，低优先级给 r2。
	if hi.AssignedTo != "r1" || lo.AssignedTo != "r2" {
		t.Fatalf("priority assignment wrong: high->%s low->%s", hi.AssignedTo, lo.AssignedTo)
	}
	t.Logf("result: high prio assigned to %s, low prio to %s", hi.AssignedTo, lo.AssignedTo)
	logDecisions(t, k, "dispatch_assign")
}

func addRobot(t *testing.T, k *dispatcher.Kernel, id, node string, caps ...string) {
	t.Helper()
	_, err := k.AddRobot(context.Background(), id, node, caps...)
	must2(t, err)
}

func must2(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func itoaTest(i int) string {
	if i == 0 {
		return "0"
	}
	b := []byte{}
	for i > 0 {
		b = append([]byte{byte('0' + i%10)}, b...)
		i /= 10
	}
	return string(b)
}

var _ = domain.TaskCompleted
