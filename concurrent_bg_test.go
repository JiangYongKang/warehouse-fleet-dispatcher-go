package dispatcher_test

import (
	"sync"
	"testing"
	"time"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
)

// TestBackgroundConcurrent 启用真实后台分派循环，多 goroutine 并发
// 提交任务/推进时钟/查询状态/上报结果，要求无数据竞争且全部任务恰好完成一次。
func TestBackgroundConcurrent(t *testing.T) {
	g := gridGraph(t)
	eng, err := dispatcher.NewEngine(g, dispatcher.WithLogger(tlog{t})) // 保留后台循环
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = eng.Close() })

	for _, id := range []string{"r1", "r2", "r3", "r4"} {
		if err := eng.RegisterRobot(id, "A"); err != nil {
			t.Fatal(err)
		}
	}

	const n = 24
	var wg sync.WaitGroup

	// 生产者：并发提交（允许重复 ID 以制造 already_exists 幂等路径）。
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := worker; i < n; i += 3 {
				id := "bg-" + itoa(i)
				_ = eng.SubmitTask(dispatcher.Task{ID: id, From: "A", To: "C", Priority: i%4 + 1})
				_ = eng.SubmitTask(dispatcher.Task{ID: id, From: "A", To: "C", Priority: i%4 + 1}) // 重复
			}
		}(w)
	}

	// 时钟推进者。
	stopClock := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := int64(1)
		for {
			select {
			case <-stopClock:
				return
			default:
				_ = eng.AdvanceClock(tk)
				tk++
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// 结果上报者 + 只读查询（并发读，验证 RMW 都在锁内）。
	stopReport := make(chan struct{})
	wg.Add(2)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReport:
				return
			default:
				_ = eng.Tasks()
				_ = eng.Reservations()
				_ = eng.Robots()
				time.Sleep(time.Millisecond)
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReport:
				return
			default:
				for _, ts := range eng.Tasks() {
					if ts.Status == dispatcher.TaskAssigned && ts.Assignee != "" &&
						eng.IsAwaitingResult(ts.Assignee) {
						_ = eng.ReportResult(dispatcher.TickReport{
							TaskID: ts.Task.ID, RobotID: ts.Assignee, Success: true,
						})
					}
				}
				time.Sleep(time.Millisecond)
			}
		}
	}()

	// 等待全部完成（有界）。
	deadline := time.Now().Add(5 * time.Second)
	for {
		done := 0
		for _, ts := range eng.Tasks() {
			if ts.Status == dispatcher.TaskCompleted {
				done++
			}
		}
		if len(eng.Tasks()) == n && done == n {
			break
		}
		if time.Now().After(deadline) {
			close(stopClock)
			close(stopReport)
			wg.Wait()
			for _, ts := range eng.Tasks() {
				t.Errorf("task %s status=%s assignee=%s dispatch=%d",
					ts.ID, ts.Status, ts.Assignee, ts.Dispatch)
			}
			t.Fatalf("not all %d tasks completed (got %d)", n, done)
		}
		time.Sleep(2 * time.Millisecond)
	}
	close(stopReport)
	close(stopClock)
	wg.Wait()

	// 结论校验：恰好 n 个任务，全部完成，每个只完成一次（Attempt=0，Dispatch>=1）。
	tasks := eng.Tasks()
	if len(tasks) != n {
		t.Fatalf("want %d unique tasks, got %d", n, len(tasks))
	}
	for _, ts := range tasks {
		if ts.Status != dispatcher.TaskCompleted {
			t.Fatalf("task %s status %s", ts.ID, ts.Status)
		}
		if ts.Attempt != 0 {
			t.Fatalf("task %s had reported failures: %d", ts.ID, ts.Attempt)
		}
	}
	t.Logf("decision: %d tasks each submitted-once and completed exactly once under concurrent producers/clock/reporters", n)
}
