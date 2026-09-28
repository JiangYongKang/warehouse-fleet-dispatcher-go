// wfdemo 是一个可本地运行的端到端演示：构造路网与机器人，提交一批带
// 优先级/依赖的任务，推进逻辑时钟，打印分派、等待、抢占、死锁打破、
// 掉线回收与完成等关键判定依据。
//
// 运行：go run ./cmd/wfdemo
// 可选：wfdemo -dir ./wfdata 可在目录中留存快照与操作日志用于重启恢复演示。
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/domain"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/network"
	"github.com/highcumontoa/warehouse-fleet-dispatcher-go/internal/storage"
)

func main() {
	dir := flag.String("dir", "", "状态目录（空则纯内存）")
	resume := flag.Bool("resume", false, "从目录中的快照恢复后只打印终态（不重新提交任务）")
	flag.Parse()

	ctx := context.Background()
	var store storage.Store
	if *dir != "" {
		if err := os.MkdirAll(*dir, 0o755); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		store = storage.NewFileStore(*dir)
	} else {
		store = storage.NewMemoryStore()
	}

	g := demoGraph()
	k, err := dispatcher.New(ctx, dispatcher.Config{Graph: g, Store: store})
	must(err)

	if *resume {
		// 仅验证从磁盘恢复后的状态。
		st := k.Snapshot()
		fmt.Printf("recovered clock=%d robots=%d tasks=%d\n", st.Clock, len(st.Robots), len(st.Tasks))
		for id, t := range st.Tasks {
			fmt.Printf("task=%s status=%-10s priority=%d attempts=%d assigned=%s\n",
				id, t.Status, t.Priority, t.Attempts, orDash(t.AssignedTo))
		}
		return
	}

	// 两台机器人对向停在单车道两端。
	mustAdd(k, ctx, "r1", "A")
	mustAdd(k, ctx, "r2", "D")
	// r3 停在 B：r1 在途中掉线后，任务回收并由位于当前位置的 r3 接手。
	mustAdd(k, ctx, "r3", "B")
	// r4 停在 C：r1 在 C 附近掉线后，任务由 r4 接手到 D。
	mustAdd(k, ctx, "r4", "C")

	// 高优先级任务与普通任务；第二个任务依赖第一个完成。
	t1, err := k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "demo-t1", Priority: 1, From: "A", To: "D",
	})
	must(err)
	_, err = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "demo-t2", Priority: 9, From: "D", To: "A",
	})
	must(err)
	_, err = k.SubmitTask(ctx, dispatcher.SubmitTaskInput{
		IdemKey: "demo-t3", Priority: 2, From: "A", To: "C", Deps: []string{t1.ID},
	})
	must(err)

	offlineAt := 2
	for tick := 0; tick < 24; tick++ {
		// r1 在 offlineAt 之后停止心跳 -> 掉线，任务安全回收。
		if tick < offlineAt {
			_ = k.Heartbeat(ctx, "r1")
		} else if tick == offlineAt {
			fmt.Println(">>> r1 heartbeat stops (simulated offline while yielding)")
		}
		_ = k.Heartbeat(ctx, "r2")
		_ = k.Heartbeat(ctx, "r3")
		_ = k.Heartbeat(ctx, "r4")
		must(k.Advance(ctx, 1))

		// 已到达终点的任务立即上报成功。
		st := k.Snapshot()
		for id, r := range st.Robots {
			if r.Status == domain.RobotExecuting && r.CurrentTask != "" {
				_, _ = k.ReportResult(ctx, r.CurrentTask, id, domain.TaskResult{
					Success: true, Note: "delivered",
				})
			}
		}
	}

	fmt.Println("\n=== key decisions ===")
	for _, d := range k.Decisions(0) {
		fmt.Printf("tick=%-3d %-16s %s\n", d.At, d.Kind, d.Detail)
	}
	fmt.Println("\n=== final tasks ===")
	st := k.Snapshot()
	for id, t := range st.Tasks {
		fmt.Printf("task=%s status=%-10s priority=%d attempts=%d assigned=%s reason=%s\n",
			id, t.Status, t.Priority, t.Attempts, orDash(t.AssignedTo), t.FailedReasonCode)
	}
	if *dir != "" {
		must(k.Checkpoint(ctx))
		fmt.Println("\nsnapshot written to", *dir)
	}
}

func demoGraph() *network.Graph {
	// 单车道 A-B-C-D（双向声明即视为互斥单车道）。
	edges := []network.Edge{}
	lanes := []network.Lane{}
	line := []string{"A", "B", "C", "D"}
	for i := 0; i+1 < len(line); i++ {
		edges = append(edges,
			network.Edge{From: line[i], To: line[i+1], Cost: 1},
			network.Edge{From: line[i+1], To: line[i], Cost: 1},
		)
		lanes = append(lanes, network.Lane{U: line[i], V: line[i+1]})
	}
	return network.NewGraphWithLanes(line, edges, lanes)
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func mustAdd(k *dispatcher.Kernel, ctx context.Context, id, node string) {
	_, err := k.AddRobot(ctx, id, node)
	must(err)
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}
