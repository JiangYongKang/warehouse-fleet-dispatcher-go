// 本地可运行演示：构建一个含环线的小型仓储路网，
// 提交带优先级与前置依赖的任务，推进逻辑时钟观察分派、预约争用、
// 对头死锁检测与打破、掉线重派，最后落盘 -> 重启恢复 -> 重放一致性校验。
//
// 运行：go run ./cmd/demo
package main

import (
	"fmt"
	"log"
	"os"
	"path/filepath"

	dispatcher "github.com/highcumontoa/warehouse-fleet-dispatcher-go"
)

type stdLogger struct{}

func (stdLogger) Printf(format string, args ...any) {
	fmt.Printf("%s\n", fmt.Sprintf(format, args...))
}

func buildGraph() *dispatcher.Graph {
	// 环线：A-B-C-D-A，另加 B-D 对角与 A-S 支线（双向），制造可争用车道。
	g := dispatcher.NewGraph()
	for _, n := range []string{"A", "B", "C", "D", "S"} {
		must(g.AddNode(dispatcher.Node{ID: n, Name: "node-" + n}))
	}
	edges := [][3]any{
		{"A", "B", 2}, {"B", "A", 2},
		{"B", "C", 2}, {"C", "B", 2},
		{"C", "D", 2}, {"D", "C", 2},
		{"D", "A", 3}, {"A", "D", 3},
		{"B", "D", 4}, {"D", "B", 4},
		{"S", "A", 2}, {"A", "S", 2},
	}
	for _, e := range edges {
		must(g.AddEdge(dispatcher.Edge{From: e[0].(string), To: e[1].(string), Length: e[2].(int)}))
	}
	return g
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	dir, err := os.MkdirTemp("", "fleet-demo-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)
	snapPath := filepath.Join(dir, "snapshot.json")

	eng, err := dispatcher.NewEngine(buildGraph(),
		dispatcher.WithLogger(stdLogger{}),
		dispatcher.WithStore(dispatcher.NewFileStore(snapPath)),
	)
	if err != nil {
		log.Fatal(err)
	}

	must(eng.RegisterRobot("r1", "A"))
	must(eng.RegisterRobot("r2", "B"))
	must(eng.RegisterRobot("r3", "S"))

	// t1 低优先级搬运；t2 高优先级对向（制造对头争用）；t3 依赖 t1。
	must(eng.SubmitTask(dispatcher.Task{ID: "t1", From: "A", To: "C", Priority: 1}))
	must(eng.SubmitTask(dispatcher.Task{ID: "t2", From: "C", To: "A", Priority: 9}))
	must(eng.SubmitTask(dispatcher.Task{ID: "t3", From: "S", To: "D", Priority: 5, Deps: []string{"t1"}}))

	// 自动推进并对到达的任务上报成功；中途让 r3 掉线再上线。
	complete := func() {
		for _, ts := range eng.Tasks() {
			if ts.Status != dispatcher.TaskAssigned {
				continue
			}
			r, err := eng.RobotSnapshot(ts.Assignee)
			if err != nil {
				continue
			}
			if r.Node == ts.To {
				_ = eng.ReportResult(dispatcher.TickReport{
					TaskID: ts.Task.ID, RobotID: ts.Assignee, Success: true,
				})
			}
		}
	}

	for tk := int64(1); tk <= 120; tk++ {
		if tk == 8 {
			if err := eng.MarkOffline("r3"); err != nil {
				log.Fatal(err)
			}
		}
		if tk == 12 {
			if err := eng.MarkOnline("r3"); err != nil {
				log.Fatal(err)
			}
		}
		if _, err := eng.AssignOnce(tk); err != nil {
			log.Fatal(err)
		}
		complete()
	}
	must(eng.Close())

	// 重启恢复：重新构建同一路网，从快照恢复，校验终态任务不丢、assigned 已回收。
	eng2, err := dispatcher.NewEngine(buildGraph(),
		dispatcher.WithLogger(stdLogger{}),
		dispatcher.WithStore(dispatcher.NewFileStore(snapPath)),
	)
	if err != nil {
		log.Fatal(err)
	}
	if err := eng2.Recover(); err != nil {
		log.Fatal(err)
	}
	fmt.Println("---- state after recovery ----")
	for _, t := range eng2.Tasks() {
		fmt.Printf("task %s: status=%s assignee=%s attempt=%d\n", t.ID, t.Status, t.Assignee, t.Attempt)
	}
	for _, r := range eng2.Robots() {
		fmt.Printf("robot %s: status=%s node=%s task=%s\n", r.ID, r.Status, r.Node, r.CurrentTask)
	}
	must(eng2.Close())
	fmt.Println("demo finished; snapshot at", snapPath)
}
