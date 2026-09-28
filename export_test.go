package dispatcher

// WaitEdgeForTest 是等待边的测试可见形式。
type WaitEdgeForTest struct{ From, To string }

// DetectCycleForTest 暴露内部确定性环检测给测试。
func DetectCycleForTest(edges []WaitEdgeForTest, nodes []string) []string {
	es := make([]waitsFor, 0, len(edges))
	for _, e := range edges {
		es = append(es, waitsFor{from: e.From, to: e.To})
	}
	return detectCycle(es, nodes)
}

// BreakDeadlockForTest 暴露内部牺牲者选择给测试。
func BreakDeadlockForTest(cycle []string, effPri func(string) int, seq func(string) int64) string {
	return breakDeadlock(cycle, effPri, seq)
}
