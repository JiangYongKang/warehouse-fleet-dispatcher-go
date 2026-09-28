package network

import (
	"reflect"
	"testing"
)

func demoGraph() *Graph {
	//    A --1-- B --1-- C
	//    |       |       |
	//    1       1       1
	//    |       |       |
	//    D --1-- E --1-- F
	return NewGraph(
		[]string{"A", "B", "C", "D", "E", "F"},
		[]Edge{
			{From: "A", To: "B", Cost: 1}, {From: "B", To: "C", Cost: 1},
			{From: "A", To: "D", Cost: 1}, {From: "D", To: "E", Cost: 1}, {From: "B", To: "E", Cost: 1},
			{From: "C", To: "F", Cost: 1}, {From: "E", To: "F", Cost: 1},
			// 反向
			{From: "B", To: "A", Cost: 1}, {From: "C", To: "B", Cost: 1}, {From: "D", To: "A", Cost: 1},
			{From: "E", To: "D", Cost: 1}, {From: "E", To: "B", Cost: 1}, {From: "F", To: "C", Cost: 1}, {From: "F", To: "E", Cost: 1},
		},
	)
}

func TestShortest(t *testing.T) {
	g := demoGraph()
	p, c := g.Shortest("A", "F")
	if c != 3 {
		t.Fatalf("cost = %d, want 3, path=%v", c, p)
	}
	t.Logf("A->F shortest=%v cost=%d", p, c)
	if g.HasNode("X") {
		t.Fatal("X should not exist")
	}
	if _, c2 := g.Shortest("A", "X"); c2 != -1 {
		t.Fatalf("unreachable cost = %d, want -1", c2)
	}
}

func TestKShortest(t *testing.T) {
	g := demoGraph()
	paths := g.KShortest("A", "F", 3)
	if len(paths) < 2 {
		t.Fatalf("expected >=2 paths, got %d: %v", len(paths), paths)
	}
	for _, p := range paths {
		t.Logf("path=%v cost=%d", p, pathCost(g, p))
	}
	// 再次调用应得到同样顺序（确定性）
	paths2 := g.KShortest("A", "F", 3)
	if !reflect.DeepEqual(paths, paths2) {
		t.Fatalf("non-deterministic: %v vs %v", paths, paths2)
	}
	// 所有路径都应是简单路径且代价非递减
	prev := -1
	for _, p := range paths {
		c := pathCost(g, p)
		if c < prev {
			t.Fatalf("costs not non-decreasing: %v", paths)
		}
		prev = c
		seen := map[string]bool{}
		for _, n := range p {
			if seen[n] {
				t.Fatalf("non-simple path %v", p)
			}
			seen[n] = true
		}
	}
}
