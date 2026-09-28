package planning

import (
	"testing"
)

func TestDetectCycles(t *testing.T) {
	// A->B->C->A 成环；D->A 是环外等待者；E->F->E 是第二个独立环。
	edges := map[string]map[string]string{
		"A": {"B": "edge:A:B"},
		"B": {"C": "edge:B:C"},
		"C": {"A": "edge:C:A"},
		"D": {"A": "edge:D:A"},
		"E": {"F": "edge:E:F"},
		"F": {"E": "edge:F:E"},
	}
	cycles := DetectCycles(edges)
	if len(cycles) != 2 {
		t.Fatalf("cycles=%d want 2: %+v", len(cycles), cycles)
	}
	t.Logf("detected cycles: %+v", cycles)
	// 每个环必须闭合（同集合），且包含 A/B/C 与 E/F 两组。
	sets := []map[string]bool{}
	for _, c := range cycles {
		m := map[string]bool{}
		for _, r := range c.Robots {
			m[r] = true
		}
		sets = append(sets, m)
	}
	found := func(target ...string) bool {
		tm := map[string]bool{}
		for _, x := range target {
			tm[x] = true
		}
		for _, m := range sets {
			if len(m) == len(tm) {
				ok := true
				for x := range tm {
					if !m[x] {
						ok = false
					}
				}
				if ok {
					return true
				}
			}
		}
		return false
	}
	if !found("A", "B", "C") || !found("E", "F") {
		t.Fatalf("wrong cycle sets: %+v", sets)
	}
	// 无环图应返回空。
	noCycle := map[string]map[string]string{
		"A": {"B": "x"},
		"B": {"C": "x"},
		"D": {"A": "x"},
	}
	if got := DetectCycles(noCycle); len(got) != 0 {
		t.Fatalf("expected no cycle, got %+v", got)
	}
}

func TestWindowsOverlap(t *testing.T) {
	cases := []struct {
		a0, a1, b0, b1 int64
		want           bool
	}{
		{0, 2, 2, 4, false}, // 端点相接不重叠（可交接）
		{0, 2, 1, 3, true},
		{2, 4, 0, 2, false},
		{5, 6, 5, 6, true},
		{10, 12, 0, 10, false},
	}
	for _, c := range cases {
		if got := WindowsOverlap(c.a0, c.a1, c.b0, c.b1); got != c.want {
			t.Fatalf("overlap(%d,%d,%d,%d)=%v want %v", c.a0, c.a1, c.b0, c.b1, got, c.want)
		}
	}
}
