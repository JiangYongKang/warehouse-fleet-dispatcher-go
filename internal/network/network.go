package network

import (
	"container/heap"
	"math"
	"sort"
	"strings"
)

// Edge 有向边。Cost 为经过该边需要的逻辑 tick 数（必须 >= 1）。
type Edge struct {
	From string `json:"from"`
	To   string `json:"to"`
	Cost int    `json:"cost"`
}

// Lane 声明一段不可分割的单车道：两个方向在同一时间窗内互斥占用。
// 不声明为 Lane 的普通双向边允许对向同时通行。
type Lane struct {
	U string `json:"u"`
	V string `json:"v"`
}

// Graph 本地路网（有向，允许双向分别声明）。
type Graph struct {
	Nodes []string `json:"nodes"`
	Edges []Edge   `json:"edges"`
	Lanes []Lane   `json:"lanes,omitempty"`

	adj  map[string]map[string]int
	node map[string]struct{}
	lane map[[2]string]bool
}

// NewGraph 由节点与边集合构造路网，并建立索引。
func NewGraph(nodes []string, edges []Edge) *Graph {
	g := &Graph{Nodes: append([]string(nil), nodes...), Edges: append([]Edge(nil), edges...)}
	g.buildIndex()
	return g
}

// NewGraphWithLanes 构造带显式单车道声明的路网。
func NewGraphWithLanes(nodes []string, edges []Edge, lanes []Lane) *Graph {
	g := &Graph{
		Nodes: append([]string(nil), nodes...),
		Edges: append([]Edge(nil), edges...),
		Lanes: append([]Lane(nil), lanes...),
	}
	g.buildIndex()
	return g
}

func (g *Graph) buildIndex() {
	g.adj = map[string]map[string]int{}
	g.node = map[string]struct{}{}
	for _, n := range g.Nodes {
		g.node[n] = struct{}{}
		if g.adj[n] == nil {
			g.adj[n] = map[string]int{}
		}
	}
	for _, e := range g.Edges {
		if e.Cost < 1 {
			e.Cost = 1
		}
		g.node[e.From] = struct{}{}
		g.node[e.To] = struct{}{}
		if g.adj[e.From] == nil {
			g.adj[e.From] = map[string]int{}
		}
		// 平行边保留代价最小者
		if c, ok := g.adj[e.From][e.To]; !ok || e.Cost < c {
			g.adj[e.From][e.To] = e.Cost
		}
		if g.adj[e.To] == nil {
			g.adj[e.To] = map[string]int{}
		}
	}
	g.lane = map[[2]string]bool{}
	for _, l := range g.Lanes {
		a, b := l.U, l.V
		if a > b {
			a, b = b, a
		}
		g.lane[[2]string{a, b}] = true
	}
}

// ensureIndex 在 JSON 反序列化后惰性重建索引（调用需在串行区内）。
func (g *Graph) ensureIndex() {
	if g.adj == nil || g.node == nil {
		g.buildIndex()
	}
}

// HasNode 判断节点是否存在。
func (g *Graph) HasNode(id string) bool {
	g.ensureIndex()
	_, ok := g.node[id]
	return ok
}

// EdgeCost 返回有向边成本，不存在返回 (0,false)。
func (g *Graph) EdgeCost(u, v string) (int, bool) {
	g.ensureIndex()
	c, ok := g.adj[u][v]
	return c, ok
}

// ListNodes 返回全部节点（确定性字典序）。
func (g *Graph) ListNodes() []string {
	g.ensureIndex()
	out := make([]string, 0, len(g.node))
	for n := range g.node {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// neighbors 返回 from 的出边目标（字典序），供路径算法确定性遍历。
func (g *Graph) neighbors(from string) []edgeCost {
	g.ensureIndex()
	m := g.adj[from]
	outs := make([]edgeCost, 0, len(m))
	for to, c := range m {
		outs = append(outs, edgeCost{to, c})
	}
	sort.Slice(outs, func(i, j int) bool { return outs[i].to < outs[j].to })
	return outs
}

type edgeCost struct {
	to   string
	cost int
}

type dijkstraItem struct {
	node string
	cost int
}

type pq []dijkstraItem

func (q pq) Len() int { return len(q) }
func (q pq) Less(i, j int) bool {
	if q[i].cost != q[j].cost {
		return q[i].cost < q[j].cost
	}
	return q[i].node < q[j].node
}
func (q pq) Swap(i, j int)       { q[i], q[j] = q[j], q[i] }
func (q *pq) Push(x interface{}) { *q = append(*q, x.(dijkstraItem)) }
func (q *pq) Pop() interface{} {
	old := *q
	n := len(old)
	it := old[n-1]
	*q = old[:n-1]
	return it
}

// Shortest 返回代价最小的节点路径与总代价；不可达返回 (nil, math.MaxInt 语义下的 -1)。
func (g *Graph) Shortest(from, to string) ([]string, int) {
	paths := g.KShortest(from, to, 1)
	if len(paths) == 0 {
		return nil, -1
	}
	return paths[0], pathCost(g, paths[0])
}

func pathCost(g *Graph, p []string) int {
	total := 0
	for i := 0; i+1 < len(p); i++ {
		c, ok := g.EdgeCost(p[i], p[i+1])
		if !ok {
			return math.MaxInt
		}
		total += c
	}
	return total
}

// dijkstra 在给定禁用节点/边条件下求最短路径（Yen 的 spur 计算复用）。
func (g *Graph) dijkstraConstrained(from, to string, bannedNodes map[string]bool, bannedEdges map[[2]string]bool) []string {
	if bannedNodes[from] || bannedNodes[to] {
		return nil
	}
	dist := map[string]int{from: 0}
	prev := map[string]string{}
	q := &pq{{from, 0}}
	heap.Init(q)
	for q.Len() > 0 {
		cur := heap.Pop(q).(dijkstraItem)
		if cur.node == to {
			// 重建路径
			path := []string{to}
			for n := to; n != from; {
				p, ok := prev[n]
				if !ok {
					return nil
				}
				n = p
				path = append([]string{n}, path...)
			}
			return path
		}
		if cur.cost > dist[cur.node] {
			continue
		}
		for _, e := range g.neighbors(cur.node) {
			if bannedNodes[e.to] {
				continue
			}
			if bannedEdges[[2]string{cur.node, e.to}] {
				continue
			}
			nd := cur.cost + e.cost
			if d, ok := dist[e.to]; !ok || nd < d {
				dist[e.to] = nd
				prev[e.to] = cur.node
				heap.Push(q, dijkstraItem{e.to, nd})
			}
		}
	}
	return nil
}

// KShortest 返回最多 k 条代价非递减的不同节点路径（Yen 算法）。
// 相同代价时按路径节点序列字典序排序，保证确定性。
func (g *Graph) KShortest(from, to string, k int) [][]string {
	if k <= 0 || !g.HasNode(from) || !g.HasNode(to) {
		return nil
	}
	if from == to {
		return [][]string{{from}}
	}
	first := g.dijkstraConstrained(from, to, nil, nil)
	if first == nil {
		return nil
	}
	A := [][]string{first}
	var B [][]string // 候选，带代价排序
	seen := map[string]bool{pathKey(first): true}

	for len(A) < k {
		prevPath := A[len(A)-1]
		for i := 0; i < len(prevPath)-1; i++ {
			spurNode := prevPath[i]
			rootPath := append([]string(nil), prevPath[:i+1]...)

			bannedEdges := map[[2]string]bool{}
			for _, p := range A {
				if i+1 < len(p) && pathKey(p[:i+1]) == pathKey(rootPath) {
					bannedEdges[[2]string{p[i], p[i+1]}] = true
				}
			}
			bannedNodes := map[string]bool{}
			for _, n := range rootPath[:len(rootPath)-1] {
				bannedNodes[n] = true
			}
			spur := g.dijkstraConstrained(spurNode, to, bannedNodes, bannedEdges)
			if spur == nil {
				continue
			}
			total := append(append([]string(nil), rootPath[:len(rootPath)-1]...), spur...)
			key := pathKey(total)
			if seen[key] {
				continue
			}
			seen[key] = true
			B = append(B, total)
		}
		if len(B) == 0 {
			break
		}
		// 选取代价最小、字典序最小的候选
		sort.Slice(B, func(i, j int) bool {
			ci, cj := pathCost(g, B[i]), pathCost(g, B[j])
			if ci != cj {
				return ci < cj
			}
			return pathKey(B[i]) < pathKey(B[j])
		})
		best := B[0]
		B = B[1:]
		A = append(A, best)
	}
	return A
}

func pathKey(p []string) string { return strings.Join(p, ">") }

// IsLane 判断有向边是否属于显式声明的单车道（双向互斥）。
func (g *Graph) IsLane(u, v string) bool {
	g.ensureIndex()
	a, b := u, v
	if a > b {
		a, b = b, a
	}
	return g.lane[[2]string{a, b}]
}

// LaneCount 返回声明的单车道数量。
func (g *Graph) LaneCount() int {
	g.ensureIndex()
	return len(g.lane)
}
