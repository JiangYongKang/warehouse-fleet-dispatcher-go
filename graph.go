package dispatcher

import (
	"sort"
	"sync"
)

// Node 是路网上的一个点位（站点/路口）。
type Node struct {
	ID   string
	Name string
}

// Edge 表示有向路段（同一路段的两个方向分别建模）。
// Length 为逻辑时间（tick）开销，用于路径规划。
type Edge struct {
	From   string
	To     string
	Length int // 通行所需 tick 数，必须 >= 1
}

// Graph 是线程安全的有向路网；构造完成后路径规划为只读操作。
type Graph struct {
	mu        sync.RWMutex
	nodes     map[string]*Node
	edges     map[[2]string]*Edge
	adj       map[string][]*Edge
	pathCache map[string][]*Edge // key: from+"\x00"+to
}

// NewGraph 创建空路网。
func NewGraph() *Graph {
	return &Graph{
		nodes:     map[string]*Node{},
		edges:     map[[2]string]*Edge{},
		adj:       map[string][]*Edge{},
		pathCache: map[string][]*Edge{},
	}
}

// AddNode 增加节点；重复增加返回 already_exists。
func (g *Graph) AddNode(n Node) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if n.ID == "" {
		return fail("Graph.AddNode", ErrInvalidArgument, "node id is empty")
	}
	if _, ok := g.nodes[n.ID]; ok {
		return fail("Graph.AddNode", ErrAlreadyExists, "node %q already exists", n.ID)
	}
	g.nodes[n.ID] = &n
	g.adj[n.ID] = nil
	g.pathCache = map[string][]*Edge{}
	return nil
}

// AddEdge 增加有向边，端点必须存在、长度为正且不重复。
func (g *Graph) AddEdge(e Edge) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e.Length < 1 {
		return fail("Graph.AddEdge", ErrInvalidArgument, "edge %s->%s length must be >= 1", e.From, e.To)
	}
	if e.From == "" || e.To == "" {
		return fail("Graph.AddEdge", ErrInvalidArgument, "edge endpoints must not be empty")
	}
	if _, ok := g.nodes[e.From]; !ok {
		return fail("Graph.AddEdge", ErrNotFound, "from node %q not found", e.From)
	}
	if _, ok := g.nodes[e.To]; !ok {
		return fail("Graph.AddEdge", ErrNotFound, "to node %q not found", e.To)
	}
	key := [2]string{e.From, e.To}
	if _, ok := g.edges[key]; ok {
		return fail("Graph.AddEdge", ErrAlreadyExists, "edge %s->%s already exists", e.From, e.To)
	}
	ec := e
	g.edges[key] = &ec
	g.adj[e.From] = append(g.adj[e.From], &ec)
	sort.Slice(g.adj[e.From], func(i, j int) bool { return g.adj[e.From][i].To < g.adj[e.From][j].To })
	g.pathCache = map[string][]*Edge{}
	return nil
}

// HasNode 判断节点是否存在。
func (g *Graph) HasNode(id string) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	_, ok := g.nodes[id]
	return ok
}

// Node 返回节点信息。
func (g *Graph) Node(id string) (Node, bool) {
	g.mu.RLock()
	defer g.mu.RUnlock()
	n, ok := g.nodes[id]
	if !ok {
		return Node{}, false
	}
	return *n, true
}

// Neighbors 返回从 from 出发的边（按 To 升序，保证规划结果确定）。
func (g *Graph) Neighbors(from string) []*Edge {
	g.mu.RLock()
	defer g.mu.RUnlock()
	src := g.adj[from]
	out := make([]*Edge, len(src))
	copy(out, src)
	return out
}

// ShortestPath 返回从 from 到 to 的确定性最短路径（Dijkstra，平局按节点 ID 字典序）。
// 无路径时返回 ErrNoPath；起点即终点返回空路径。
func (g *Graph) ShortestPath(from, to string) ([]*Edge, error) {
	return g.shortestPathCached(from, to)
}

// shortestPathImpl 是 Dijkstra 实现；forbidden 中的边不参与规划（供改道使用）。
// 平局规则与邻接顺序共同保证结果确定。
func (g *Graph) shortestPathImpl(from, to string, forbidden map[[2]string]bool) ([]*Edge, error) {
	const inf = 1<<62 - 1
	dist := map[string]int{}
	prev := map[string]*Edge{}
	visited := map[string]bool{}
	for id := range g.nodes {
		dist[id] = inf
	}
	dist[from] = 0

	for {
		u := ""
		best := inf
		cands := make([]string, 0)
		for id, d := range dist {
			if !visited[id] && d <= best {
				cands = append(cands, id)
			}
		}
		sort.Strings(cands)
		for _, id := range cands {
			if dist[id] < best {
				best = dist[id]
				u = id
			}
		}
		if u == "" {
			break
		}
		visited[u] = true
		if u == to {
			break
		}
		for _, e := range g.adj[u] {
			if forbidden != nil && forbidden[[2]string{e.From, e.To}] {
				continue
			}
			nd := dist[u] + e.Length
			if nd < dist[e.To] {
				dist[e.To] = nd
				prev[e.To] = e
			}
		}
	}

	if dist[to] == inf {
		return nil, fail("Graph.ShortestPath", ErrNoPath, "no path from %q to %q", from, to)
	}
	var path []*Edge
	for cur := to; cur != from; {
		e := prev[cur]
		if e == nil {
			return nil, fail("Graph.ShortestPath", ErrNoPath, "unreachable: %q", cur)
		}
		path = append(path, e)
		cur = e.From
	}
	for i, j := 0, len(path)-1; i < j; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
	return path, nil
}

// shortestPathCached 带缓存的最短路径查询（无禁边时）。
func (g *Graph) shortestPathCached(from, to string) ([]*Edge, error) {
	ck := pathKey(from, to)
	g.mu.RLock()
	if cached, ok := g.pathCache[ck]; ok {
		g.mu.RUnlock()
		out := make([]*Edge, len(cached))
		copy(out, cached)
		return out, nil
	}
	g.mu.RUnlock()

	p, err := g.shortestPathImpl(from, to, nil)
	if err != nil {
		return nil, err
	}
	g.mu.Lock()
	g.pathCache[ck] = p
	g.mu.Unlock()
	out := make([]*Edge, len(p))
	copy(out, p)
	return out, nil
}

// sortedNodes 返回全部节点 ID 升序（供引擎做确定性遍历）。
func (g *Graph) sortedNodes() []string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	ids := make([]string, 0, len(g.nodes))
	for id := range g.nodes {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

// pathKey 构造缓存键。
func pathKey(from, to string) string { return from + "\x00" + to }
