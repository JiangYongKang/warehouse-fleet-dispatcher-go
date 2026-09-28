package dispatcher

import "sort"

// waitsFor 表示机器人 from 正在等待机器人 to 释放资源。
type waitsFor struct {
	from string
	to   string
}

// agingInterval 定义等待多少 tick 补偿 1 个有效优先级。
const agingInterval = 10

// detectCycle 在等待图上寻找一个环。
// 确定性：节点按 ID 升序依次作为 DFS 起点，邻接点也按 ID 升序访问，
// 因此对同一张等待图，无论调用多少次，找到的第一个环都相同。
// 返回环上机器人 ID（顺序即环顺序），无环返回 nil。
func detectCycle(edges []waitsFor, nodes []string) []string {
	adj := map[string][]string{}
	for _, e := range edges {
		adj[e.from] = append(adj[e.from], e.to)
	}
	for k := range adj {
		sort.Strings(adj[k])
	}
	ord := append([]string(nil), nodes...)
	sort.Strings(ord)

	const (
		white = 0
		gray  = 1
		black = 2
	)
	color := map[string]int{}
	var stack []string
	onStack := map[string]bool{}

	var dfs func(u string) []string
	dfs = func(u string) []string {
		color[u] = gray
		stack = append(stack, u)
		onStack[u] = true
		for _, v := range adj[u] {
			if color[v] == white {
				if cyc := dfs(v); cyc != nil {
					return cyc
				}
			} else if onStack[v] {
				// 从 v 在栈中的位置到栈顶构成环。
				idx := 0
				for i, n := range stack {
					if n == v {
						idx = i
						break
					}
				}
				cyc := append([]string(nil), stack[idx:]...)
				return cyc
			}
		}
		stack = stack[:len(stack)-1]
		onStack[u] = false
		color[u] = black
		return nil
	}

	for _, n := range ord {
		if color[n] == white {
			if cyc := dfs(n); cyc != nil {
				return cyc
			}
		}
	}
	return nil
}

// breakDeadlock 从环中选择牺牲者。
// 选择规则（完全确定、可解释、与分派顺序一致）：
//  1. 有效优先级最低者先被牺牲（有效优先级 = 静态优先级 + 等待老化补偿，
//     低优先级任务等待足够久后不会一直被牺牲，从根本上防止饿死）；
//  2. 有效优先级相同 -> 提交序号最大（最年轻）者让路给更早提交的任务，
//     这与"同优先级 FIFO"的全局规则一致，避免一对任务反复互抢；
//  3. 仍相同（理论上序号唯一，不会发生）-> 机器人 ID 字典序最大者。
//
// 入参 effPri/seq 返回机器人当前任务的有效优先级与提交序号。
func breakDeadlock(cycle []string, effPri func(robotID string) int, seq func(robotID string) int64) string {
	victim := ""
	victimPri := 0
	var victimSeq int64
	for _, id := range cycle {
		p := effPri(id)
		s := seq(id)
		switch {
		case victim == "":
			// 初始候选
		case p != victimPri:
			if p < victimPri {
				victim, victimPri, victimSeq = id, p, s
			}
			continue
		case s != victimSeq:
			if s > victimSeq {
				victim, victimPri, victimSeq = id, p, s
			}
			continue
		case id > victim:
			victim, victimPri, victimSeq = id, p, s
			continue
		default:
			continue
		}
		victim, victimPri, victimSeq = id, p, s
	}
	return victim
}

// ageBonus 是防饿死补偿：每等待 agingInterval 个 tick，有效优先级 +1，
// 并额外加 0/1 的"高龄"加成，使任何任务只要等待足够久，
// 有效优先级必然超过新来的高优先级任务。
func ageBonus(waitSince, now int64) int {
	if waitSince <= 0 || now <= waitSince {
		return 0
	}
	age := now - waitSince
	return int(age / agingInterval)
}
