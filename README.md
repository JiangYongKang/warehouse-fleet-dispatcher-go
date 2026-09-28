# 仓储多机器人任务分派与路径预约内核

一个可在本地运行的、**结论确定且可解释**的仓储车队调度内核。它用本地路网数据和本地
状态存储模拟多机器人调度，覆盖并发派单、共享路网路径预约与冲突消解、机器人掉线、
执行失败与重启恢复等场景。

内核不依赖墙钟与外部系统：所有时间都是单调递增的**逻辑时钟（tick）**，所有对外变更
操作都被串行化并写入 **append-only 操作日志（WAL）**。因此对同一批操作，无论执行
多少遍、是否经过重启，最终状态都完全一致。

## 目录结构

```
kernel.go                 内核入口：串行锁、操作日志、重放恢复、任务流转 API
internal/domain/          领域模型、状态、错误码、优先级/资源键
internal/network/         本地路网：Dijkstra 最短路、Yen K 最短路、显式单车道
internal/planning/        路径资源、时间窗重叠、wait-for 死锁环检测
internal/scheduler/       分派、逐段预约、等待/改道/抢占、死锁打破、掉线回收
internal/storage/         MemoryStore 与 FileStore（原子快照 + JSONL WAL）
internal/dispatchtest/    端到端/并发/死锁/恢复/兼容测试
cmd/wfdemo/               本地可运行的端到端演示
```

## 快速开始

```bash
# 端到端演示（纯内存）：分派、死锁、让行恢复、掉线回收、他机接手
go run ./cmd/wfdemo

# 带文件持久化：运行结束后写出快照
go run ./cmd/wfdemo -dir ./wfdata

# 从磁盘快照恢复并打印终态（验证重启恢复）
go run ./cmd/wfdemo -dir ./wfdata -resume
```

## 验证方法

```bash
# 全量测试（含 -race 数据竞争检测）
go test ./... -race

# 对并发用例做稳定性压测（反复运行，要求无 flake、无 data race）
go test ./internal/dispatchtest/ -race -count=30 -run TestConcurrent

# 只跑死锁/恢复相关
go test ./internal/dispatchtest/ -v -run 'TestHeadOn|TestRestart|TestOffline|TestState'
```

所有测试在关键判定点通过 `t.Logf` 打印**输入与判定依据**（分派的优先级/路径、
等待的占用者、抢占的受害者、死锁环成员、回收原因与尝试次数等），用 `-v` 可查看。

---

## 一、任务分派规则

### 任务与机器人

- 任务包含：基础**优先级**（数值越大越优先）、取货点 `From`、送货点 `To`、
  **前置依赖** `Deps`、能力要求 `Requires`、**幂等键** `IdemKey`。
- 机器人包含：当前节点、能力集合 `Caps`、状态与最近心跳。
- 机器人完成任务时**不必位于取货点**：它会先从当前节点**空驶**到 `From` 取货
  （路径的“取货腿”），再载货前往 `To`（“送货腿”）。已取货状态 `PickedUp`
  被显式记录。

### 分派条件

一个 `pending` 任务只有在以下条件**全部**满足时才会被分派：

1. 所有前置依赖都已 `completed`；任一依赖 `failed` 则任务被拒绝/转失败，
   原因码为 `dependency_failed`；依赖未完成则继续等待（`dependency_pending`）。
2. 存在**在线、空闲、能力满足**的机器人。分派器选“空驶 + 送货”总代价最小的
   机器人，代价相同取 ID 最小（确定性）。
3. 从机器人位置到取货点、取货点到终点均可达。

机器人若已位于终点，则任务作为**零里程任务**直接进入 `executing` 等待结果上报。

### 优先级与防饿死

- 调度顺序按**有效优先级**排序：`effPrio = base + 已等待 tick 数`。
- 处于 blocked/waiting 的机器人在有效优先级上还会按其持续等待时长获得额外加权。
- 因此一个低优先级任务等待足够久后，其有效优先级必然超过后到的高优先级任务，
  **不会被无限期饿死**（见 `TestNoStarvation`）。

### 状态流转与拒绝原因码

```
pending ──assign──▶ assigned ──first move──▶ running ──arrive──▶ executing ──success report──▶ completed
  ▲                   │                          │
  │                   └──── fail/offline/preempt ┴──▶ pending（可重新分派）
  └──────────────────────── （尝试次数耗尽）────────▶ failed
```

非法流转一律返回携带稳定 **Code** 的错误，调用方可区分处理：

| Code | 含义 |
| --- | --- |
| `already_exists` | 机器人/任务 ID 重复 |
| `duplicate_submit` | 相同幂等键重复提交（返回既有任务，不再次执行） |
| `not_found` | 机器人/任务/节点/依赖不存在 |
| `no_route` | 起终点之间不可达 |
| `no_eligible_robot` | 暂无满足条件的空闲机器人 |
| `dependency_pending` / `dependency_failed` | 依赖未完成 / 依赖已失败 |
| `not_assigned` | 对未分派任务上报结果 |
| `invalid_state` | 错误机器人上报、终态后重复上报等非法流转 |
| `at_wrong_node` | 声称成功但机器人不在终点/不在 executing |
| `execution_failed` | 上报执行失败（计入尝试次数，可重试） |
| `robot_offline` | 机器人已掉线 |
| `deadlock_victim` | 因死锁被抢占（让行，不计失败） |
| `task_timeout` | 任务执行超过超时上限 |

### 幂等与“只执行一次”

- **重复提交**：相同 `IdemKey` 只接受第一次，后续返回同一个任务与
  `duplicate_submit`，绝不会产生第二个执行。
- **重复上报**：任务一旦进入终态（`completed`/`failed`），再次上报返回
  `invalid_state`，状态不再改变。并发重复提交/上报由内核串行锁保证恰好一次生效
  （见 `TestConcurrentSubmitNoDuplicateExecution`、`TestConcurrentIdempotentReports`）。

---

## 二、路径预约与冲突消解

### 资源与时间窗

- 资源分为**有向边** `edge:u:v` 与**节点** `node:id`。
- 占用用半开时间窗 `[start, end)` 表示。**端点相接不算重叠**（`a1 == b0` 允许
  交接），因此机器人可在同一 tick 释放并被另一方获取。
- 终点节点占用到任务显式结束（哨兵“无限长”窗），中途节点/边按通过耗时占用。
- **单车道**需在路网中用 `network.Lane{U,V}` 显式声明；声明后两个方向在同一
  时间窗内互斥（额外占用 `lane:u:v`）。普通双向边**允许对向并行**。

### 逐段预约（而非一次性占满整条路径）

机器人在运行时**逐跳**申请资源，而不是派单时一次性预约整条路径：

- 当前持有所在节点；申请下一跳时需要 `edge:u:v`（含可能的 `lane:`）与目标节点窗。
- 冲突时按以下顺序消解（全部决策写入审计日志）：
  1. **等待（wait）**：原地等待占用者释放；
  2. **改道（reroute）**：在 Yen K 最短路候选中换一条首跳不冲突的路径，
     路径始终是到当前子目标（取货点/终点）的合法简单路径；
  3. **抢占（preempt）**：仅抢占**严格更低有效优先级**的占用者（含停车占用），
     被抢任务安全回收；抢占不到则继续等待。

### 死锁检测与打破

- 内核根据“谁持有资源、谁在等待谁”构建 **wait-for 有向图**，用 DFS 检测其中
  的环（见 `internal/planning.DetectCycles`）。
- 典型死锁：两台机器人在单车道上对向行驶，各自持有对方前进所需资源，形成
  `r1→r2→r1` 的环（见 `TestHeadOnDeadlockBroken`）。
- 打破策略：在环中选择**有效优先级最低**（并列时 ID 最小）的受害者**让行**——
  释放其全部预约、任务退避若干 tick（`DeadlockBackoff`）后从当前位置重新规划继续。
  让行不计为执行失败，任务归属不变；若环中所有任务都在等待，老化机制保证不同
  受害者不会无限循环。

---

## 三、失败、掉线与恢复语义

### 心跳与掉线

- 机器人通过 `Heartbeat` 刷新存活时间；超过 `HeartbeatTimeout` 未心跳即判掉线。
- 掉线机器人的**预约全部释放**、计划删除；其**未完成任务安全回收**并重新入队，
  可由位于该任务当前位置的空闲机器人**接手**（见 `TestOfflineReassign`）。
- 掉线后重新发心跳视为重新上线（无任务时回到 idle）。

### 执行失败与尝试次数

- 上报 `success=false` 或执行超时，安全释放资源并把任务退回 `pending`。
- 每次“真正开始执行”（离开起点/恢复后首次移动）计一次 `attempts`，达到
  `MaxAttempts` 后任务进入终态 `failed`（原因码区分 `execution_failed` /
  `task_timeout`），不会再被调度。
- 已 `completed` 的任务永远不会再次执行。

### 持久化：原子快照 + WAL

- 所有变更操作以 JSONL 追加写入 WAL；快照通过“临时文件 + rename”**原子落盘**，
  不会出现半更新文件。
- 重启时先载入最近快照，再按序重放 WAL。**拒绝结论也被记录并重放**（被拒绝的
  操作重放时同样不产生副作用），因此恢复后状态与连续运行完全一致
  （见 `TestRestartReplay`、`TestDeterministicAcrossIndependentRuns`）。
- 因为所有行为只依赖逻辑时钟与操作顺序，不依赖调度时序/随机数，两次独立运行
  同一批操作的结论也完全一致。

### 状态格式兼容范围

- 快照带 `version` 字段（当前为 v3）。加载时执行迁移：
  - 旧版本**缺失的字段**（如早期没有 `idem_index`、`picked_up`）会被补全或从
    既有数据重建（幂等索引从任务重建）。
  - 更高版本（含内核不认识的新字段）可被读取，未知字段不影响继续运行。
- 相关用例：`TestStateBackwardCompat`（v1 缺字段快照）、
  `TestStateForwardAndBackwardCompat`（未来版本带未知字段）、
  `TestCheckpointTruncateLog`（快照截断日志后恢复一致）。

> 兼容性约定：字段**新增**保持向后兼容；字段**删除/语义变更**通过版本迁移处理；
> 任务状态机、资源键与时间窗语义在同一主版本内保持稳定。

## 可解释性

每次分派、等待、改道、抢占、死锁打破、回收、到达、取货、成功/失败都会生成一条
`Decision`（含 tick 与判定依据）。可通过 `Kernel.Decisions()` 读取，演示程序也会
打印完整决策链。
