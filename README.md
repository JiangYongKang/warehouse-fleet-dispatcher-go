# 仓储多机器人任务分派与路径预约内核（Go）

一个可在本地运行、**确定性**的多机器人调度内核：用本地路网（有向图）和本地快照存储
模拟机器人任务分派、共享路网预约与冲突消解、掉线/失败处理与重启恢复。
不依赖任何外部服务或墙钟——所有时间都用单调的**逻辑时钟（tick）**表示，
因此同一批操作无论重放多少次、并发如何交错，结论都一致、可解释。

- 模块：纯 Go 库 `github.com/highcumontoa/warehouse-fleet-dispatcher-go`
- 演示：`go run ./cmd/demo`
- 测试：`go test ./...`（建议加 `-race -count=N`）

---

## 1. 核心模型

### 1.1 路网（`graph.go`）

- 节点 = 站点/路口；**有向边** = 可通行方向，`Length` = 通过该边所需 tick 数（≥1）。
- 一条物理巷道的两个方向分别建模为两条有向边，但在**预约层归一化为同一条
  双向单车道资源**（`edgeRes` 取两端点规范化 `a~b`），因此对向冲突能被检测。
- 路径规划为确定性 Dijkstra：等长路径的平局按节点 ID 字典序，结果带缓存。
- 规划阶段**不绕开堵车车道**：几何路径固定，运行时冲突一律由“等待 / 死锁退避”
  解决。这样同一任务无论重派多少次走的都是同一条路径，便于解释与重放。

### 1.2 逐拍占用模型（`reservation.go` / `assign.go`）

时间被离散化为 tick，每拍（`AssignOnce` / `AdvanceClock`，或后台循环）依次执行：

1. **分派**：把就绪任务派给满足条件的空闲在线机器人；
2. **运动**：逐台机器人推进一个 tick；
3. **死锁检测与打破**：构建实时等待图，发现环则确定性地选择牺牲者退避。

资源占用不变量：**每台机器人任意时刻至多持有一个资源**（某个节点，或其所在的某条边），
窗口为半开区间 `[start, end)`。受阻时机器人**继续占住当前资源**（窗口顺延），
于是“持有并等待”的真实死锁可以发生，也因此能被检测。

运动语义：

- 在节点上：申请下一条边；被占则在当前节点等待；
- 进入长度 L 的边：占用该车道 L 个 tick，`enterAt+L` 时申请目的节点；
- 节点是**零停靠途经点**：到达中间节点后若下一条边空闲，同拍立即进入，
  路径总耗时严格等于各边长度之和；
- 申请目的节点时若被一个**空闲停靠机器人**占着，视为“支线可让位”，到达者接管节点
  （空闲机器人不参与死锁）；若被**执行中的机器人**占着，则形成真实等待边。

### 1.3 任务分派（`task.go` / `assign.go`）

- 任务带 `Priority`（越大越高）与前置依赖 `Deps`（全部 `completed` 才就绪）。
- 就绪队列排序（完全确定）：
  1. **有效优先级** = 静态优先级 + **老化补偿**（见 1.5）降序；
  2. 有效优先级相同 → 提交序号 `Seq` 升序（同优先级严格 FIFO）。
- 机器人选择：在空闲在线机器人中，选“从当前位置经 `From` 到 `To` 的最短路
  总长度最小”者；平局取机器人 ID 字典序最小。
- **互斥保证**：同一任务同一时刻只有一个执行者；一台机器人同一时刻只做一个任务；
  所有状态变更在同一把互斥锁内串行完成。

非法流转一律返回带稳定错误码的 `*Error`（见 `errors.go`），原因可区分：

| 错误码 | 触发情形 |
| --- | --- |
| `invalid_argument` | 空 ID、边长度非法、依赖重复/自依赖、时钟回退等 |
| `not_found` | 节点/机器人/任务/快照不存在 |
| `already_exists` | 重复注册机器人、**重复提交同一任务** |
| `deps_unmet` | 依赖的任务尚未提交 |
| `invalid_state` | 重复下线/上线、对未到达任务提前上报、引擎已关闭 |
| `robot_offline` / `robot_busy` | 机器人不在线 / 正忙（由分派内部使用） |
| `stale_result` | 上报者非当前执行者、任务已被回收/完成后的迟到失败上报 |
| `already_done` | 已 completed/failed 的终态任务再次收到结果 |
| `conflict` / `no_path` / `deadlock` | 资源冲突、不可达、死锁消解超界 |

> 说明：在“依赖必须先存在”的提交规则下，新任务不可能经由依赖形成环
> （唯一可达的环是自依赖，直接拒绝）；依赖环检测作为防御性检查保留。

### 1.4 预约冲突、死锁与改道（`deadlock.go` / `assign.go`）

- **冲突**：同一节点或车道在半开窗口内被两台机器人需要 → 后到者等待，
  窗口顺延、继续持有当前资源。
- **等待图**：边 `A -> B` 表示 A 想要的资源正被执行中的 B 持有。
  节点/机器人都按 ID 升序遍历，**DFS 找到的第一个环确定且唯一**。
- **打破死锁**：从环中选牺牲者，规则与分派顺序一致、单调：
  1. 有效优先级最低者先牺牲（老化后低优先级不会一直被牺牲）；
  2. 相同 → 提交序号最大（**最年轻**）者给更早提交的任务让路；
  3. 再相同 → 机器人 ID 字典序最大者。
- **退避方式**（不更换机器人、不重排队、保持几何路径）：
  - 停在边上车 → 撤回该边起点；
  - 停在节点上（占着对方目的节点）→ 沿来路退一个节点；无路可退则就地释放节点。
  - 退避冷却时长 = 对手越过所争资源所需 tick + 缓冲，并随**连续退避次数递增**，
    保证同一对机器人再次成环时本次让路更久，单调收敛、不发生活锁。

### 1.5 防饿死（`deadlock.go: ageBonus`）

- 老化补偿 `ageBonus = 就绪等待 tick / 10`（每等待 10 tick 有效优先级 +1）。
- **等待从“任务已就绪、却因资源不足派不出去”的第一个 tick 起算**
  （`WaitTick`），尚未就绪（依赖未完成）不计时；一旦分派成功立即清零。
- 因此任何任务只要等待足够久，有效优先级必然超过新到的高优先级任务；
- 同优先级 FIFO（按 `Seq`）也保证提交顺序即服务顺序。
- 测试 `TestAgingOverridesFreshHigh` 精确验证：等待足够久的低优先级任务会
  反超一个新到的、静态优先级更高的任务先被服务。

---

## 2. 任务结果与幂等（`report.go`）

结果上报 `ReportResult(TickReport)` 的判定：

- 任务不存在 → `not_found`；
- 上报机器人不是当前执行者 → `stale_result`；
- 尚未走完路径（未到达）就上报 → `invalid_state`；
- `completed` 任务再次收到**成功**上报 → `already_done`，**幂等空操作，绝不二次执行**；
- `completed` 后迟到的**失败**上报 → `stale_result`（结果已裁定，不翻案）；
- `failed` 终态的任何上报 → `already_done`；
- 任务已被掉线/抢占回收为 `pending` 后的旧上报 → `stale_result`。

失败重试语义：

- **只有“机器人上报执行失败”才累加 `Attempt`**；调度死锁退避、掉线回收、
  重启回收都**不累加**失败次数（另有观测字段 `Dispatch` 统计分派次数）。
- `Attempt < MaxAttempts(=3)`：任务回 `pending` 重新分派；老化计时重置后重新累计。
- `Attempt >= 3`：任务进入 `failed` 终态，永不再执行。

---

## 3. 掉线与重启恢复

### 3.1 掉线 / 上线（`commands.go`）

- `MarkOffline(id)`：
  - 执行中的任务安全回收为 `pending`（`Assignee` 清空、老化计时重置），
    **不计失败次数**，可被其他空闲机器人重新分派；
  - 该机器人全部预约立即释放（其他被它挡住的任务下一拍即可前进）；
  - 已完成任务不受影响。
- `MarkOnline(id)`：机器人在其最后已知节点恢复为在线并占住该节点。
- 对已下线重复下线、对在线重复上线 → `invalid_state`。

### 3.2 持久化与原子性（`store.go` / `persist.go`）

- 任意一次状态变更（注册、提交、分派拍、上报、上下线、抢占退避）都在**同一把锁内、
  变更完成后**整体落盘。
- `FileStore.Save` 采用“写同目录临时文件 → `fsync` → `rename` 原子替换”，
  崩溃时只会留下旧文件或完整新文件，**不存在半更新 JSON、不存在不确定状态**。
- 也提供 `MemoryStore`（进程内、同样全有或全无）。

### 3.3 重启恢复语义（`recovery.go`）

从快照恢复时：

- `completed` / `failed` 终态任务**原样恢复，永远不会被再次执行**；
- 宕机前处于 `assigned`（在途）的任务，其执行进度/在途占用无法保证一致，
  **一律安全回收为 `pending`**（保留失败计数 `Attempt` 等字段），
  预约全部作废、由恢复后的当前路网重新规划，杜绝“机器人已不在但车道仍被占”；
- 机器人恢复为最后节点的占用；掉线机器人保持掉线、不带预约；
- 逻辑时钟与提交序号 `Seq` 从快照恢复，保证**重放同一批操作结论一致**
  （测试 `TestReplayDeterminism` 两次独立运行逐任务比对
  状态/执行者/分派次数完全相同）；
- 快照引用的节点必须仍存在于当前路网，否则 `not_found` 快速失败，不猜测状态。

### 3.4 状态格式兼容范围

当前格式 `fleet-snapshot/v2`（文件内含 `"format"` 字段）。

- **v1 → v2 迁移**：无 `format` 字段即视为 v1。迁移内容：
  - 任务无 `seq`：按数组出现顺序回填 FIFO 序号；
  - 预约资源为复合串 `"node:<id>"` / `"edge:<a>-><b>"`：拆分为 `kind + res_id`；
  - 机器人字段 `task` → `current_task`。
  - 见 `TestSnapshotV1Compatibility`。
- **字段增删容错**：
  - v2 新增的 `wait_tick` / `dispatch` / `reason` 等字段在旧文件中缺失时取零值，
    不影响读取；
  - 文件中出现当前版本不认识的未来字段（如 `battery`、`tags`）会被 JSON 解码
    直接忽略，见 `TestUnknownFieldsTolerated`；
  - 无法识别的 `format` 版本号会显式报错而不是按错误语义加载。

---

## 4. 并发模型

- 所有公开方法在 `Engine.mu` 下串行化状态变更；预约表/时钟另有内部同步。
- 默认启动一个后台分派 goroutine（`assignCh` 容量 1，信号自动合并）；
  提交/上报等操作会唤醒它。测试可用 `WithoutBackground()` 关闭后台循环，
  改为纯显式 `AssignOnce(tick)` 驱动，获得严格确定的时序。
- 测试覆盖：
  - `TestConcurrentSubmitAndReport`：无后台、4 goroutine 并发提交同一批任务
    （每个 ID 恰好成功一次，其余 `already_exists`），40 任务全部恰好完成一次；
  - `TestBackgroundConcurrent`：**保留真实后台 goroutine**，生产者/时钟推进者/
    只读查询者/结果上报者多 goroutine 并发，`go test -race -count=3` 无数据竞争，
    且 24 个任务各提交一次、完成一次。

单测日志会打印关键输入与判定依据（就绪排序、阻塞者、死锁环与各成员有效优先级、
牺牲者选择、退避时长、回收与恢复决定等），形如：

```
[tick 3] DEADLOCK cycle [r1 r2] at tick 3; [r1(task=t1,effPri=5,seq=1) r2(task=t2,effPri=1,seq=2)]; victim="r2" ...
[tick 4] robot "r1" completed route for task "t1", arrived "B" at tick 4 (awaiting result report)
```

---

## 5. 验证方法

```bash
go vet ./...
go test ./...                     # 全部单测（15 个）
go test -race -count=5 ./...      # 竞态检测 + 重复运行，确认稳定通过
go test -v .                      # 查看带 tick 前缀的判定日志
go run ./cmd/demo                 # 端到端演示：依赖/优先级、对头死锁、掉线、落盘重启恢复
```

测试清单：

| 测试 | 覆盖点 |
| --- | --- |
| `TestSubmitValidation` | 空 ID、未知节点、未知/重复/自依赖、重复提交、错误码可区分 |
| `TestDependencyGating` | 依赖未完成不分派、完成后放行 |
| `TestExclusiveAssignment` | 任务/机器人双重互斥 |
| `TestPriorityDispatch` | 高优先级先分派、平局 FIFO |
| `TestAgingOverridesFreshHigh` | 老化补偿反超新到高优先级（防饿死） |
| `TestReportIdempotency` | 提前/冒充/重复/迟到上报的区分与幂等 |
| `TestFailureRetryAndTerminal` | 失败计数、重试上限、`failed` 终态 |
| `TestConcurrentSubmitAndReport` | 无后台并发竞争、唯一提交、全部完成 |
| `TestBackgroundConcurrent` | 真实后台 goroutine 并发，`-race` 无竞争 |
| `TestOfflineReassign` | 掉线回收预约/任务、他机接手、上线恢复、非法流转 |
| `TestRestartRecovery` | 落盘重启、在途任务回收、终态不重执行、恢复后继续完成 |
| `TestReplayDeterminism` | 同输入两次运行逐任务结论一致 |
| `TestSnapshotV1Compatibility` | v1 历史快照迁移与恢复 |
| `TestUnknownFieldsTolerated` | 未来新增字段被容忍、往返可读 |
| `TestSmokeBasicAssign` / `TestSmokeHeadOn` | 正常派单不回归 + 双向单车道对头死锁全流程 |

---

## 6. 主要文件

| 文件 | 职责 |
| --- | --- |
| `errors.go` | 稳定错误码与可解释错误类型 |
| `graph.go` | 路网、确定性 Dijkstra |
| `task.go` / `robot.go` | 任务/机器人状态 |
| `reservation.go` | 逐拍资源占用、冲突查询、顺延、回收 |
| `clock.go` | 确定性逻辑时钟 |
| `assign.go` | 分派、运动推进、等待图、死锁检测与退避、老化排序 |
| `deadlock.go` | 确定性环检测、牺牲者策略、老化补偿 |
| `report.go` | 结果上报幂等、失败重试终态 |
| `commands.go` | 机器人注册/上下线、任务提交校验、依赖检查 |
| `store.go` / `persist.go` | v1/v2 快照、原子落盘、内存存储 |
| `recovery.go` | 快照导出、重启恢复与在途任务安全回收 |
| `lifecycle.go` | 引擎装配、后台循环、时钟推进、只读查询 |
| `cmd/demo/main.go` | 端到端本地演示 |
