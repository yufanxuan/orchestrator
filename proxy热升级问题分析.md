# xproxy 热升级工作进展与 proxy 框架改进点分析

> 本文档面向 proxy 框架（orchestrator 仓库）的 agent 分析，梳理：
> 1. xproxy 侧已完成的全部热升级工作（现状）
> 2. proxy 框架侧可做的改进点（供框架 agent 评估）
> 3. 一个需要框架侧解决的关键问题：老进程不退出（systemd 兼容性）

---

## 一、背景

- 需求：FE2026050900370 BMS 节点组件热升级（Miracle 支持）
- 结论（前期评审确定）：**conductor 不做热升级**（冷重启 + nacre-agent 三层重试兜底），**仅 proxy 做热升级**
- 架构：新架构下 node-proxy/sandbox-proxy 被 node-ctl 统一取代；xproxy 是独立数据面二进制（`paths.proxy_executable`），由 `node-ctl proxy serve` 调用，作为 proxy 框架的静态定制 App
- 热升级机制整体框架由 proxy 框架提供（`MasterControl` 5 能力 + 继承 listener 自动恢复），**框架提供能力，xproxy 负责编排**

---

## 二、xproxy 侧已完成的全部工作

### 2.1 新增文件（3 个核心包 + cmd 编排）

| 文件 | 职责 |
|---|---|
| `pkg/hotupgrade/snapshot.go` | sealed memfd 快照：凭证/调用 side-table 序列化为 `CredsSnapshotList`，memfd 写入 + F_SEAL_GROW/SHRINK 封条，**全程不落盘**（(sid, token) 是明文凭证） |
| `pkg/hotupgrade/migrate.go` | `ExportToMemfd` / `RestoreFromMemfd`：快照进出 sealed memfd |
| `pkg/hotupgrade/orchestrator.go` | 编排核心：`Orchestrator.Run/Drain/Rollback/SuccessorPID` + 两阶段就绪探针（stats gate + data listener dial），通过 `ExtraFiles` 传递 listener FD + snapshot memfd |
| `pkg/masterext/hotupgrade.go` | side-table 迁移：`PrepareHotUpgrade`（导出+释放 Flock）/ `RestoreHotUpgrade`（重建+回放+SetGeneration）/ `RollbackHotUpgrade` / `IsHotUpgrade` / `RestoreFromEnv` |
| `cmd/hotupgrade.go` | SIGUSR2 编排入口：`installHotUpgradeHandler` / `runHotUpgrade` / `sdNotify` / 分代路径工具集 |

### 2.2 修改文件

| 文件 | 改动 |
|---|---|
| `pkg/shmtable/table.go` | 新增 `SetGeneration`（热升级恢复 generation，防 nacre-agent 观测到 generation=0 触发全量重推） |
| `pkg/credshm/writer.go` | 新增 `SetGeneration` |
| `pkg/invokeshm/writer.go` | 新增 `SetGeneration` + `Items()`（导出用） |
| `pkg/masterext/ext.go` | `Start` 检测 `IsHotUpgrade()` → 走 `RestoreFromEnv`（而非冷启动 Create） |
| `cmd/main.go` | Configure hook 保存 `lastConfig` + 清理分代残留；BindRuntime 捕获 `xproxyRuntime`/`xproxyMasterExt`；注册 `initHotUpgradeContext` |

### 2.3 热升级完整流程（SIGUSR2 触发）

```
SIGUSR2
  → installHotUpgradeHandler（幂等 gate，一次一个升级）
  → runHotUpgrade:
     [1] control := rt.MasterControl()          // 框架能力，未 serving 则忽略
     [2] orch.Run:
          GracefulShutdown()                     // P1：cancel subscriber、挂 worker 监管、关 metrics（不关 accept）
          CaptureListenerFDs()                   // 框架：dup data/mmds listener FD
          ext.PrepareHotUpgrade()                // xproxy：side-table → sealed memfd，Close 释放 Flock
          fork node-ctl relay                    // exec 磁盘新二进制（M4：解决 fork 旧二进制问题）
             ExtraFiles: data FD(3), mmds FD(4), creds memfd(5), invoke memfd(6)
             Env: KUASAR_PROXY_HOT_UPGRADE=1 + 各 FD 编号
     [3] 两阶段就绪探针 probeReady:
          Phase 1: stats gate（routesync Bookmark 完成，非 503）
          Phase 2: data listener TCP dial（确认新 worker Accept，零空窗）
     [4] 成功后 DrainWorkers(timeout)             // 老 worker drain
     [5] sdNotify MAINPID=<新pid>                // 通知 systemd（Type=simple 无效，见 §四）
     [6] stopHotUpgrade(); select{}              // 老进程永久阻塞，保持 systemd unit active
```

新 master 侧（successor）：
```
node-ctl proxy serve --config <frozen.yaml>   // 分代路径 .g<N+1>
  → Configure hook 收到分代 config
  → masterext.Start 检测 KUASAR_PROXY_HOT_UPGRADE=1
  → RestoreFromEnv: 从继承的 memfd 重建 side-table + 回放记录 + SetGeneration(旧值)
  → 框架 RunMaster 自动恢复继承 listener（框架已有能力）
```

### 2.4 关键设计点

1. **凭证迁移不落盘**：sealed memfd 经 ExtraFiles 传递，进程被杀也不留明文凭证文件
2. **generation 继承**：`SetGeneration(旧值)` 防止 nacre-agent 把 restart 当冷启动触发全量凭证重推
3. **零空窗**：stats gate 通过 ≠ 新 worker 已 Accept；两阶段探针关闭 stats-ready 与数据面就绪之间的空窗（实测 0.26ms 空窗被消除）
4. **分代路径**：stats/shm 路径 `.g<N>` 递增，防新旧 master 互踩；Configure hook 每次启动清理残留（覆盖 SIGKILL 孤儿）
5. **幂等**：`active` gate 拒绝并发 SIGUSR2
6. **F4 降级**：side-table 迁移失败不 fail master，/v1/* 降级为 no-token 直到 nacre-agent 重推

### 2.5 测试状态

| 范围 | 覆盖率 |
|---|---|
| `pkg/hotupgrade`（3 新文件） | **90.3%** ✅ |
| `masterext/hotupgrade.go` | ~88.5%（函数级 restoreCreds 90% / restoreInvoke 91.7%；缺口为 memfd 写失败等不可注入防御分支） |
| `cmd/hotupgrade.go` | 纯函数 100%（genScan/suffix/symlink/resolve）；`runHotUpgrade` 22.5%（成功路径以 `select{}` 阻塞收尾，单测不可达） |
| 端到端 | 已验证：SIGUSR2 触发、连续升级（gen 1→2）、凭证迁移、worker drain、零空窗（全程 400 无 000）、新二进制生效、systemd active、老空壳不响应二次 SIGUSR2 |

---

## 三、proxy 框架侧改进点（供框架 agent 评估）

> 框架当前已提供 5 个 `MasterControl` 能力 + 继承 listener 自动恢复。以下改进点按价值排序，**均非阻塞**（xproxy 侧已用 workaround 绕开）。

### 3.1 老进程不退出 → systemd 保持 unit active（**关键问题，见 §四**）

这是目前**最值得框架侧解决**的问题，也是本文档要重点讨论的。

### 3.2 `MasterControl` 增加 HotUpgrade 状态查询（低价值，可选）

现状：xproxy 的 `runHotUpgrade` 在 `rt.MasterControl() == nil` 时直接忽略 SIGUSR2（未 serving 场景）。框架无法告知"为什么 nil"（未 serve / 已 drain / worker 角色）。

建议（可选）：`MasterControl` 暴露 `HotUpgradeReady() bool` 或错误原因，让编排方能区分"还没 ready 稍后再试"与"永久不可用"，避免静默丢信号。

### 3.3 fork 目标解析（M4 的框架化，中价值）

现状：xproxy 通过 **node-ctl 中转 fork** 解决"fork 旧二进制"问题——exec 磁盘上的 node-ctl，node-ctl 再按 frozen config 的 `paths.proxy_executable` 启动新 xproxy。这是 xproxy 侧 workaround，绕过了框架。

建议（可选）：框架 `MasterControl` 提供 `ForkSuccessor(...)` 或 `ResolveCurrentExecutable()`，把"fork 哪个二进制、如何 exec"框架化，消除 node-ctl 中转环节，减少一次 exec 链。

### 3.4 就绪语义补充（低价值，可选）

现状：xproxy 用 stats 503 门 + data listener dial 两阶段确认 successor 就绪。框架内部实际有"Bookmark 完成"信号，但未暴露。

建议（可选）：`MasterControl` 暴露 `SuccessorReady() <-chan struct{}` 或 Bookmark 完成回调，让编排方不必自行 HTTP 轮询 stats。

### 3.5 RunMaster 热升级环境标记的文档化（低价值）

现状：继承 listener 自动恢复是框架能力，但触发条件（环境标记）依赖 xproxy 侧约定 `KUASAR_PROXY_HOT_UPGRADE`。建议框架侧把该约定文档化并固化为常量，避免两仓各定义一份。

---

## 四、关键问题：老进程不退出（请框架侧评估）

### 4.1 问题描述

热升级成功后，老 master 已完成 drain（无流量、无监听、side-table 已交接），此时需要：
- **老进程不能退出**（`os.Exit(0)`）——因为 `proxy.service` 是 `Type=simple`，**MainPID 退出 = systemd Deactivate = 杀整个 cgroup**（含刚 fork 的新 master + 新 workers）
- 框架没有 sd_notify `READY=1` 代码，`Type=notify` 会卡死；`Type=forking` 不匹配 exec 模型

**xproxy 当前 workaround**（`cmd/hotupgrade.go:177-178`）：
```go
stopHotUpgrade()          // 停止 SIGUSR2 监听，防老空壳二次响应（nil 指针 fault）
select {}                 // 永久阻塞，保持 MainPID 存活 → unit active
```

### 4.2 workaround 的代价（三次实测确认）

1. **老空壳进程永久泄漏**：每次升级累积一个不服务任何流量的僵尸壳进程（只占一个进程槽 + FD 最小集）。连续升级 N 次 → N 个空壳
2. **systemd MainPID 指向老空壳**：`MainPID` 永远是最老的空壳，systemd 对该 PID 的探活/重启语义失真
3. **运维困惑**：`systemctl status` 显示 active，但 MainPID 不是实际服务进程
4. **二次 SIGUSR2 目标**：下一个 SIGUSR2 若发给 systemd MainPID（老空壳），会被 `stopHotUpgrade` 忽略 → 升级不生效（需发给新 master 的实际 PID）
5. 未来清理方案（kill 上一代空壳）仍需精确追踪进程链，复杂度高

### 4.3 请框架侧评估的方案

**方案 A（推荐）：框架实现 sd_notify `READY=1` + `MAINPID`，服务改 `Type=notify`**
- 框架 `RunMaster` 启动完成时发 `READY=1`；热升级时老进程发 `MAINPID=<新pid>` 后**正常退出**
- 配套：`NotifyAccess=all` + `KillMode=process`（老架构 sandbox-proxy.service 的三件套）
- 老进程可正常 exit，无空壳泄漏
- 代价：框架需加 sd_notify 发送代码（仅 master 角色）；proxy.service 配置变更
- **这是老架构已验证过的标准方案**（sandbox-proxy/node-proxy 就是这么做的）

**方案 B：框架提供 ForkSuccessor 并接管 systemd 通知**
- 框架在热升级时知道新 PID，由框架统一发 `MAINPID` 通知
- 与方案 A 配套

**方案 C：维持现状（select{} 阻塞）**
- 接受空壳泄漏，后续在 xproxy 侧加"每次升级 kill 上一代空壳"清理
- 零框架改动，但运维模型不干净

### 4.4 需要框架侧回答的问题

1. 框架是否愿意实现 sd_notify `READY=1`（master 角色）？工作量评估？
2. `Type=notify` 切换对现有部署（proxy.service）的影响面？
3. 若维持 `Type=simple`，框架能否提供"热升级后让老进程安全退出且不触发 cgroup 清理"的机制（如 `systemd notify` 手动 Deactivate 抑制）？
4. 老架构三件套（`Type=notify + NotifyAccess=all + KillMode=process`）是否可直接复用为框架标准？

---

## 五、附：proxy 框架已提供的能力（现状基线，无需改动）

| 能力 | 说明 |
|---|---|
| `MasterControl.GracefulShutdown()` | P1：cancel subscriber、挂 worker 监管、关 metrics，幂等，不停 accept |
| `MasterControl.DrainWorkers(timeout)` | P2：worker 停 accept + drain，超时强杀，阻塞至退出 |
| `MasterControl.RollbackPhaseOne()` | 回滚 P1：subscriber 重启、监管恢复、metrics 重开 |
| `MasterControl.CaptureListenerFDs()` | dup data/mmds listener FD（mmds 未启用时 nil） |
| 继承 listener 恢复 | `RunMaster` 在热升级环境标记下自动恢复继承 FD（框架既有能力） |
