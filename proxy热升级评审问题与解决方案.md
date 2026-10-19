# proxy 热升级方案评审问题与解决方案

> **变更编号**: FE2026050900370-BMS节点组件热升级
> **评审对象**: `proxy热升级方案.md` 原始设计 V1.4（SIGUSR2 + FD 继承 + 内存快照 + worker drain 技术路线）
> **代码基线**: orchestrator main `40c7af9`
> **结论速览**: 方案方向可行（与旧架构 sandbox-proxy/node-proxy 热升级先例同构，FD 传输层复用判断准确），但三轮代码评审共发现 **12 个问题（7 个阻塞级）**，其中问题 10（fork 目标为旧二进制）是唯一"不修则升级整体无效"级别。全部修复已合并进方案 V2.0；本文件是问题与解决方案的完整记录，供 issue 回复使用。

---

## 1. TL;DR

| # | 级别 | 问题 | 方案 |
|---|------|------|------|
| 1 | 阻塞 | 新 master 沿同一路径 `O_TRUNC` 重建 SHM，清空老 master/worker 的 mmap 路由表，且老 master 退出时误删新 master 的 SHM 文件 | SHM 路径分代 `ShmPath.g<gen>` + 持有者自删 + 冷启动清扫 + bind 先行 |
| 2 | 阻塞 | statsSocket 列入 FD 继承 → conductor traffic GET 在新老 master 间分裂；同路径重建有 EADDRINUSE 与删除竞态 | 不继承 FD，随 routesync 会话分代重建 + 稳定符号链接 |
| 3 | 阻塞 | conductor 同 id 插件注册 evict 老会话，老 subscriber 有重连逻辑 → 新老 master **互踢震荡** | fork 前取消老订阅者；新 master 注册 + Bookmark 前置于 ready |
| 4 | 阻塞 | `metrics_listen` 直接 `net.Listen` 绑 TCP，新 master bind 必然 `EADDRINUSE`；Prometheus 固定地址抓取，分代路径不可用 | 老 master fork 后关闭 metrics listener；新 master 退避重试 bind |
| 5 | 阻塞（安全） | 路由快照落盘 `/run/.../route-snapshot.json`，而 `RouteEntry` 携带 APISecret/各 token/MmdsSecret——**凭证明文落盘** | **删除路由快照**：Bookmark 前置于 ready 后 resync 已全量重建，快照冗余 |
| 6 | 阻塞 | admission dump 在 SIGUSR2 时执行，但 master 是 slot 分配者且到 drain 前仍在消费 routesync——**快照必然过期**，free-list 冲突致同 slot 双分配 | dump 挪到 quiesce 之后；快照走 sealed memfd 不落盘 |
| 7 | 中 | drain 期间 superviseWorker 会重启崩溃老 worker（破坏 drain、污染 workerEpochs） | quiesce 时挂起监管循环，回滚时恢复 |
| 8 | 中 | SIGUSR2 无幂等；systemd stop 落在升级中 = cgroup 全杀；nacre-agent PID 计数在窗口内翻倍；在途 Create barrier 失败 503 | in-progress 拒绝；冷停文档化；终态判定 + ready-pipe；503 列入降级清单 |
| 9 | 低 | "fork 后原地改 ShmPath/StatsSocket" 破坏 EffectiveConfig digest 不变量 | 路径覆盖作为 sealed bootstrap 自带校验的独立字段 |
| 10 | **阻塞（最高）** | 方案 §3.2.4 指定新 master 由 `/proc/self/exe` fork——该符号指向**运行中进程的原 inode**，nacre-agent 替换磁盘二进制后 exec 它得到的仍是**旧二进制**，**升级在二进制层面无效** | ForkNewMaster exec 磁盘路径；bootstrap 身份校验改为 component + ConfigSchemaVersion 跨版本兼容（worker 路径保持同文件校验） |
| 11 | 中 | RunMaster 黑盒：subscriber/superviseWorker/statsServer 封在 masterGroup 内，SIGUSR2 编排够不着 | RunMaster 结构重构独立 PR 先行（任务组可控化） |
| 12 | 低 | 热升级携带冻结 EffectiveConfig，不重读 proxy.yaml——升级窗口内改配置不生效，未文档化 | 语义写入方案与运维手册 |

---

## 2. 问题详情（代码实证）

### 2.1 问题一：SHM 双进程冲突

| 事实 | 位置 | 后果 |
|------|------|------|
| `proxyshm.Create` 对磁盘文件 `O_TRUNC` 重建 | `internal/proxyshm/table.go:145`，调用点 `internal/proxyapp/master.go:32` | 新 master 沿同一路径重建，启动瞬间清空老 master/worker 正在 mmap 的路由表 → drain 期间新请求 404 |
| 清空后老 master 仍是唯一写者并继续写 | `master.go` 写路径不变 | 同一文件双写者，内容互相破坏 |
| 老 master 退出 `defer os.Remove(cfg.ShmPath)` | `internal/proxyapp/master.go:37` | 新 master 正在使用的 SHM 被误删 |

### 2.2 问题二：stats_socket 继承策略错误 + 删除竞态

1. statsSocket 是**聚合状态服务**（master 本地缓存名下 worker 的 inflight/idleSince 汇总），与 dataListener 的 per-connection 自包含 relay 本质不同：共享 accept 对后者无害、对前者返回半份数据——conductor 的 traffic GET 随机命中新老 master，且各自的 worker 集可信度判断都会得出 503，结果在半份数据/503 间抖动。
2. 若同路径重建：老 master 活跃监听时 `listenUnix` 同路径 `EADDRINUSE`；即使错开时序，老 master 退出的 `defer os.Remove(cfg.StatsSocket)`（`master.go:100`）删的是路径不是 inode——新 master 的 socket 文件被误删。

### 2.3 问题三：routesync 双会话互踢震荡

conductor 同 id 插件注册 evict 老会话并关闭其路由流（`internal/configsock/plugin.go:20,54-56`）。老 master 的 subscriber 有**断线重连逻辑（指数退避）**——被踢后自动重连再踢回新会话：新旧 master 互相抢注，drain 窗口内租约反复震荡。被踢后老体系降级：parked 请求等 `park_timeout` 超时、MMDS 查询 503、pause/delete 不再传播。

### 2.4 问题四：metrics_listen 端口冲突

`cfg.MetricsListen` 非空时 master 直接 `net.Listen` 绑 TCP（`internal/proxyapp/master.go:159-161` → `internal/appnet/netserve.go:50`）。新 master bind 同地址必然 `EADDRINUSE`——与问题二同类，但**没有 routesync 动态发现机制**：Prometheus 按固定地址抓取，分代路径方案不可用。

### 2.5 问题五：路由快照会把凭证明文落盘（安全）

`RouteEntry` 携带 `APISecret`、`ServiceSecret`、`EnvdAccessToken`、`ForwardAccessToken`、`TrafficAccessToken`、`MmdsSecret`（`internal/routesync/proto.go:102-118`）。原方案将其序列化到 `/run/sandbox-proxy/route-snapshot.json`——全仓纪律是 secrets 只存在于 sqlite 密文/进程内存/受信 mmap，该快照新增明文磁盘落点。

### 2.6 问题六：admission dump 时点错误

master 是 admission slot 分配者（`internal/proxyshm/view.go:106-113`，`ApplyUpsert` 内 `admission.PrepareUpsert`）。原时序在 SIGUSR2 时 dump，但老 master 到 drain 前都在消费 routesync 分配 slot——dump 之后的分配不在快照里：新 master 恢复的 free-list 与实际冲突，**同 slot 双分配**。

### 2.7 问题七/八/九：中低

| # | 问题 | 位置 |
|---|------|------|
| 7 | `superviseWorker`（500ms 退避无限重启）在 drain 期间拉起崩溃老 worker——fresh worker 重新 accept，drain 失效，workerEpochs 污染 admission | `internal/proxyapp/master.go` |
| 8 | SIGUSR2 无幂等（双 fork）；systemd stop 落在升级中 = cgroup 全杀；nacre-agent CheckPID 数 3 进程而切换窗口是 6 个；在途 Create barrier 随老会话失败 503 | 编排面 |
| 9 | "fork 后原地改 `ShmPath`/`StatsSocket`" 破坏 `EffectiveConfig` digest 不变量（digest 覆盖整个 canonical 序列化） | `internal/proxyapp/effective.go:20-42` |

### 2.8 问题十：新 master 的 fork 目标是旧二进制（最高优先级）

原方案 §3.2.4 指定新 master 由 `master fork master（/proc/self/exe proxy serve）` 产生。内核语义：`/proc/self/exe` 指向**运行中进程的原 inode**；nacre-agent 编排顺序为 `backup → update（替换磁盘二进制）→ Signal(SIGUSR2)`——信号到达时磁盘已是新二进制，老进程的 `/proc/self/exe` 仍指旧 inode（通常已 `(deleted)`）。**fork 出的"新 master"运行旧代码，热升级在二进制层面无效**。对照：worker 用 `/proc/self/exe` 正确（必须与 master 同二进制）；错误是把 worker 的做法照搬给新 master。

### 2.9 问题十一/十二：RunMaster 黑盒与冻结配置

| # | 问题 | 位置 |
|---|------|------|
| 11 | `RunMaster` 是阻塞函数，subscriber/superviseWorker/statsServer/RunGC/per-worker mmdsrpc 封在 `masterGroup` 内，SIGUSR2 编排（quiesce 各步骤）无法从信号 goroutine 触碰 | `internal/proxyapp/master.go` |
| 12 | 新 master 经 sealed bootstrap 接收冻结 `EffectiveConfig`、不重读 `proxy.yaml`——升级窗口内配置修改不生效，语义未文档化 | `internal/proxyapp/effective.go` |

---

## 3. 解决方案

### 3.1 统一模型：资源分代（generation）

冷启动 gen=0 沿用原路径（零感知）；每次热升级 gen+1，受影响资源路径携带代号；gen 随热升级 bootstrap（sealed memfd）传递，路径覆盖作为自带校验的独立字段（问题 9）。

| 资源 | 策略 |
|------|------|
| dataListener / mmdsListener | **FD 继承**（per-connection 服务，共享 accept 无害） |
| admission memfd | **FD 继承 + attach**（匿名 inode 无路径；共享 arena 是设计意图） |
| 路由表 SHM | **分代重建** `ShmPath.g<gen>`（O_TRUNC 只打无人 mmap 的新文件） |
| statsSocket | **分代重建** `StatsSocket.g<gen>` + 稳定符号链接 `proxy-stats.sock -> .g<gen>` |
| metricsListener | 老 master 关闭 + 新 master 退避重试 bind |

**SHM 分代细则**（问题 1）：持有者自删（resolvedPath 守卫）；冷启动 glob 清扫 `ShmPath.*` 孤儿；**bind 先行**（data_listen 作为单实例互斥锁先于 SHM 创建——当前代码顺序相反）；worker 无需感知分代。

**stats 分代细则**（问题 2）：新 master bind 新路径、新 routesync 注册携带新路径——conductor 收到注册后 traffic GET 单向收敛到新 master；conductor **零改动**（Register.Proxy.StatsSocket 字段本就存在）；命名规则：冷启动原名、热升级 `.g<gen>`、重启复位，`proxy.yaml` 不变。

### 3.2 routesync 会话切换（问题 3）

老 master **fork 前就取消**自己的 subscriber（quiesce 的一部分；独立可取消 ctx，cancel 即停不再重连）——互踢窗口彻底归零，老路由视图冻结与 drain 语义一致。新 master 在 fork 新 worker **之前**完成注册 + 收到 Bookmark 再发 ready——conductor 侧租约全程有持有者，路由权威空窗压缩到 resync 时长（几十 ms）。不采用双 plugin id + conductor 多租约（改动面远超收益）。

### 3.3 metrics_listen（问题 4）

老 master fork 后**立即关闭** metrics listener（观测面不承担正确性，缺失与冷重启等价）；新 master 退避重试 bind；跨升级 Prometheus counter 重置由 `rate()` 自动处理。

### 3.4 路由快照删除（问题 5）

Bookmark 前置于 ready 后，**路由快照冗余**：新 master 自身 resync 已全量重建路由表与 MMDS 投影。删除快照整节——净减 100-150 行、-0.5 人天、消除明文凭证落盘面。admission 状态不来自 routesync，dump 保留但必须经 sealed memfd。

### 3.5 admission 交接（问题 6）

dump 挪到 **quiesce 之后**（停订阅者 + 挂起监管 + in-flight upsert 应用完）——fork 前无任何分配，快照-现实竞态消失；新 master 经 FD 继承 attach 同一 memfd（新增 `AttachMaster(fd)` 路径，`NewMaster` 硬编码 `MemfdCreate` 不能复用）恢复 records/free/nextGen/syncGen/workerEpochs。

### 3.6 中低问题（问题 7/8/9）

| # | 解决 |
|---|------|
| 7 | quiesce 时挂起老 master 全部 superviseWorker；回滚时恢复 |
| 8 | SIGUSR2 in-progress 幂等拒绝；冷停终态文档化（nacre-agent 回退认识）；WaitNewPID 终态判定 + ready-pipe；Create 503 列入降级清单 |
| 9 | 路径覆盖作为 sealed bootstrap 自带校验字段，或覆盖后重新 freeze |

### 3.7 fork 目标修正（问题 10，最高优先级）

1. ForkNewMaster exec **磁盘路径**：内置 master 用 `configresolve.CurrentExecutables()` 解析的 node-ctl 路径；xproxy 用 `paths.proxy_executable`。时序自洽：nacre-agent 先替换二进制再发信号。
2. sealed bootstrap 身份校验改为跨版本语义：现有 `ExecutableIdentity`（`internal/componentexec/bootstrap_linux.go:60-80`）验证"同一文件身份"，旧二进制的 bootstrap 会被新二进制拒绝——改为"同 component + `ConfigSchemaVersion` 兼容"（字段已存在）；**worker 路径的同文件身份校验保持不变**。
3. worker 链路自动获得新代码：新 master 再 fork worker 用其自身 `/proc/self/exe`（= 新二进制）。
4. 回滚安全不变：新二进制启动/校验失败 → 老 master 继续运行。

### 3.8 结构先行与冻结配置（问题 11/12）

RunMaster 结构重构（任务组可见化/可控化、暴露 upgrade 编排接口）独立 PR **先行**——避免在黑盒上打洞；行数已计入，无新增工作量。冻结配置语义写入方案与运维手册；nacre-agent 可选：升级前检查 proxy.yaml mtime 提示配置漂移。

### 3.9 最终时序（实现以此为准）

```
SIGUSR2 → 幂等检查（in-progress 拒绝二次触发）
  → quiesce：停 routesync subscriber + 挂起 superviseWorker + 等 in-flight upsert 应用完
  → dump admission 内存状态（sealed memfd，不落盘）
  → 老 master 关闭 metrics listener
  → fork 新 master（exec 磁盘上的新二进制——绝不用 /proc/self/exe；
      ExtraFiles: dataListener/mmdsListener/admission memfd FD
      + bootstrap[gen、路径覆盖、admission 快照] + ready-pipe）
  → 新 master：恢复 listener → attach admission + 恢复内存态 → bind StatsSocket.g<gen>
        → 注册 routesync → 收到 Bookmark（全量 resync，含 MMDS 投影）
        → fork 新 worker → worker ready → 新 master 发 ready
  → 老 master 通知老 worker drain（停 accept，排空存量；inflight 判定复用 proxystats）
  → 老 worker 退出 → 老 master 释放 credshm Flock（仅 xproxy）→ sd_notify MAINPID → 退出

失败回滚（任一步失败/超时）：
  kill 新进程 + 老 master 重启 subscriber + 恢复 metrics listener
  + 恢复 superviseWorker +（已 drain 的老 worker 接受其退出，replacement 由恢复后的监管拉起）
```

---

## 4. 行为边界与降级清单（验收口径）

| 降级 | 触发窗口 | 影响评估 |
|------|---------|---------|
| 老 worker 上 parked 请求等 `park_timeout` 超时返回 | quiesce 至老 worker 退出 | park 本就有秒级超时语义 |
| 老 worker 的 MMDS 查询 503 | 同上 | MMDS 仅 envd 初始化使用，存量 relay 不依赖 |
| conductor 侧 pause/delete 暂不传播到老 worker | 同上 | 视图冻结是 drain 语义的一部分 |
| 在途 Create（barrier 未 ACK）失败 → 503 | quiesce 至新 Bookmark | SDK 重试覆盖 |
| metrics 短暂缺失 | 老 master 关 listener 至新 master bind 成功 | 观测面降级，与冷重启等价 |
| traffic GET 偶发失败 | stats 分代切换瞬间 | 客户端重试即恢复 |
| **超过 drain 上限（10min 可配置）的 websocket 长会话断开** | 老 worker force kill | **"零中断"承诺 = 尽力排水 + 上限可配置** |

---

## 5. 问题 → 方案 V2.0 章节映射

原 V1.4 文档已按本文件全部修复合并重写为 V2.0：

| 问题 | V2.0 章节 |
|------|----------|
| 1 | §2.2 资源交接策略 / §3.4 分代模型细则 |
| 2 | §2.2 / §3.5 |
| 3 | §3.5 routesync 会话切换 |
| 4 | §2.2 / §3.8 |
| 5 | §3.6.1 路由表：无快照 |
| 6 | §3.6.2 admission |
| 7 | §3.7 drain |
| 8 | §3.3 回滚 / §3.8 杂项 |
| 9 | §2.2（digest 约束） |
| 10 | §3.2（worker ≠ 新 master 表 + fork 目标） |
| 11 | §4/§5（RunMaster 重构第一 PR） |
| 12 | §3.8（冻结配置） |

---

## 6. 验收标准

完整清单见方案 V2.0 §7（18 项）。核心新增项：

- [ ] 升级后新 master 二进制为磁盘新版本（`/proc/<pid>/exe` 指向新文件），worker 链路同为新版本
- [ ] 旧二进制的热升级 bootstrap 被新二进制接受（component + schema version），worker 同文件校验不受影响
- [ ] 连续两次升级（gen 0→1→2）后磁盘仅存最新一代文件
- [ ] 全程 conductor 侧 proxy 插件租约无无持有者窗口
- [ ] traffic GET 无分裂；稳定路径 `proxy-stats.sock` 任意代数可拨通
- [ ] 磁盘任意时刻无凭证明文快照
- [ ] drain 期间 kill 老 worker 不被重启，其余正常排空
- [ ] SIGUSR2 二次触发被拒绝；回滚后老 master 恢复订阅/metrics/监管

---

## 7. 待办与依赖

| 事项 | 归属 | 说明 |
|------|------|------|
| credshm/invokeshm Reader 无锁分析核实 | xproxy 仓 | 需在 `kuasar-sandbox-cci` 仓核实 `writer.go`/`reader.go` 后确认 |
| xproxy MasterExtension 生命周期 | xproxy 仓 | 新 master 冷启动重跑 extension 生命周期，进程内状态不迁移，需评估 |
| netns FD 传递验证 | 本仓 | mmdsListener 在 netns 中 bind，FD 跨进程继承需集成测试覆盖 |
| nacre-agent 编排适配 | nacre 仓 | WaitNewPID 感知 MAINPID 切换；CheckPID 容忍窗口内双倍进程（终态判定）；冷停终态纳入回退逻辑；健康检查与 ready-pipe 超时（30s）对齐 |
