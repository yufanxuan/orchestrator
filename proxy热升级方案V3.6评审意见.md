# proxy 热升级方案 V3.6 评审意见

> **评审对象**: `proxy热升级方案v3.md`（V3.6，框架最小改动版）
> **代码基线**: orchestrator main `40c7af9`
> **证据基础**: 两轮实测验证（独立 PoC 9 项 + 真实组件 4 项，均在 Docker linux/arm64 通过/复现，测试代码已入仓可复跑）
> **结论速览**: V3.6 的架构取舍**方向正确**（职责上移 xproxy、框架最小化、砍缓存交接），骨架可以保留。但存在 **2 个必改项**（高危，其一与实测数据直接矛盾）+ 4 个中危 + 3 个低危。修订为 V3.7 后可进入实现。

---

## 1. TL;DR

**认可项（不要改回）**：

| 项 | 理由 |
|----|------|
| 职责划分（框架两原子能力 + xproxy 编排） | 开源框架最小化，上游友好 |
| 不做缓存交接（路由 resync / admission 重建） | 删掉了最难的两块（AttachMaster、quiesce 时序），代价可控 |
| fork 目标 = exec 磁盘新二进制 | 问题 10 已修复（实测：`/proc/self/exe` 替换后 ENOENT，磁盘路径正常） |
| metrics 关闭/重试、SIGUSR2 幂等、SHM/stats 分代、30min drain 上限（政策） | 均正确 |

**必改项**：

| # | 级别 | 问题 | 修复 |
|---|------|------|------|
| P1 | 高（事实错误） | "砍掉停 subscriber，靠 conductor 同 id evict"——与实测矛盾：真实双 master 实测 **0.31 秒内互踢 4 个来回**，震荡持续到一方退出；drain 上限 30min 意味着震荡最长 30 分钟；新 master 的 Bookmark 会被反复打断 → 可能 ready 超时 → **升级活锁失败** | 把"取消 subscriber"作为 `GracefulShutdown` 的第一步**内部动作**（cancel ctx，约 3 行，不是新增接口——评审砍的是接口不是动作）；回滚时重启 subscriber |
| P2 | 高（集成断点） | drain 默认 30min 与 nacre-agent 超时预算矛盾：官方 PDF 明确**单组件 120s / 总 600s**——proxy 健康排水期间 nacre-agent 早已判超时触发回退 | proxy 的成功判定改为"**新 PID 健康**"（HealthCheck 打新 master），不等旧进程退出；nacre-agent 适配从"可选"改为**必做** |

---

## 2. 必改项详情

### P1：不能砍"停 subscriber"——实测震荡证据

**V3.6 的主张**（附录 A #3 + §1.2）："独立'停 subscriber'接口已砍掉——靠 conductor 同 id evict 机制，旧进程在优雅退出期间 subscriber 短暂重连抖动可接受（旧进程反正要退）"。

**三个事实叠加，该主张不成立**：

1. **实测证据**（真实双 master + 真实 conductor，测试 `TestRoutesyncEvictionOscillation` 已入仓）：master B 注册后 **0.31 秒内 registered=4 / deregistered=4**，两进程全程存活、持续抢注租约。subscriber 重连极快（初期退避几乎为零），互踢是**持续拉锯**，不是"短暂抖动"。
2. **drain 上限 30min**：旧 master 要活到排水结束才退出——震荡窗口 = 整个 drain 期（最长 30 分钟），不是"马上要退"。
3. **新 master 的 ready 依赖 Bookmark 稳定完成**：它注册后同样会被旧 master 的重连踢掉，resync 被反复打断 → `waitTableSync` 长期不满足 → 新 worker 不 Serve → 30s ready 超时 → 回滚。**最坏情况：升级因震荡活锁而失败**。

**连带影响**：§4.3 声称旧 master 路由表"冻结不再写"——subscriber 重连后会重新 resync，表根本不冻结，该假设同样被打破。

**修复（仍满足"最小框架改动"约束）**：

- `GracefulShutdown` 的**第一步**就是 cancel routesync subscriber 的 ctx（进程内部动作，~3 行，不需要暴露任何新接口）。此后旧 master 视图真正冻结，与 drain 语义一致。
- 该语义对**纯框架场景**（systemd stop → 优雅退出）同样正确：排水期冻结路由视图本来就是期望行为。
- 回滚路径（fork 失败/ready 超时）需**重启 subscriber**（superviseWorker 式重拉即可）。

### P2：30min drain 与 nacre-agent 超时预算的硬冲突

官方 PDF（FE2026050700631 设计决策 2.8）：**单组件升级超时 120s，全组件总超时 600s**。

V3.6 的 drain 默认 30min：nacre-agent 在 120s 就会判 proxy 升级超时并触发**回退**——而此时 proxy 可能正处于健康排水中（新 master 已接管、存量连接正常服务）。回退动作（kill 新进程 + 恢复旧进程）反而是**破坏性**的。

**修复**：

1. nacre-agent 对 proxy 的成功判定改为"**新 PID 出现且健康**"（HealthCheck 打新 master 的数据面/管理面），不等旧 PID 消失——旧进程 drain 属于正常的后台收敛。
2. nacre-agent 适配（WaitNewPID 语义、CheckPID 容忍窗口内双倍进程、MAINPID 切换）从 §7.3 的"可选"改为**必做项**。
3. 若 nacre-agent 超时参数不可改，则 proxy 侧 drain 默认值需 ≤ 编排预算；两边必须对齐一个数。

---

## 3. 中危问题

### M1：凭证表导出"临时文件"违反本方案自己的验收标准

§4.5.2 写"`e.creds.Items()` → 导出全部 (sid, token) → **临时文件**"，而验收第 13 条要求"**磁盘任意时刻不存在含凭证明文的快照文件**"——(sid, token) 就是凭证明文，且进程被 kill 时临时文件可能残留。这是问题 5（路由快照落盘）的同款错误换个位置复发。

**修复**：改经 **sealed memfd** 传递（fork 时本来就要 ExtraFiles 传 FD，顺手），不落盘。

### M2：admission 不交接的影响被低估为"瞬时"

§4.3 写"resync 完成前 inflight 计数为 0（可能瞬时超限放行）"。实际情况：**整个 drain 窗口（默认 30min）内新老两套 arena 分别计数**——存量连接记在旧 arena、新连接记在新 arena，每个 sandbox 的合并真实 inflight 可达配置限额的 **2 倍**。这不是瞬时，是整个排水期的限额保护降级。

**修复**：§4.3/§5 如实写为"排水期内限额为分域计数（合并可超限，上限 2×）"，并请评审确认该降级可接受（若不可接受，需在 Create/恢复凭证表之外补一个跨域计数方案——但那又回到缓存交接的老路，建议先确认接受）。

### M3：编排时序自相矛盾（停 accept 的时机）

§3.2 的 `GracefulShutdown` 定义包含"通知 worker drain（停止 accept）"，且 §4.2 编排第一步就 trigger GracefulShutdown；但 §4.2 时序图里 "notify drain (stop accept)" 画在**新 master ready 之后**。两种顺序行为不同：

- 若 fork 前就通知停 accept：fork + 恢复 + resync + 新 worker ready 期间（几百 ms～秒级）**没有任何进程 accept**，新连接全部滞留 backlog（不 refused 但明显延迟）；
- 若 ready 后再通知：旧 worker 在此期间继续接新连接（被当作存量排空），无服务停滞。

**修复**：统一为后者——`GracefulShutdown` 拆两段：**前段**（触发时）只做"挂起 worker 监管 + 取 FD + 导出凭证表"；**后段**（新 master ready 后）才"通知旧 worker 停 accept"。§3.2/§4.2 两处改为一致。

### M4：热升级 bootstrap 的身份校验语义未定义（问题 10 后半段）

§6.2 的"热升级 bootstrap（sealed memfd + gen + 路径）"没有写身份校验语义。xproxy 侧新 bootstrap 若复用 componentexec 现有的 `ExecutableIdentity` 同文件身份校验（`internal/componentexec/bootstrap_linux.go:60-80`），旧二进制创建的 bootstrap 会被新二进制**拒绝**（文件身份不同）——升级必然失败在第一步。

**修复**：新 bootstrap 的校验语义明确为"**同 component + `ConfigSchemaVersion` 兼容**"（字段已存在，正好承载）；worker 路径的同文件校验保持不变。

---

## 4. 低危 / 建议

| # | 项 | 说明 |
|----|----|------|
| L1 | "bind 先行 — 删除，非必要" | **认可**：在分代路径设计下，冷启动实例 O_TRUNC 的是 gen0 原路径，不伤在役的 g1。但注意"第二个冷启动实例打断在役实例"是**今天代码就存在**的隐患（真实 master 实测：第二实例 bind 失败退出后，在役 master 隧道直接 404，测试 `TestSecondMasterStartCorruptsServingInstance` 已入仓）——与热升级无关、改动很小，建议顺手修而不是从方案里"删除"了事 |
| L2 | 稳定符号链接丢失 | V2.0 的 `proxy-stats.sock -> .g<gen>` 原子符号链接在 V3.6 里没提——升级后运维/监控脚本按稳定路径 curl 会 404。约 20-30 行，建议捡回 |
| L3 | 回滚的 worker 恢复语义 | 已 drain 的旧 worker 关闭了 listener FD 副本，**无法恢复 accept**——"恢复 worker 监管"的实际含义是拉起 replacement worker（新 epoch）。文档写明，避免实现者误以为原 worker 可复活 |
| L4 | 工作量 | 4.5-6.5 人天对缩减后的范围合理，但 P1/P2 修复 + nacre-agent 适配（必做）计入后实际约 **5.5-7 人天**，建议按此报 |

---

## 5. 证据基础（两轮实测，测试代码已入仓可复跑）

| 验证 | 方法 | 结果 |
|------|------|------|
| **互踢震荡（P1 依据）** | 真实双 proxy master + 真实 conductor（`cmd/node-ctl/proxy_mechanism_test.go`） | **OSCILLATION-CONFIRMED**：0.31s 内 registered=4 / deregistered=4，双进程存活持续抢注 |
| SHM 同路径破坏 + 分代有效 | 真实 `proxyshm` 跨进程（同上文件） | 同路径 Create → 对端视图 WIPED；`.g1` 分代 → 对端 intact |
| bind 顺序危害（L1 依据） | 真实 master 时序（同上文件） | 第二实例 EADDRINUSE 退出后，在役 master 隧道 404 |
| admission memfd attach | 真实 `proxyadmission`（`internal/proxyadmission/arena_exec_test.go`） | memfd 跨 exec 后 mmap 内容 sha256 一致 |
| FD 继承 + exec 新二进制 + listener 交接 + 存量连接 + 符号链接 + `/proc/self/exe` | 独立 PoC（v1/v2 双二进制完整升级流程） | 7/7 PASS：交接 ~4ms、存量连接全程存活、`/proc/self/exe` 替换后 ENOENT |

---

## 6. V3.7 修订要求（按 V3.6 章节映射）

| V3.6 章节 | 修订 |
|-----------|------|
| §1.2 / 附录 A #3 | 删除"砍掉停 subscriber"表述；改为"GracefulShutdown 第一步内部取消 subscriber（非新增接口），回滚时重启" |
| §3.2 | GracefulShutdown 拆前段/后段（M3）；补第一步 cancel subscriber（P1） |
| §4.2 | 时序图统一：notify drain 移到新 master ready 之后（M3）；补 nacre-agent 成功判定语义（P2） |
| §4.3 | "冻结不再写"保留（P1 修复后成立）；admission 行改"排水期分域计数，合并可超限上限 2×"（M2） |
| §4.5.2 | 凭证导出"临时文件"→ sealed memfd（M1） |
| §6.2 | 热升级 bootstrap 补身份校验语义：component + ConfigSchemaVersion（M4） |
| §7.3 | nacre-agent 适配改必做（P2）；实现顺序补"P1 的 subscriber 取消/重启" |
| §9 验收 | 增补见 §7 |
| 附录 A | L1 补注：bind-first 在分代设计下非必要（认可），但并发启动打断在役实例为现存隐患建议顺手修 |

---

## 7. 验收标准增补

- [ ] 升级全程 conductor 侧租约不出现震荡（实测方法：双 master 并存窗口内 registered/deregistered 各 ≤ 1 次）
- [ ] 新 master ready 不依赖旧 subscriber 退出（取消语义生效，Bookmark 单次完成）
- [ ] nacre-agent 在新 PID 健康后即判定成功，不等旧 PID 消失；单组件超时预算 ≥ drain 上限或明确分离
- [ ] 凭证表迁移全程无磁盘明文文件（含失败/被 kill 场景无残留）
- [ ] GracefulShutdown 前段不通知停 accept：fork 至 ready 期间新连接可正常完成握手（无 backlog 停滞）
- [ ] 排水期内 admission 分域计数的降级语义已获评审确认
