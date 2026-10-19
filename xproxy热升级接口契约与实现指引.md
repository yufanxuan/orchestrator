# xproxy 热升级接口契约与实现指引

> **变更编号**: FE2026050900370-BMS节点组件热升级
> **读者**: `kuasar-sandbox-cci`（xproxy）与 `nacre-agent` 的实现者
> **设计依据**: `proxy热升级方案v3.md`（V3.7）+ `proxy热升级方案V3.7评审补充.md`（已采纳）
> **框架基线**: orchestrator main，本仓已交付 commit `4263570`（优雅退出+排水）与 `381093e`（FD 继承与捕获）。**fork 与就绪信号采用零框架改动的 node-ctl 中转方案（§3，已实验验证）——本需求对框架仓无进一步改动**

---

## 1. 分工与依赖总览

| 层 | 仓库 | 状态 |
|----|------|------|
| 框架能力一：优雅退出 + 排水 | orchestrator `4263570` | ✅ 已合入 main |
| 框架能力二：listener FD 继承/捕获 | orchestrator `381093e` | ✅ 已合入 main |
| fork 与就绪信号（node-ctl 中转 + stats 探针） | 无新框架代码 | ✅ 已实验验证（§3，`proxy_hotrelay_test.go`） |
| SIGUSR2 编排 + 凭证表迁移 | kuasar-sandbox-cci | 本文档 §4/§5/§6 |
| nacre-agent 适配 | nacre 仓 | 本文档 §7 |
| 集成测试与验收 | 三仓协同 | 本文档 §8 |

**关键架构约束（实现前必读）**：

1. xproxy 只能 import `github.com/kuasar-sandbox/orchestrator/app/proxy`（公开层）。`internal/`（proxyapp/componentexec/proxystats 等）对 xproxy 不可见——所有框架交互必须经 `app/proxy` 导出面。
2. `componentexec.Exec` 是**进程替换**（`unix.Exec`），老 master 不能用它 fork 继任者（会丢掉排水与存量连接）——必须用 §3 的子进程变体。
3. 新 master 的入口走**现有 App 路径**（receiveComponent → Configure → BindRuntime → RunMaster）：继任者的组件 bootstrap 由框架在 fork 时按新二进制构造，xproxy 侧零新入口代码。

---

## 2. 框架已交付接口契约（commit 4263570 + 381093e）

### 2.1 MasterControl（编排的总入口）

```go
// 获取方式：xproxy 在 BindRuntime(master) 时保存 *proxy.Runtime；
// SIGUSR2 到达后调用（此时 master 已 serving，control 非 nil）。
control := runtime.MasterControl()   // app/proxy 公开方法；worker 进程/未 serving 时返回 nil

// —— 前段（可逆、幂等）——
control.GracefulShutdown()
//   ① 取消 routesync subscriber（路由视图冻结，不再重连——防互踢震荡，P1）
//   ② 挂起 worker 监管（worker 崩溃不再被拉起；运行中 worker 不受影响）
//   ③ 关闭 metrics listener
//   注意：不通知 worker 停 accept（M3：fork 至 ready 期间老 worker 继续接新连接）

// —— 后段（不可逆，仅在继任者 ready 后调用）——
control.DrainWorkers(timeout)
//   通知所有 worker 停 accept + 排空存量（inflight 归零自退出），超时强杀；阻塞直到全部退出

// —— 回滚（前段之后、后段之前失败时）——
control.RollbackPhaseOne()
//   重启 subscriber + 恢复 worker 监管 + 重绑 metrics
//   ★ xproxy 必须在此之后补凭证表恢复（见 §5.3，V3.7.1 #1）

// —— FD 捕获（前段之后、fork 之前）——
dataFD, mmdsFD, err := control.CaptureListenerFDs()
//   返回 dup 出的 *os.File；mmdsFD 在 MMDS 未启用时为 nil；调用方持有并传给 fork
```

**时序约束**：`GracefulShutdown` 必须是 SIGUSR2 处理的**第一个框架调用**（P1 实测依据：双 master 互踢 0.31s 4 回合）；`DrainWorkers` 只能在继任者 ready 之后调用（M3）。

### 2.2 继任者侦测与 listener 恢复（框架内嵌）

框架在 fork 时注入的环境（xproxy 经 §3 的 API 传入，不手工构造）：

```
KUASAR_PROXY_HOT_UPGRADE=1           # 总开关
KUASAR_PROXY_HOT_UPGRADE_DATA_FD=4   # data listener FD 位置（固定）
KUASAR_PROXY_HOT_UPGRADE_MMDS_FD=5   # mmds listener FD 位置（未启用时不设）
```

继任者的 `RunMaster` 自动检测并恢复（跳过 `net.Listen`），**恢复后立即 unset**，且 `withoutWorkerEnvironment` 保证这些变量永不进入 worker 进程。**xproxy 无需写任何恢复代码**。

### 2.3 排水配置

```yaml
# proxy.yaml
drain_timeout: 30m   # 排水上限，默认 30m；master 与 worker 两侧共同遵守
```

实测参考（Docker）：归零即退 23ms；3s 上限下长连接活满 3014ms；1s 上限全链路（断连→退出→端口释放）~1020ms。

---

## 3. fork 与就绪机制（零框架改动，已实验验证）

> 最小改动原则的落实：评审要求框架只交付"两原子能力 + FD 暴露点"。初版契约曾规格化框架侧
> `ForkHotMaster` 公开 API（~250-300 行），按原则修正为以下零改动方案，并已在本仓
> `cmd/node-ctl/proxy_hotrelay_test.go` 用真实进程链（测试进程 → node-ctl → custom-proxy 组件）
> 端到端验证通过（两轮）：env/FD 双跳穿透 ✓、stats 探针就绪 55-156ms ✓、优雅退出 ~1020ms ✓。

### 3.1 fork：经 node-ctl 中转（复用现有派发链）

```
老 xproxy（SIGUSR2 handler，前段+凭证导出之后）
  │ exec.Command(node-ctl, "proxy", "serve", "--config", <冻结配置临时文件>)
  │   ExtraFiles = [dataFD, mmdsFD, ...xproxy 私有 FD]      // 子进程 fd 3,4,...
  │   Env       += KUASAR_PROXY_HOT_UPGRADE=1 + FD 寻址 + xproxy 私有寻址
  ▼
node-ctl 子进程
  │ runProxy → LoadProxy(临时文件) → 检测 paths.proxy_executable 非空
  │ → componentexec.Exec（现有代码，unix.Exec 进程替换）
  │   · 打开磁盘上的新二进制并盖身份章（M4 由此消解：接收端同文件校验对 /proc/self/exe 通过）
  │   · 冻结配置封入 sealed memfd（现有机制）
  │   · withoutBootstrapEnvironment 只剥组件 bootstrap 变量——热升级 env 与 FD 全部穿透
  ▼
新 xproxy（同 PID，组件二进制）
  │ App 路径：receiveComponent → Configure → BindRuntime → RunMaster
  │ → 检测热升级 env → RestoreHotUpgradeListeners（框架已交付）
  ```

**已验证的穿透事实**（`componentexec` 源码 + 实测）：

1. `withoutBootstrapEnvironment` 仅剥离 `KUASAR_INTERNAL_COMPONENT_BOOTSTRAP_FD` 前缀（`bootstrap_linux.go:394-404`）——热升级 env 完整穿透两跳；
2. `exec.Command` 的 ExtraFiles 在 node-ctl 子进程中无 CLOEXEC，`unix.Exec` 进程替换后 FD 编号保持不变——继任者按 env 寻址恢复（实测日志 `data listener restored from inherited descriptor`）；
3. node-ctl 派发要求临时配置含 `paths.proxy_executable`（指向磁盘新二进制——nacre-agent 已替换的文件）。

**冻结配置临时文件**：

- 内容 = 老进程冻结 EffectiveConfig 的声明式重序列化（YAML，含**分代路径重写**，见 §6.1）；
- 经 node-ctl `LoadProxy → FreezeConfig` 重新冻结——**等效冻结**（内容来自老冻结值而非 proxy.yaml，升级窗口内改 proxy.yaml 不生效的语义保持，问题 12 兼容）；分代路径在 Freeze **之前**写入（问题 9 兼容，无 freeze 后改字段）；
- 敏感性 = proxy.yaml 同级（非秘密，0600 临时文件）；ready/回滚后删除。

**FD 布局**（老 xproxy 的 ExtraFiles 顺序，固定不可变序）：

| 子进程 FD | 内容 | 寻址 env |
|---|---|---|
| 3 | data listener | `KUASAR_PROXY_HOT_UPGRADE_DATA_FD=3` |
| 4 | mmds listener（可选） | `KUASAR_PROXY_HOT_UPGRADE_MMDS_FD=4` |
| 5+ | xproxy 私有 FD（凭证快照 memfd 等） | xproxy 自定义 env（如 `XPROXY_HOT_CREDS_FD=5`） |

（组件 bootstrap 的 memfd 由 node-ctl 在派发时自行分配编号，与本布局无冲突。）

### 3.2 就绪信号：stats 探针（现有 503 门语义）

继任者 stats socket 的 `POST /v1/traffic:batchGet` 存在就绪门（`proxystats/socket.go:97`）：
`table.Synced()`（routesync **Bookmark 完成**）**且** `master.Available()`（**worker 集可信**）之前一律 **503**。这正是 V3.7 定义的 ready 条件，且是文档化行为（node-proxy.md §8）。

**探针规格**：

```
POST []（空 batch）→ 未就绪: 503 / 就绪后: 400（gate 先于空数组校验，socket.go:97→105）
老 master 轮询继任者 stats socket（分代路径，老 master 自己生成）：
  每 50ms 一次，30s 超时；503 → 非 503（400/200）即就绪
  探针期间若继任者进程退出 → 失败 → 回滚
```

实测：继任者就绪 55-156ms（两轮）。可选增强（生产探针）：经 MasterHost `Routes().Get(sid)` 取真实
(RunID, Profile, State) 元组发真实查询 → 就绪后 200（`app/proxy/extension` 已暴露路由查询，
custom-proxy 示例的 `WrapManagement` 即此用法）。

**失败即回滚**：探针超时/进程退出 → kill 继任者 → §4.3 回滚。任何启动失败（组件校验、
Configure、凭证恢复、listener 恢复、Bookmark 未完成）都会让探针停留在 503 或进程死亡，天然覆盖。

### 3.3 退路（仅当未来要求更严格信号时）

若评审后续要求"框架主动发 ready"（消除轮询），最小退路是在 RunMaster 热模式加 ready 管道
（env + 就绪时写入，~50 行，归入能力一"FD 传递"范畴）——当前不实施。

---

## 4. xproxy 编排实现规格（cmd/xproxy/main.go + pkg/xproxy/hotupgrade/）

### 4.1 主状态机

```
SIGUSR2 到达（systemd MainPID）
  ├─ 若 in-progress → 记录并返回（幂等，问题八）
  ├─ control := savedRuntime.MasterControl()；若 nil（未 serving）→ 记录错误返回
  ├─ in-progress = true（defer 清理）
  │
  ├─ [1] control.GracefulShutdown()                 // 前段（P1：subscriber 已停）
  ├─ [2] dataFD, mmdsFD := control.CaptureListenerFDs()
  ├─ [3] 凭证导出：credsItems := e.creds.Items(); invokeItems := ...（§5.1）
  ├─ [4] 快照写入 sealed memfd（自持引用 + 传子进程，M1：不落盘）
  ├─ [5] e.creds.Close(); e.invokeShm.Close()        // 释放 Flock
  ├─ [6] 关 metrics 已由前段完成
  ├─ [7] gen := nextGeneration()（§6.1）；写冻结配置临时文件（分代路径 + paths.proxy_executable=新二进制）
  ├─ [8] cmd := exec.Command(node-ctl, "proxy", "serve", "--config", 临时文件)
  │         ExtraFiles = [dataFD(3), mmdsFD(4), credsMemFD(5)]
  │         Env += KUASAR_PROXY_HOT_UPGRADE=1, DATA_FD=3, MMDS_FD=4, XPROXY_HOT_CREDS_FD=5
  │
  ├─ [8b] 轮询继任者 stats socket 的空 batch 探针（§3.2，50ms 间隔 / 30s 超时）
  │        非 503 → 成功路径 ↓ ；超时或进程退出 → kill cmd；回滚（§4.3）
  │
  ├─ [9] 成功：control.DrainWorkers(drainTimeout)     // 后段：老 worker 排水退出
  ├─ [10] sd_notify("MAINPID=<cmd.Process.Pid>")
  ├─ [11] 等待 DrainWorkers 返回（阻塞至全部退出或超时强杀完成）
  ├─ [12] 旧进程退出（App.Run 返回 → 进程结束；credshm Flock 已在 [5] 释放）
  └─ 继任者（新进程内）：框架已自动完成 ready + sd_notify READY 由 xproxy main 在
       BindRuntime 后的自身生命周期里处理（或经 systemd Type=exec 默认语义）
```

### 4.2 与框架 standalone 路径的叠加

升级期间若 systemd stop（SIGTERM）落在老进程上：框架 `RunMaster` 的 standalone 分支会再次调用 `GracefulShutdown`（幂等，无副作用）+ `DrainWorkers`——与 [9] 相同，行为正确。xproxy 无需特判。

### 4.3 回滚（含 V3.7.1 #1 凭证恢复）

```
kill cmd（若未退出）
→ control.RollbackPhaseOne()                     // subscriber/监管/metrics 恢复
→ 凭证表恢复（xproxy 仓内，与正向迁移同构）：
    e.creds = credshm.Create(路径)               // Flock 已在 [5] 释放
    for item := range 自持 memfd 快照 { e.creds.Upsert(item) }
    e.creds.InheritGeneration(savedGen)          // 不重置，避免 nacre-agent 全量重推
    invokeshm 同构
→ in-progress = false；记录升级失败日志
（已 drain 的老 worker 无法恢复 accept——FD 已关；由恢复后的监管拉起 replacement，
  新 epoch，属预期语义，见 V3.7 §8）
```

---

## 5. 凭证表迁移规格（pkg/xproxy/masterext/ext.go 热升级分支）

### 5.1 正向（老 master，SIGUSR2 handler 内）

1. `items := e.creds.Items()`（`writer.go:105`）+ invokeshm 同构 + 当前 generation 值；
2. 写入 **sealed memfd**（参照 `componentexec.newSealedFile` 的 seal 集：WRITE|GROW|SHRINK|SEAL）——**禁止临时文件**（M1，凭证明文不落盘）；
3. 老进程**保留自己的 memfd 引用**（回滚要用），经 `ForkHotMaster` ExtraFiles 传一份给继任者（fd 7）；
4. `e.creds.Close()` 释放 Flock → 继任者才能 Create。

### 5.2 继任者（MasterExtension.Start 的热升级分支）

`Start` 检测 `XPROXY_HOT_CREDS_FD` 存在 → 走恢复路径（否则走现有冷启动建空表路径）：

1. 从 fd 7 读取快照（memfd 跨 exec 内容不变——已有实测：`arena_exec_test.go` 同机制验证）；
2. `credshm.Create()`（Flock 已释放，不 ErrLocked）→ 逐条 `Upsert` → `InheritGeneration`；
3. invokeshm 同构；
4. 校验 generation 单调（快照 gen ≥ 磁盘残留）。

**Reader 无锁语义**（迁移窗口的正确性依据，已有结论）：worker 以 `OpenReadOnly` 只读 attach，Flock 只是写者锁——[5] 之后、继任者 Create 之前，**新 worker 可直接只读旧文件处理新请求**；仅"成为写者"需等锁。

### 5.3 回滚恢复

见 §4.3——老进程自持的 memfd 引用让回滚与正向完全同构。

---

## 6. 分代路径与 stats 符号链接（L2）

### 6.1 gen 记账与路径重写（xproxy Configure hook 内闭环，框架零改动）

**实现位置**：xproxy 的 `Configure` hook（`proxy.Hooks.Configure`，运行于 FreezeConfig **之前**——这是问题 9 的正解：路径覆盖发生在冻结前，不触碰 digest 不变量）：

```
Configure(ctx, cfg):
  1. 扫描磁盘：glob dir(cfg.StatsSocket)/proxy-stats.sock.g* 与 proxy-routes.shm.g*
     → maxGen = 解析出的最大 N（无残留则 0）
  2. 清扫：删除所有 .g<maxGen> 之外的分代残留（覆盖 SIGKILL 场景的孤儿）
  3. 若本次为热升级继任者（XPROXY_HOT_CREDS_FD 存在）：
       gen = maxGen（磁盘上的残留就是上一代留下的？——否：
   ★ 正确记账：老 master 在 [7] 重写路径时已选定 gen = maxGen+1 并已把
     .g<gen> 写入新 cfg；继任者 Configure 从自身 cfg 的路径后缀解析 gen）
  简化规则（推荐实现）：
    - gen 完全由"路径后缀解析"驱动，不做独立状态：
      冷启动（无后缀）= 原路径；有后缀 = 沿用后缀值
    - 老 master [7] 的职责：扫描磁盘 maxGen → 新 cfg 路径打 .g<maxGen+1> 后缀
  4. cfg.StatsSocket / cfg.ShmPath 替换为分代路径后返回（框架随后 Freeze）
```

### 6.2 命名与符号链接

```
proxy-stats.sock          ← 稳定符号链接 → proxy-stats.sock.g<gen>（bind 后原子换：
                              tmp symlink + rename；运维/监控用稳定路径）
proxy-stats.sock.g<gen>   ← 新 master bind 的真实 UDS（新 inode，无 EADDRINUSE/删除竞态）
proxy-routes.shm.g<gen>   ← 新 master O_TRUNC 重建（无人 mmap，安全）
```

- 老 master 退出的 `os.Remove` 只删自己 cfg 里的老路径（分代后互不误删）；
- 继任者 bind 后执行符号链接原子替换（xproxy 在 BindRuntime 后、或经 MasterExtension.Start 时机执行，~20 行）；
- 冷启动（无 .g 残留）不建符号链接，行为与现状一致。

---

## 7. nacre-agent 适配规格（P2 必做）

| 项 | 规格 |
|----|------|
| 成功判定 | **新 PID 出现且健康**（HealthCheck 打新 master 的数据面/管理面）即判成功，**不等旧 PID 消失**——旧进程排水是后台收敛（默认 30min） |
| WaitNewPID | 以**终态**判定：新 PID 存活 + 健康；不要求"旧 PID 全部消失" |
| CheckPID | 升级窗口内 cgroup 进程数 = 双倍（老 master+workers + 新 master+workers）——容忍，不做数量断言失败 |
| MAINPID | 老进程 `sd_notify MAINPID=<新pid>` 后切换；nacre-agent 的 systemd 查询随 MainPID 走 |
| 超时预算 | 单组件 120s < drain 30min 的冲突由"成功判定前移"消解：判定点在 ready（<30s），远小于 120s；**不要**把"旧进程退出"纳入超时预算 |
| 停止/回退 | systemd stop 落在升级中 = cgroup 全杀（等效冷停，可接受）；回退流程需认识该终态 |

---

## 8. 测试与验收映射

### 8.1 可复用的现成资产（orchestrator 仓，Docker linux/arm64）

| 资产 | 用途 |
|------|------|
| `cmd/node-ctl/proxy_hotupgrade_test.go`（TestProxy* 系列） | 冷启动/并发拒绝/worker 崩溃/优雅排水回归 |
| `cmd/node-ctl/proxy_hotfd_test.go` | **继任者经继承 listener 服务的端到端样板**（xproxy 集成测试可直接抄此模式） |
| `cmd/node-ctl/proxy_hotrelay_test.go` + `examples/custom-proxy` | **两跳中转全链路样板**（node-ctl 派发 + env/FD 穿透 + stats 就绪探针——xproxy 编排的直接参照；组件替身用 `go build ./examples/custom-proxy`） |
| `cmd/node-ctl/proxy_mechanism_test.go` | 互踢震荡/SHM 破坏/bind 顺序隐患/双 master——升级全程复跑断言"registered/deregistered 各 ≤1 次" |
| `internal/proxyadmission/arena_exec_test.go` | memfd 跨 exec 模式（凭证 memfd 同机制） |

### 8.2 xproxy 侧必测清单（对照 V3.7 §9）

1. 端到端：SIGUSR2 → 双进程并存 → 存量长连接活过交接 → 排水完成 → 老进程退出（连续升级 gen 0→1→2 无残留）；
2. 回滚：ready 超时注入 → 老进程恢复（subscriber/监管/metrics/凭证表）→ 数据面持续可用；
3. 凭证：迁移后无重推窗口（generation 继承）；失败/被 kill 场景磁盘无明文残留；
4. 震荡回归：升级全程 conductor 侧租约 registered/deregistered 各 ≤1 次；
5. 二进制：升级后 `/proc/<新pid>/exe` 指向磁盘新文件、worker 链路同版本；
6. stats：稳定路径 `proxy-stats.sock` 任意代数可拨通且命中当前 master。

---

## 9. 实现顺序建议与工作量

| 步 | 内容 | 仓 | 估算 |
|----|------|----|------|
| 1 | Configure 分代重写 + 符号链接 | kuasar-sandbox-cci | 0.5 人天 |
| 2 | 凭证表迁移（正向 + 继任者 + 回滚） | kuasar-sandbox-cci | 1 人天 |
| 3 | SIGUSR2 状态机 + node-ctl 中转 fork + stats 探针 + 回滚编排 | kuasar-sandbox-cci | 1-1.5 人天 |
| 4 | nacre-agent 适配 | nacre | 0.5-1 人天 |
| 5 | 三仓联调 + §8.2 全清单 | 协同 | 1-1.5 人天 |
| | **合计（无框架依赖）** | | **4-5.5 人天** |

**阻塞关系**：步骤 1/2/3 均无框架依赖（两个原子能力已合入），可并行；步骤 5 依赖全部。
