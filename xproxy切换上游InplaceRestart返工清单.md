# xproxy 切换上游 InplaceRestart 方案返工清单

> 决策背景：上游 orchestrator `280730d`（#472）实现了 socket activation + graceful shutdown +
> InplaceRestart，与本需求（FE2026050900370）功能同域。已决策**切换到上游方案**。
> 本清单基于上游 #472 真实代码与文档（`docs/node-proxy.md` §9.4/§9.5、`docs/extensions.md`
> Proxy lifecycle）逐项比对内网 xproxy 现有实现（提交 `9c19208` + `36b176bd`）产出。

---

## 〇、上游方案速览（与内网方案的机制对照）

**上游流程**：`A → ready B → drain A → 原 PID exec 成 A′ → 回收 B → 仅 A′`

- A = 原 master；B = candidate（新二进制，用同目录**私有名** shm/stats + 相邻 metrics 端口 p+1）
- B 业务就绪后 → A 排水 → **A 原地 exec 加载新二进制（PID 不变）** → A′ 就绪 → B 排水回收
- 二进制更新方式：**原子替换 `paths.proxy_executable` 指向的文件**（无中转、无 fork relay）
- listener 由 Service 层持有副本，替换期间**不重绑**（地址 listener 与激活 listener 同规则）

| 维度 | 内网现行方案（作废方向） | 上游方案（目标） |
|---|---|---|
| 触发 | SIGUSR2，xproxy 自己捕获 | SIGUSR2 框架原生注册（`inplace_restart: true`），或 `App.InplaceRestart(ctx)` |
| 继任方式 | 老 master fork 继任者（新 PID）+ node-ctl 中转 | **原 PID exec 替换**，systemd 无感知 |
| systemd | sd_notify MAINPID 转移 + `Type=notify` 四件套 | **`Type=simple` 即可**（PID 不变），配 socket units 模板 |
| listener 交接 | `KUASAR_PROXY_HOT_UPGRADE_*_FD` env + ExtraFiles | Service 持有副本 / systemd `LISTEN_FDNAMES` 激活 + `fd=<name>` 表达式 |
| 就绪判定 | stats 503 gate + data dial 两阶段探针 | 框架 `WaitReady`/业务就绪（Bookmark + workers） |
| 路径分代 | `.g<N>` 递增 + 清扫 | **A/A′ 沿用配置原名，B 用框架生成的私有名**；退出后只删自己的 |
| drain 配置 | `drain_timeout` | `shutdown_timeout`（正数，零 = 立即清理；`inplace_restart` 依赖它为正） |
| 失败恢复 | xproxy 编排层 Rollback | 框架自动：换 B、重试 exec、重试 A′ 初始化（30s 准备上限，1/2/4/8/10s 退避） |

**系统级前置（三项硬依赖，缺一不可）**：
1. conductor 必须升级到含 #472 的版本（proxy 注册上报 `InplaceRestartV1` capability，conductor 侧协商）
2. 部署模板改造：`kuasar-proxy-data.socket` / `kuasar-proxy-mmds.socket`（可选）+ service 单元（上游给全套模板）
3. nacre-agent 编排适配（见 §四）

---

## 一、可整体删除的 xproxy 代码（上游已有等价物）

| 现有模块 | 处置 | 上游等价物 |
|---|---|---|
| `pkg/hotupgrade/orchestrator.go`（Run/Drain/Rollback/SuccessorPID、两阶段探针、ExtraFiles 组装） | **删除** | Service 引擎 + `WaitReady`/`Status` |
| `cmd/hotupgrade.go`：`installHotUpgradeHandler` / `runHotUpgrade` / `sdNotify` | **删除** | 框架原生 SIGUSR2 注册；PID 不变，MAINPID/notify 无意义 |
| node-ctl relay fork 链（老 xproxy exec node-ctl 再 exec 新 xproxy） | **删除** | 原子替换 `proxy_executable` 文件 + A 原 PID exec |
| 分代路径工具集：`applyNextGen` / `stripGenSuffix` / `genScan` / symlink 维护 + Configure 里的残留清扫 | **删除** | 框架管理 B 私有名与资源回收；A/A′ 恒用原名 |
| stats 探针（`probeStatsEmptyBatch` 503 gate + data dial） | **删除** | `WaitReady` + B 私有控制面 |
| `RestoreFromEnv` / `IsHotUpgrade` 的 env 检测分支（`pkg/masterext/ext.go` Start 内） | **删除** | 框架 Bookmark 初始化（"no old-instance catch-up condition"） |
| `sdNotify`、空壳退出改造（os.Exit(0) 那次修改） | **删除** | 不再需要 |

估计净删除约 1500~2000 行（原 `pkg/hotupgrade` 编排面 + `cmd/hotupgrade.go` 主体）。

## 二、必须保留并重写的部分（内网定制，上游无等价物）

### 2.1 凭证/side-table 迁移（**最大难点，需方案决策**）

上游**没有 A→B/A′ 的扩展状态传递机制**（extension 参与点仅有 Configure / BindRuntime /
Start / Shutdown），凭证 memfd 快照链（`snapshot.go` / `migrate.go` 的 Export/Restore）没有现成挂点。三个选项：

**已排除：向上游提需求。** 开源 owner 不会接受（决策输入），且上游设计哲学有直接佐证——
文档明示 "There are no persistent statistics records, completion files or cross-instance
counter aggregation"（连统计这种低敏数据都不做跨实例传递），InplaceRestart 的设计就是
"实例无状态、重初始化"，明文凭证状态传递通道与其相悖。

收敛为 A/B 二选一，关键差异（此前未展开的两点）：

| 维度 | **A. nacre 重推转正** | **B. credshm/invokeshm 共享 mmap** |
|---|---|---|
| 做法 | B 空表起步，nacre 升级后全量重推凭证；memfd 机制退役 | A/B/A′ mmap **同一文件**（xproxy 自管路径，不走框架分代）；表内容经 MAP_SHARED 页天然同步，**根本无迁移**；Flock 重定义为写者互斥 |
| 凭证表 | ✅ 可重推（nacre 有源） | ✅ 天然共享 |
| **调用表（invoke）** | ❌ **没有重推源**——nacre 不持有这份副本，窗口期 /v1 调用状态查询不完整，需业务单独评估丢弃影响 | ✅ 天然共享，无窗口 |
| 窗口期安全/可用性 | 新请求 token 校验降级（no-token 放行），安全降级需业务书面确认 | 无降级窗口 |
| 实现难度 | 低（F4 降级路径已有，转正即可） | **可能比预想低**：credshm 本就是 mmap 共享文件 + Flock 的跨进程设计（master/worker 共享）；上游模型下 A/A′ 同 PID 同路径、B 按同一固定路径 open 即可。需审计两点：A/B 并存窗口的写时序（nacre 窗口期推新凭证给谁写）；表格式版本的前向兼容纪律（A 老格式 ↔ B 新格式） |
| 依赖 | nacre 重推及时性 | 表结构版本管理纪律 |

**推荐策略**：**B 为主目标**（一次解决两张表、无降级窗口、且 credshm 的既有架构基础使其
可能接近零改动），A 降级为 B 验证失败时的兜底路径（届时需补 invoke 表丢弃影响评估 +
no-token 降级的业务确认）。B 的设计验证（写时序审计 + 格式兼容性测试）建议与切换阶段 1
并行启动。

### 2.1.1 B 方案可行性验证结论（已完成，**可行**）

验证分两层，结论：**B 方案可行**，唯一硬性前提是路径来源独立（见下）。

**机制层 PoC（Docker linux/arm64 实测，全绿）**——完整模拟上游时间线
（A 写 → B 读并写 → A exec → B 获锁再写 → A′ 重建读取）：

| # | 命题 | 结果 |
|---|---|---|
| 1 | 独立进程 mmap MAP_SHARED 互见（A→B） | ✅ B 读到 A 全部字段 |
| 2 | 反向互见（B→A，B 无锁写、A 持锁读） | ✅ flock 建议锁不阻塞 mmap 读写，读多写少友好 |
| 3 | Flock 跨进程写者互斥 | ✅ A 持锁时 B `LOCK_NB` 被拒 |
| 4 | **exec 后锁自动交接** | ✅ A exec（Go fd 默认 CLOEXEC）→ 锁随 fd 关闭自动释放 → B 立即获锁 → B 退出 → A′ 阻塞获锁——**上游 exec 流程天然匹配锁交接，无需现行 PrepareHotUpgrade 的显式释放协议** |
| 5 | exec 后文件数据持久（A′ 重新 open+mmap 重建视图） | ✅ A exec 前写的数据完好 |
| 6 | 跨世代互见（B 在 A exec 后写、A′ 读到） | ✅ |

**框架层代码走查（上游 #472 源码，无阻碍）**：

1. **资源清理边界**：`retainedResources` 只保留/清理框架自知的资源（data/MMDS/stats/metrics
   listener + 框架 shm）；extension 自管文件（credshm/invokeshm）不在其列，框架不触碰
2. **B 的 config 改写面**（`service.go` candidate 分支）：**仅替换 `StatsSocket` 与 `ShmPath`
   两个字段**为 Service 私有资源路径，其余字段原样传递（pinned baseline）——xproxy 自有
   配置字段不受波及
3. extension 在 B 进程内的 Start/BindRuntime 环境自由，可 open 任意路径
4. 佐证选项 C 出局的判断：上游框架自身用 sealed memfd 传 continuation 状态
   （`service_continuation_linux.go`），属框架内部机制，不对 extension 开放

**收敛后的内网确认项（1 硬性 + 2 联调）**：

| 级别 | 事项 | 说明 |
|---|---|---|
| **硬性前提** | credshm/invokeshm 路径**不得派生自框架 `shm_path` / `stats_socket`** | 这两个字段在 B 的 config 里被私有名化；若现行实现是派生（如 `shm_path + ".creds"`），改为 xproxy 自有配置字段或固定约定路径——明确的小适配点 |
| 联调 | 写时序：nacre 窗口期推新凭证给谁写 | PoC 已证协议层可行（无锁读 + Flock 写互斥 + exec 交接）；归属 A 还是 B 属 xproxy/nacre 联调确定 |
| 联调 | 表格式版本前向兼容 | A（老格式）与 B（新格式）并存窗口需格式兼容；建议表头 version 字段 + 升级路径只做向后兼容变更 |

### 2.2 `SetGeneration` 体系（nacre 观测语义）

现行目的：热升级恢复 generation，防 nacre 把 restart 当冷启动触发全量凭证重推。上游模型下：

- PID 不变 + 原 shm_path 沿用（A′ 沿用 A 的表），**generation 断层问题本身大幅弱化**
- 上游暴露 `RestartID` / `Status` / traffic 响应中的 `inplaceRestart: true`（从受理到清理全程）
- 返工：`shmtable/credshm/invokeshm` 的 `SetGeneration` 是否保留取决于 §2.1 的选项；
  nacre 侧重推判据需改为消费 `inplaceRestart` 标志/`RestartID`，**不再看 PID 变化**

### 2.3 extension 生命周期对接（**新增必做项**）

- **`Shutdown` 钩子是 InplaceRestart 的硬性要求**（"extensions must provide Shutdown；不实现则
  操作在 drain A 之前即被拒绝"）。masterext 与 workerext 都需实现：
  ```go
  type Shutdown interface { Shutdown(context.Context) error }
  ```
  语义：业务已排空后、core 取消 Start ctx 之前调用，入参 ctx 携带剩余预算；Join 后台工作、
  只释放自有句柄、不关共享 listener、完成后不再持有 Host
- BindRuntime 每 epoch 重新调用（A′/B/worker 全部如此），现行"一次性捕获 runtime/masterExt"
  的写法需按此重审；master BindRuntime 可投递 `Runtime.ListenerFiles`（如走 activation）
- Configure 在 A′ 中**从受理时固定的原始输入重跑**（pinned target：受理时固定二进制 identity
  + digest + 原始 YAML，后续磁盘变更不影响本次操作）——`lastConfig` 的保存逻辑删除，改为无状态

## 三、配置与部署返工

| 项 | 现行 | 改为 |
|---|---|---|
| `drain_timeout` | 自有字段 | `shutdown_timeout: 30s`（正数；零=立即；`inplace_restart` 依赖它） |
| `inplace_restart` | 无 | `true`（注册原生 SIGUSR2；`App.InplaceRestart` 调用则两种取值皆可） |
| `data_listen` | `:3443` | 可选改 `"fd=proxy-data"`（配合 socket unit）；保持 TCP 地址也合法 |
| metrics | 单端口 | 规则变化：固定 `host:p` 时 A/A′ 用 p、**B 用 p+1**；65535 拒绝操作；B 需在 A drain 前成功绑定 |
| proxy.service | 内网四件套（notify） | **回退 `Type=simple`** + `Sockets=` 引用 socket units；`KillMode=control-group`、`TimeoutStopSec > shutdown_timeout`（上游模板 45s 对 30s） |
| socket units | 无 | 新增 `kuasar-proxy-data.socket`（MMDS 可选 `-mmds.socket`，支持 `NetworkNamespacePath`） |
| 二进制发布 | node-ctl + xproxy 同发 | **原子替换 `proxy_executable` 文件**为发布动作；版本采用需"先普通启动一次"完成初代采纳 |

资源预算提示（上游文档明示）：预留双实例、双配额、FD/内存峰值与相邻 metrics 端口。

## 四、周边系统返工

### 4.1 conductor
- 必须升级到含 #472 的构建（`configsock` capability 协商：proxy 上报 `InplaceRestartV1`）
- MMDS 走激活 listener 时，conductor 侧 policy 下发 `mmds.listen: "fd=proxy-mmds"` 形式

### 4.2 nacre-agent（P0 接口整体重定义）
- 触发：`systemctl kill --kill-who=main --signal=SIGUSR2` 仍可用；但**等待/健康判定语义全变**——
  PID 不变，WaitNewPID 失效，改为消费 traffic 响应的 `inplaceRestart` 标志 / `Status` / 日志终态
- CheckPID：升级窗口内容忍双实例（A/A′ 与 B 并存），上限两套 master+workers
- 凭证重推：若选 §2.1 选项 A，重推成为升级后的标准动作（不再是异常兜底）

### 4.3 运维/监控
- metrics 双端口（p/p+1）采集适配；升级期指标可能短暂缺口（上游明示可接受）
- `systemctl status` 的 MainPID 恒为真实 master（对比旧方案的空壳问题，此为净收益）

## 五、框架仓（orchestrator fork）本地提交处置

本地 main 领先 upstream 7 个提交，**全部属于被 #472 取代或被上游模型消解的范围**：

| 提交 | 处置 | 理由 |
|---|---|---|
| `4263570` PR1（drain/优雅退出） | 放弃 | 上游 graceful shutdown + Service drain 体系取代；`drain_timeout` 字段被 `shutdown_timeout` 取代 |
| `381093e` PR2（FD 继承/env 契约） | 放弃 | 上游 activated listeners + Service listener 副本取代 |
| `92d1399` / `ca41c06`（集成测试） | 放弃 | 测试对象（中转/分代机制）不存在了 |
| `051c0d7`（shutdown 竞态修复） | 放弃 | master/process/worker 被上游重写，修复随旧结构消亡 |
| `248e428`（UT 补齐） | 放弃 | 同上 |
| `20c0ced`（sd_notify READY=1） | 放弃 | 上游模型 PID 不变 + `Type=simple`，READY/MAINPID 均无消费者 |

**建议操作**：本地留 `legacy/hotupgrade-inner` 备份分支后，main 直接 **reset 到 `upstream/main`**
（`280730d`）。比逐文件解 6 处冲突干净——所有冲突文件的"本地侧"内容都已判废。
xproxy 的框架依赖版本要求随之变为 **orchestrator ≥ #472（`280730d`）**。

配套文档处置：《xproxy热升级接口契约与实现指引》《systemd就绪通知适配指引》《内网验证结果回应》
标注"已被上游 #472 方案取代"归档，避免误导后续维护者。

## 六、工作量估算与建议顺序

| 阶段 | 内容 | 粗估 |
|---|---|---|
| 1 | 框架仓 reset 到 upstream/main + CI/构建打通 + xproxy 编译适配（删 §一 清单 + Shutdown 钩子） | 2~3 人天 |
| 2 | **凭证迁移选项 B 落地**（credshm/invokeshm 共享 mmap：固定路径 + 写时序审计 + 格式前向兼容测试）；并行做 B 的设计验证，失败则回退选项 A 并补齐其两项业务确认（invoke 表丢弃影响、no-token 降级） | 3~5 人天（含联调） |
| 3 | conductor 升级 + 部署模板（socket units + service）改造 | 1~2 人天 |
| 4 | 端到端重验（对照内网原验证矩阵逐项重跑 + 失败恢复场景：换 B/重试 exec/升级窗口 stop） | 3~4 人天 |

**风险提示**：
- 上游 #472 的 Service 体系庞大（service.go 808 行 + continuation/control/entry 等），xproxy 团队
  需预留理解成本；建议先跑通上游原生 `examples/custom-proxy` 的 InplaceRestart E2E 再动手改造
- conductor/nacre/部署三线必须同版本窗口升级（capability 协商失败会直接拒绝 InplaceRestart）
- 凭证选项 A 的窗口期降级需业务方书面确认

---

*上游参考：`docs/node-proxy.md` §9.2 Business readiness / §9.3 Bounded shutdown / §9.4 InplaceRestart /
§9.5 Named listeners；`docs/extensions.md` Proxy lifecycle；部署模板内嵌于 §9.5。*
