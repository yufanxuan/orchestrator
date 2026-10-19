# xproxy 热升级切换上游方案：实施内容与测试指引

> 配套文档：《xproxy切换上游InplaceRestart返工清单.md》（决策与工作量）、其 §2.1.1（B 方案
> 可行性验证结论）。本文聚焦执行层：**xproxy 具体改什么、怎么测**。
> 前提：orchestrator 框架 ≥ `280730d`（#472），框架仓零改动，全部工作在 xproxy 仓 +
> nacre 仓 + 部署配置。

---

## 一、前置条件（开工前逐项确认）

| # | 项 | 判定方式 |
|---|---|---|
| 1 | 框架构建 ≥ `280730d` | `node-ctl --version` / 构建脚本基线；含 `inplace_restart` 配置字段 |
| 2 | conductor 同版本窗口 | conductor 注册面接受 `InplaceRestartV1` capability（configsock 协商） |
| 3 | nacre-agent 适配排期 | 触发改 `systemctl kill --kill-who=main --signal=SIGUSR2`；等待语义改消费 `inplaceRestart` 标志 |
| 4 | 部署模板就绪 | socket units（`kuasar-proxy-data.socket` 等）+ service 单元（上游 `docs/node-proxy.md` §9.5 全套模板） |
| 5 | **credshm/invokeshm 路径来源审计** | **硬性**：确认现行路径不派生自框架 `shm_path`/`stats_socket`（这两个字段在 B 的 config 中被框架私有名化）；若是派生的，先改为 xproxy 自有配置字段（见 §三） |

---

## 二、代码改动清单

### 2.1 删除（上游已覆盖，预计净删 1500~2000 行）

| 文件/模块 | 删除内容 | 上游等价物 |
|---|---|---|
| `pkg/hotupgrade/orchestrator.go` | 整文件：编排核心、`Orchestrator.Run/Drain/Rollback/SuccessorPID`、两阶段探针、ExtraFiles 组装 | 框架 Service 引擎 + `WaitReady`/`Status` |
| `pkg/hotupgrade/snapshot.go` + `migrate.go` | memfd 快照导出/导入（`CredsSnapshotList` 序列化保留与否见 §三） | 由共享 mmap 取代（无迁移） |
| `cmd/hotupgrade.go` | `installHotUpgradeHandler` / `runHotUpgrade` / `sdNotify` / 分代路径工具集（`applyNextGen`/`stripGenSuffix`/`genScan`/symlink 维护） | 框架原生 SIGUSR2 注册 + B 私有名机制 |
| `cmd/main.go` | Configure 保存 `lastConfig` 的逻辑、分代残留清扫、`initHotUpgradeContext` | 上游 pinned baseline（受理时固定原始输入，重跑 Configure） |
| `pkg/masterext/ext.go` | `Start` 内 `IsHotUpgrade()` 分支与 `RestoreFromEnv` 调用 | 框架 Bookmark 初始化（B 无追赶条件） |
| 对框架 `MasterControl` 的全部调用 | `GracefulShutdown`/`CaptureListenerFDs`/`RollbackPhaseOne` 编排调用点 | API 在新框架中不存在，编译期即暴露全部调用点 |

对应 UT 一并删除：orchestrator 编排测试、分代路径测试、探针测试。

### 2.2 重写

| 文件 | 改动 |
|---|---|
| `pkg/masterext/hotupgrade.go` | 保留数据结构，重写生命周期：删除 `PrepareHotUpgrade`/`RestoreHotUpgrade`/`RollbackHotUpgrade`（memfd 迁移三件套），改为 §三的共享 mmap 打开逻辑 |
| `pkg/masterext/ext.go` | `Start` 只做"打开共享表"（固定路径 open + mmap + 视初始化）；新增 `Shutdown` 实现（见 2.3） |
| worker 侧 extension | 同步新增 `Shutdown`；worker 对共享表的只读访问保持 |
| `pkg/shmtable` 等三包的 `SetGeneration` | 共享 mmap 下 A/B/A′ 天然同 generation，**大概率整体退役**；保留与否由 §五 T2 场景结论定 |

### 2.3 新增

**`Shutdown` 钩子（必做，缺它 InplaceRestart 被直接拒绝）**：

```go
// master 与 worker extension 均需实现
func (e *masterExtension) Shutdown(ctx context.Context) error {
    // 语义：业务已排空后、框架取消 Start ctx 之前被调用，ctx 携带剩余预算。
    // 要求：Join 后台 goroutine；只释放自有句柄；不关闭共享 listener；
    //      完成后不再持有 Host。无后台工作的 worker 侧可直接 return nil。
    // 注意：共享 credshm/invokeshm 的 mmap 与 fd 在此【不要】关闭——
    //      它们要跨 exec 存续给 A′/B（见 §三锁协议）。
    return e.background.Wait(ctx)
}
```

**配置载体（推荐独立文件，维持框架零改动）**：xproxy 自有配置（credshm/invokeshm 路径、
Flock 策略、表格式版本）放入独立 YAML（如 `/etc/node-ctl/xproxy-ext.yaml`）由 extension
自行读取——**不要**写入 `proxy.yaml`（框架严格解析 `KnownFields(true)` 会拒绝未知字段，
加字段即破坏"框架零改动"）。

---

## 三、B 方案实施细节（credshm/invokeshm 共享 mmap）

机制已 PoC 实证（返工清单 §2.1.1），实施要点：

1. **路径规则**：固定路径（如 `<run_root>/xproxy-creds.shm` / `xproxy-invoke.shm`），来自
   xproxy 独立配置；A/B/A′ 一律按同一路径 `open(O_RDWR)` + `mmap(MAP_SHARED)`。
   首次创建者 `O_CREATE` + `ftruncate` 初始化表头（magic + version + 容量）。
2. **Flock 协议**（PoC 实证的时序链）：
   - 写者以 `LOCK_EX` 持锁（持有期间为"活跃写者"，即当前接凭证推送的实例）
   - 读者（其他实例/worker）无锁直接读——flock 是建议锁，不阻塞 mmap 读
   - **A exec 时锁随 fd（CLOEXEC）自动释放**——无需任何显式交接代码；B 在窗口期轮询
     `LOCK_NB` 抢锁成为新写者；B 退出释放，A′ 阻塞 `LOCK_EX` 接管
3. **表头版本字段**：`version uint32`，升级路径只做向后兼容变更（新增字段追加尾部、语义位
   保留）；A（旧格式）与 B（新格式）并存窗口以 B 兼容读 A 为准。
4. **写时序约定（与 nacre 联调确定）**：凭证推送通道在升级窗口期打给谁——建议规则：
   "推送目标 = 当前 Flock 持有者"（A 持锁时打 A，A exec 后 B 已持锁打 B），由 nacre 按此
   发现目标；xproxy 侧收到推送的实例持锁写入，其余实例经 MAP_SHARED 天然可见。
5. **清理规则**：任何实例的 Shutdown/退出路径**不得删除/截断**共享表文件；表的重建只发生在
   显式运维操作或表头 magic 损坏时。

---

## 四、配置与部署

**proxy.yaml**（框架字段）：

```yaml
shutdown_timeout: 30s        # 正数；drain 预算；原 drain_timeout 语义
inplace_restart: true        # 注册原生 SIGUSR2
data_listen: "fd=proxy-data" # 可选；配合 socket unit，也可保持 TCP 地址
metrics_listen: 127.0.0.1:9095  # 注意：固定端口时 B 自动用 p+1，确保 p+1 可用
```

**部署**（模板取自上游 `docs/node-proxy.md` §9.5）：`kuasar-proxy-data.socket`（+可选
`-mmds.socket`，MMDS 的 netns 用 socket unit 的 `NetworkNamespacePath`）+ `kuasar-proxy.service`
（`Type=simple`、`Sockets=`、`KillMode=control-group`、`TimeoutStopSec=45s > shutdown_timeout`、
`Restart=on-failure`、独立 `RuntimeDirectory`）。

**二进制发布**：新版本以**原子替换**（`install + rename`）落到 `paths.proxy_executable`
指向的路径；首次从旧版本采用需先普通重启一次完成初代采纳。

---

## 五、测试计划

### 5.1 单元测试

| 编号 | 测试 | 要点 |
|---|---|---|
| U1 | `Shutdown` 钩子 | 预算内 Join 后台任务；不关共享表 fd；超时返回错误 |
| U2 | 共享表打开/初始化 | 首建（O_CREATE + 表头）、二次打开（校验 magic/version/容量）、版本不匹配的兼容读路径 |
| U3 | Flock 协议 | 持锁写者互斥（LOCK_NB 拒绝）、无锁读不受阻、锁释放后接管——可参照 PoC 的三进程模型改造为 UT |
| U4 | 表格式前向兼容 | 旧格式字节流 → 新代码读取（追加字段场景）round-trip |
| U5 | 删除回归 | 删除模块后全仓编译 + 既有 UT 全绿（确认无残留引用） |

### 5.2 集成 / E2E 场景矩阵

| 编号 | 场景 | 操作 | 预期与判定 |
|---|---|---|---|
| T1 | 基础升级 | 部署新二进制 → `systemctl kill --kill-who=main --signal=SIGUSR2 kuasar-proxy.service` | 日志出现 A→B→exec→A′ 序列；`systemctl status` MainPID 不变；完成后仅 A′ 存活 |
| T2 | 零空窗 | 升级前建立长连接（心跳流）持续 curl 数据面 | 全程无断连；替代原"零空窗 400 无 000"验证 |
| T3 | 凭证连续性（**B 方案核心**） | 建立带 token 的沙箱会话 → 升级 → 同 token 复访 | 升级前后 token 校验连续通过，无 no-token 降级窗口 |
| T4 | 并发写时序 | 升级进行中由 nacre 推送新凭证 | 按 §三.4 规则写入；升级完成后全部实例可见；无丢失 |
| T5 | 连续升级 ×3 | 依次替换三个版本逐次触发 | 每代 MainPID 不变；共享表内容跨三代连续；无残留 B |
| T6 | 失败恢复：B 失败 | 部署损坏二进制后触发 | 框架自动换 B 重试（1/2/4/8/10s 退避）；A 全程服务不中断；恢复正确二进制后升级自愈完成 |
| T7 | 失败恢复：升级窗口 stop | 升级进行中 `systemctl stop` | cgroup 全杀等效冷停（已文档化终态）；重启后正常服务，共享表完好 |
| T8 | 优雅 stop | 有 inflight 长连接时 `systemctl stop` | worker 按预算排空后退出；`TimeoutStopSec=45s` 内完成；日志 `completed` |
| T9 | metrics 双端口 | 升级期间抓取 p 与 p+1 | B 起来后 p+1 可抓；A′ 接管后回 p；计数器允许重置 |
| T10 | nacre 观测 | 升级全程 nacre 侧观测 | traffic `inplaceRestart: true` 从受理到清理；nacre 不误判为冷启动（不触发非预期全量重推） |
| T11 | 回滚 | 升级失败后框架自动恢复 / 显式取消 | A 恢复独占服务；共享表 Flock 回到 A；无半开状态 |

### 5.3 观测命令速查

```sh
# 触发（nacre 编排同款）
systemctl kill --kill-who=main --signal=SIGUSR2 kuasar-proxy.service
# 全程状态（MainPID 应始终不变）
systemctl status kuasar-proxy.service
# traffic 观测 in-place 标志（经 conductor / stats socket）
# B 阶段：MainPID 的管理端点短暂不可用属预期，看 B 私有控制面日志
journalctl -u kuasar-proxy.service -f
```

### 5.4 验收基准

对照内网原验证矩阵逐项重跑（原矩阵全部项目在上游方案下等价映射到 T1~T11），新增项为
T4（并发写）、T6/T7（失败恢复，上游新能力，原方案未覆盖）。全部通过 = 切换验收完成。

---

## 六、实施顺序

1. 框架基线切换（xproxy 编译跑通：删 §2.1 + 空实现 Shutdown）→ U5
2. 共享 mmap 落地（§三）→ U2/U3/U4
3. 配置与部署模板（§四）→ T1/T2 基础链
4. nacre 适配联调 → T4/T10
5. 失败恢复与终态（T6/T7/T8/T11）
6. 全矩阵回归（T1~T11）→ 验收

粗估 9~14 人天（与返工清单 §六一致）；T3/T4 是 B 方案的核心验收场景，建议尽早排入。
