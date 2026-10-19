# 框架侧 sd_notify 就绪通知落地与 xproxy 适配指引

> 回应《proxy热升级问题分析.md》§四（老进程不退出）与 §三（框架改进点评估）。
> 框架侧已按**方案 A** 完成实现（orchestrator commit `20c0ced`），本文档说明：
> 1. 框架改了什么
> 2. 通知分工契约（谁发什么）
> 3. xproxy 侧适配步骤（unit 切换 + 代码改动）
> 4. 切换后的行为变化与运维面
> 5. 对 §三各改进点的框架侧裁决

---

## 一、结论先行

| 事项 | 结论 |
|---|---|
| §四 老进程不退出 | **框架已实现方案 A**：RunMaster serving 后自动发 `READY=1`；xproxy 移除 `select{}` 空壳，改为 `MAINPID` 通知后正常退出 |
| 为什么之前 Type=simple 无效 | systemd **只给 `Type=notify` 的 unit 注入 `NOTIFY_SOCKET`**，simple/exec unit 里进程拿不到 socket 路径，`sdNotify` 是 no-op——切 unit 后现有代码立即生效 |
| 维持 Type=simple 让老进程退出 | **不可行**：默认 `KillMode=control-group` 会连带杀新 master；改 `KillMode=process` 则新 master 孤儿化（脱离 systemd 管理）。`MAINPID` 转移是唯一合法交接手段，且只在 notify 类型下生效 |
| §三 3.2/3.3/3.4 | 不做（理由见 §六），xproxy 现有 workaround 继续有效 |
| §三 3.5 环境标记常量化 | 可选，低优先；见 §六 |

---

## 二、框架侧改动明细（commit `20c0ced`）

| 文件 | 改动 |
|---|---|
| `internal/proxyapp/sdnotify.go`（新增） | `notifySystemdReady(logger)`：`NOTIFY_SOCKET` 缺失时静默 no-op；存在时发 `daemon.SdNotify(false, "READY=1")`；发送失败仅 Warn 日志，**不阻断启动** |
| `internal/proxyapp/master.go`（+1 行） | RunMaster 的 "proxy master serving" 日志之后、阻塞信号之前调用 `notifySystemdReady` |
| `internal/proxyapp/sdnotify_test.go`（新增） | UT 三分支：unixgram 实测投递 `READY=1` / 无 env 零输出 / 发送失败告警 |
| `deploy/node-proxy.service` | **保持 `Type=exec` 不变**（上游兼容），注释中给出 notify 四件套启用指引 |
| `docs/node-proxy.md` / `node-proxy_zh.md` | 新增 §3.1 "systemd readiness and hot-upgrade handover"（中英同步） |

语义细节（xproxy 需要知道的三点）：

1. **no-op 条件**：手动运行、容器、`Type=simple|exec` unit 下框架完全不发送，现有 Docker 测试与开源部署零影响
2. **env 保留**：`SdNotify` 以 `unsetEnv=false` 调用，`NOTIFY_SOCKET` 不清除——热升级 fork 出的继任者进程继承该 env，能在自己的 serving 点上报就绪
3. **worker 不发**：只有 master 走 RunMaster；worker reexec 不经过该路径，不会误发 READY

---

## 三、通知分工契约

| 通知 | 发送方 | 时机 | xproxy 需要做什么 |
|---|---|---|---|
| `READY=1` | **框架**（RunMaster serving 点） | master 完成 listener bind、policy 同步、worker 拉起之后 | **无需做任何事**，冷启动与热升级继任者均自动覆盖 |
| `MAINPID=<新pid>` | **xproxy**（编排层，`sdNotify` 已实现） | 继任者两阶段探针通过之后、老进程退出之前 | 现有代码在 `Type=notify` 下直接生效，**零改动** |

时序自洽性说明：
- MAINPID 运行中切换不要求新 MainPID 重发 READY（unit 的 start 阶段早已结束）
- xproxy 探针成功即保证继任者已 serving（框架 READY 已发出），必然发生在老进程退出之前
- relay 链 pid 不变（unix.Exec 同进程替换），`NOTIFY_SOCKET` 随 env 继承，同 cgroup 满足 `NotifyAccess=all`

---

## 四、xproxy 侧适配步骤

### 4.1 proxy.service 切换（四件套）

```ini
[Service]
Type=notify              # exec/simple → notify：active 判定从"进程存在"变为"serving 完成"
NotifyAccess=all         # 允许 cgroup 内任意进程发通知（继任者发 READY、老进程发 MAINPID 都需要）
KillMode=process         # stop 只信号 MainPID，worker 由 master 优雅退出带走
StartLimitIntervalSec=0  # 禁用 start 限流：conductor 故障期的 start 超时重试不会撞 10s/5 次限流躺平
ExecStart=/usr/bin/node-ctl proxy serve --config=/etc/node-ctl/proxy.yaml
Restart=always
RestartSec=1
# TimeoutStartSec 默认 90s 即可（WaitPolicy 等 conductor policy 的阻塞由它兜底）
# TimeoutStopSec 需 > drain_timeout + 退出开销（默认 90s，排水预算大的节点同步调大）
```

### 4.2 升级顺序（强制约束）

```
先升级二进制（node-ctl + xproxy，含框架 sd_notify 代码）→ 再切 unit 到 Type=notify
回滚反序：先回 unit → 再回二进制
```

反序的后果：`Type=notify` unit 跑不发 READY 的旧二进制 → activation 卡到 `TimeoutStartSec` 判启动失败。
正序则全程安全：新二进制 + 旧 unit（exec）下通知是 no-op，可独立灰度。

### 4.3 代码改动（cmd/hotupgrade.go）

移除空壳阻塞，改为 MAINPID 通知后正常退出：

```go
// 之前（cmd/hotupgrade.go:177-178）：
stopHotUpgrade()          // 停止 SIGUSR2 监听，防老空壳二次响应
select {}                 // 永久阻塞，保持 MainPID 存活 → unit active

// 之后：
stopHotUpgrade()          // 保留：仍应停止二次响应
sdNotify("MAINPID="+strconv.Itoa(newPID))   // 现有调用，notify unit 下生效
// 老进程正常退出：RunMaster 返回 → main 返回 / os.Exit(0)
// MainPID 已转移，退出不触发 Deactivate，cgroup 不被清理
```

收益（对应分析文档 4.2 的五个代价全部消除）：
1. 无空壳进程泄漏
2. MainPID 始终指向活着的 master——**nacre-agent 的 `systemctl MainPID → SIGUSR2` 信号路径恢复正常，连续升级链不再断裂**（此前 gen 2→3 只能靠给新 pid 手动发信号）
3. `systemctl status` 显示真实服务进程
4. systemd 对 MainPID 的探活/重启语义恢复正常

### 4.4 建议的内网验证清单

- [ ] 冷启动：`systemctl start proxy` → `systemctl is-active` 在 serving 后才变 active（进程起来到 active 之间有 WaitPolicy 耗时）
- [ ] SIGUSR2 热升级：老进程退出后 unit 仍 active，`systemctl status` MainPID = 新 master pid
- [ ] 连续升级 gen 1→2→3：第二次 SIGUSR2 仍发给 systemctl MainPID（新 master），升级链不断
- [ ] `systemctl stop`：只 master 收 SIGTERM，worker 经 drain 排空后自退（长连接在 drain 窗口内存活）
- [ ] conductor 停止状态下 `systemctl start proxy`：卡 TimeoutStartSec 后失败重试，conductor 恢复后自愈；确认未触发 start-limit
- [ ] master SIGKILL（模拟崩溃）：worker 靠 drain-pipe EOF 自退，无持久孤儿；systemd 按 Restart=always 冷重启

---

## 五、切换后的行为变化与运维面

| 维度 | Type=exec（现状） | Type=notify 四件套（切换后） |
|---|---|---|
| active 判定 | exec 成功即 active（**存在虚假 ready 窗口**：WaitPolicy 阻塞期 worker 未起但 unit 显示 active） | 框架 serving 完成才 active |
| conductor 故障期 | 无限卡但假 active | 诚实 start 超时 → `Restart=always` 重试循环 → conductor 恢复后自愈（`StartLimitIntervalSec=0` 防躺平） |
| `systemctl stop` | cgroup 广播 SIGTERM：**worker 同时收信号，ctx 立即取消，inflight 连接直接断，完全不排水** | 只信号 master → 两阶段优雅退出 → worker 经 drain 管道排空后自退 |
| stop 超时 | 同左 | drain 总时长必须 < `TimeoutStopSec`，否则 systemd 补 SIGKILL |
| MAINPID 通知 | 无效（无 NOTIFY_SOCKET） | 生效，热升级交接闭环 |
| 监控告警 | — | conductor 故障期 proxy 会出现 start-fail 记录，告警规则需将 proxy start-fail 根因关联到 conductor，避免误报 |

补充说明（KillMode=process 的兜底）：master 被 SIGKILL 时 worker 不会被 systemd 清理，但框架已有 drain-pipe EOF 机制（master 死 → 管道写端关闭 → worker 自退），无持久孤儿。

---

## 六、对 §三改进点的框架侧裁决

| 项 | 裁决 | 理由 |
|---|---|---|
| 3.1 老进程不退出 | **已实现**（方案 A，见 §二） | — |
| 3.2 HotUpgrade 状态查询 | **不改框架** | `MasterControl()==nil` 时 xproxy 侧自行打日志即可解决排查问题；为区分 nil 原因改 API 签名（破坏性）或加可选接口，收益不成比例 |
| 3.3 ForkSuccessor 框架化 | **本需求不做** | node-ctl 中转已验证可靠；框架化收益仅省一次 exec（毫秒级），代价是框架内化 fork 编排 + M4 身份校验，与"框架提供能力、xproxy 编排"分工相悖；且"node-ctl 版本 ≥ xproxy"约束依然存在 |
| 3.4 SuccessorReady 信号 | **不建议做** | Bookmark 完成 ≠ worker 已 accept，框架内部信号**替代不了** Phase 2 的 data dial；即使暴露信号，两阶段探针仍需保留，收益仅剩省掉 Phase 1 的 HTTP 轮询 |
| 3.5 环境标记常量化 | **可选、低优先** | 三个 env 名（`KUASAR_PROXY_HOT_UPGRADE` / `_DATA_FD` / `_MMDS_FD`）两仓各一份。若做：框架导出到 public API（app/proxy 包），xproxy 引用；若不做：以契约文档为准，两边字符串保持一致（框架侧有 UT 锁定拼写） |

---

## 七、框架侧验证情况

- UT：`TestNotifySystemdReadySendsReadyState`（unixgram 实测投递 `READY=1`）/ `WithoutSocketEnvIsNoop`（无 env 零输出）/ `FailedSendWarns`（失败告警不 panic）三分支全 PASS
- Docker 回归：proxyapp + cmd/node-ctl 全套件 RC=0——插桩二进制在无 `NOTIFY_SOCKET` 环境下行为零变化
- 依赖：`coreos/go-systemd/v22` 为 go.mod 既有 direct 依赖（launcher/builder 已在用），daemon 子包纯 Go，不影响 `CGO_ENABLED=0` 构建

---

## 八、FAQ

**Q：老进程退出会不会带走新 master？**
不会。MAINPID 通知后 systemd 的 tracked pid 已指向新 master，老 MainPID 退出不触发 Deactivate，也不会清理 cgroup。

**Q：继任者需要自己发 READY 吗？**
不需要。框架在继任者的 RunMaster serving 点自动发（NOTIFY_SOCKET 已随 fork 链继承）。xproxy 的 `sdNotify MAINPID` 只管交接。

**Q：热升级失败回滚（RollbackPhaseOne）时通知怎么算？**
回滚意味着老 master 继续服务，不退出、不发 MAINPID——通知状态不变，unit 语义不受影响。READY 只在 serving 点发过一次，回滚不重发（老 master 本来就已 ready）。

**Q：不切 unit 会怎样？**
框架新代码是 no-op，一切维持现状（含 `select{}` 空壳的五个代价）。unit 切换是 xproxy 侧的独立决策，可以按节点灰度。
