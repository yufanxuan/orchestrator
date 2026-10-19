# 框架侧对内网验证结果的确认与回应

> 回应《xproxy 热升级进度汇报》。结论：**方案闭环确认，三个确认项逐条答复如下**；
> 另附框架侧针对 RunGC-after-Close bug 模式的同类隐患自查结果（无同类问题）。

---

## 一、验证结果确认

端到端矩阵（老进程正常退出、MAINPID 转移、连续升级 ×3、分代平铺、notify 冷启动）全部通过，
方案 A 的分工（**框架 READY=1 + xproxy MAINPID 转移**）在内网真实环境闭环，框架侧确认。
两个新修复的认定：

- **RunGC 泄漏 fault**：真实 bug，定性正确——`context.Background()` 的 GC goroutine 在表
  Close（munmap）后继续 tick，访问已解映射内存。你们判断"回滚场景同样触发"正确：
  回滚路径老 master 继续服务，PrepareHotUpgrade 已 Close 的表同样会被 GC goroutine 踩。
  修复方向（data nil 守护 + goroutine 自终止）正确
- **分代路径嵌套**：真实 bug，`stripGenSuffix` 先剥后缀回 base 再叠加的修法正确，
  与 frozen config 带旧代路径的根因分析一致

## 二、请确认项逐条答复

### Q1：框架侧 `20c0ced` 是否已合入主干 / 发版计划？

**提交状态**：`20c0ced` 已在本仓（orchestrator fork）main 分支顶端，提交链：
`051c0d7`（fix: shutdown 竞态）→ `248e428`（test: 覆盖率补齐）→ `20c0ced`（feat: sd_notify）。
推送远端与上游合流的节奏由仓 owner 安排；对上游 PR 的评估结论不变（no-op 增量、
零新依赖、API 面零变化，合流阻力最小的一类改动）。

**对 xproxy 发版的实际约束**：本仓无独立发版流程，联调按二进制交付。唯一硬约束是
升级顺序的第一环——**切 `Type=notify` 之前，节点上的 node-ctl 二进制必须已包含 `20c0ced`**。
xproxy 侧发版说明建议直接绑定该前提（"node-ctl ≥ 含 sd_notify 的构建"）。

### Q2：`Type=notify` 四件套是否纳入框架部署模板？

**维持保守路线，模板保持 `Type=exec`**（`20c0ced` 已含四件套的注释指引）。理由：

- 改默认值会同时改变所有下游部署的 start 判定与 stop 行为，上游接受度低；
  注释指引 + 文档（`docs/node-proxy.md` §3.1）已把启用方式、升级顺序、参数约束写全
- 内网 proxy.service 是自治 unit，切四件套不依赖框架模板的默认值
- 若后续上游合流完成、希望统一默认，模板改 notify 是一行改动，随时可切

### Q3：symlink 滞后一代是否符合预期？框架是否感知新代主动更新？

**功能上可接受（nacre-agent 走注册流拿真实路径），但不符合稳定名的理想语义，值得修；
修复位置在 xproxy 侧，框架侧不做。**

- 框架不做的理由：stats socket 路径对框架只是 config 里的一个字符串（继任者的 frozen
  config 带 `.g<N+1>`，框架原样 bind），分代与 symlink 是 xproxy 分代工具集的编排概念；
  框架去感知/更新它会引入框架对编排细节的耦合，违反"框架提供能力、xproxy 编排"的分工
- 根因猜想（供参考）：与刚修的嵌套 bug 同源——symlink 的创建/更新某处用了 frozen config
  的旧值（`.g<N>`）而非继任者实际路径（`.g<N+1>`）。修法建议：继任者在 serving 探针通过后
  以 `stripGenSuffix` 的 base + 实际 gen **原子重建**（`symlink + rename`）symlink，
  保证任意时刻稳定名都指向有效 socket，且不存在删除-重建窗口

## 三、框架侧同类隐患自查（针对 RunGC bug 模式）

收到 RunGC-after-Close SIGSEGV 报告后，框架侧对全部"后台 goroutine 访问 mmap 状态"的
位置做了排查，结论：**无同类隐患**，结构性差异如下：

| 框架侧位置 | 停止机制 | 为何安全 |
|---|---|---|
| `masterStats.RunGC` / `workerStats.RunGC` | 挂 `masterCtx`/`workerCtx`，master 侧计入 `masterGroup` | RunMaster 的 defer 顺序保证 `masterGroup.Wait()`（等全部任务退出）先于 `table.Close()`（munmap）执行 |
| routesync subscriber | `subscriberManager.stop()+join()`（`051c0d7`） | 无界 join，unmap 前等待解链完成——本需求中已修的同款竞态 |
| `proxyshm.Updates.readLoop` | stop pipe + `Close()` 内 `<-u.done` | Close 即等待 goroutine 完全退出 |
| `WatchRoute` 路由监视 | 返回的 cancel func 内 `<-done` | 调用方取消即等待退出，且位于 worker 进程侧 |
| `ReadWakeLoop`（per-worker） | 随 fd 关闭退出 | 旧 epoch 的 wake fd 在 runWorkerProcess 返回时关闭，阻塞的 Read 立即返回 |

xproxy 的 `shmtable.RunGC` 出问题的本质：goroutine 用 `context.Background()`（无取消源）
且无 join——框架侧同构位置全部具备"ctx 取消 + Wait/join"双保险，不存在该模式。

## 四、对收尾清单的补充意见

1. **`supervising process which is not our child` 日志**：同意，systemd 对 MAINPID 指向
   非直接子进程的标准提示（fork 型交接的正常现象），无需处理
2. **零空窗 curl 复核**：建议做——两阶段探针消除了探针语义内的空窗，curl 终验可确认
   探针间隔（130ms）外的数据面连续性
3. **补验项**：`systemctl stop` 与 conductor 故障期场景可按《proxy热升级systemd就绪通知
   适配指引.md》§4.4 清单执行；另建议补一项**升级窗口内 systemctl stop**（V3.7 已文档化：
   cgroup 全杀 = 等效冷停，nacre-agent 回退逻辑需认识该终态）
4. **活跃沙箱凭证迁移**：同意补验——entries=0 只验证了机制通路，凭证数据经 memfd 迁移的
   端到端正确性需要真实数据

---

*框架侧提交链：`051c0d7` fix / `248e428` test / `20c0ced` feat（sd_notify）；
配套文档：《xproxy热升级接口契约与实现指引.md》《proxy热升级systemd就绪通知适配指引.md》*
