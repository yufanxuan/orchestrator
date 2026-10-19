# proxy 热升级方案 V3.7 评审补充（实施前小修）

> **评审对象**: `proxy热升级方案v3.md` V3.7
> **结论**: V3.7 对 P1/P2/M1-M4/L1-L4 全部正确落实，**方案批准实施**。本文件为实施前的 3 个小修（1 个中危缺口 + 2 个文档瑕疵），建议随 V3.7.1 或实现 PR 说明带上，不阻塞开工。

---

## 1.（中危）回滚路径缺失凭证表恢复

**问题**：V3.7 前段（GracefulShutdown）包含"导出凭证 → `credshm.Close()` 释放 Flock"，发生在 **fork 之前**（§4.5.2 步骤 1-2）。但 §4.4 回滚清单只写了"取消 DrainWorkers、恢复 worker 监管、重启 subscriber、重开 metrics"——**没有凭证表**。

**后果**：回滚后旧 master 的 `Writer` 已关闭——`/nacre/credentials` push 持续 503、新沙箱凭证不可写，直到下次升级成功或冷重启。

**修复**：回滚步骤补一项（xproxy 仓，约 30-50 行）：旧 master **从自己仍持有的 memfd 快照**重新 `credshm.Create()` + 逐条 `Upsert` 恢复 + generation 继承——与正向迁移同构（§4.5.2 的逆过程）。memfd 在旧进程内的引用未关闭，数据仍在。invokeshm 同构。

## 2.（低）§4.2 时序图步骤 8 残留旧顺序

"老 master 释放 credshm Flock → sd_notify MAINPID → 退出"——Flock 已在前段 fork 前 Close（§4.5.2），此步只剩 `sd_notify MAINPID` + 退出。删掉"释放 Flock"，避免实现者按图在 drain 结束后再 Close 一次（对已关闭的 Writer 调 Close）。

## 3.（备注）前段挂监管后至 ready 前的 30s 窗口

前段"挂起 worker 监管"后、新 master ready 前（≤30s）：老 worker 若崩溃**无 replacement**（监管已挂）。概率低且新 master 即将接管，可接受——建议在 §8 风险表注一句，避免联调时当 bug 报。

---

## 修订位置速查

| V3.7 章节 | 修订 |
|-----------|------|
| §4.4 回滚表 | fork 失败/ready 超时行补"凭证表恢复（memfd 快照 → Create + Upsert + gen 继承）" |
| §4.2 时序图 | 步骤 8 删"释放 Flock" |
| §8 风险表 | 补"前段挂监管至 ready 前（≤30s）worker 崩溃无 replacement，可接受" |
