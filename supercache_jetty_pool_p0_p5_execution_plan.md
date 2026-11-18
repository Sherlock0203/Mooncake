# Jetty 池化与 Physical RM P0-P5 执行计划

日期：2026-11-10。该计划使用阶段门禁和里程碑，不虚构人日或日历日期。每个目标节点由对应 Owner 在看板登记；未满足准入条件不得跨阶段合入生产路径。

## 阶段总览

| 里程碑 | 范围 | 交付物 | 准入 | 准出/测试门 | 当前状态 |
|---|---|---|---|---|---|
| M0 | 前置 physical RM 代码 | DeviceKind、identity、profile、bootstrap v2 validator/generation helper、adapter gate、UT | 当前 `e1d39c4d` | 作者报告 UT 通过；需归档命令/环境；生产 RM 仍关闭 | 编码完成，证据归档待补 |
| M1/P1 | RM legacy | RM JFS/JFR/import/post、v2 事务/target owner、flush/delete/shutdown | M0；B-01/B-03/B-04 有版本结论 | CTest、interposer、双端 READ/WRITE、partial/early、30 分钟 soak、无泄漏/hang | 未开始 |
| M2/P2 | registry 与业务池 | LocalPortTarget、ImportedTargetRegistry、静态预算、SendJettyPool/LIFO、单 attempt 单 WR、完成门 | M1 全部签署；B-02/B-05/B-06 关闭 | 并发 lease/credit、空池/满池、early CQE、回滚、真实 RM 数据正确 | 未开始 |
| M3/P3 | 异步回收 | ERROR/drain、marker/software flush、DeleteQueue 三态、JFC lease、Refill、tombstone/ABA | M2；B-03/B-04 异步契约 | marker/WR_UNHANDLED/delete 三态/队列满/quarantine/迟到 CQE，无提前释放/盲重试 | 未开始 |
| M4/P4 | 探测恢复 | ProbeResource、campaign、PathHealth、backoff、逐路径恢复/timer cancel | M3；B-08、B-05 关闭 | probe timeout/stale/资源故障、业务错误、恢复与新故障交错、无业务恢复、实机故障恢复 | 未开始 |
| M5/P5 | Runtime/Store/性能 | buffer/staging owner、SDK/Store/freeBatch/shutdown、benchmark/soak | M4；B-07/B-09 关闭 | 取消/超时/析构、4/6/非对称拓扑、30 分钟 pooled soak、SLO 达标 | 未开始 |
| M6 | 灰度上线 | 开关、指标、告警、灰度批次、回滚手册和证据包 | M5 无 NOT_RUN、版本矩阵冻结 | 小流量→扩大；任意 trigger 可关闭 admission 并回滚，不原地切 native mode | 未开始 |

## 可执行子任务

### P0：规范、证据和现有计数

1. 固定 Mooncake/UMDK/驱动/固件/ABI 快照，补齐 B-01、B-03、B-04 的 provider 问题单。
2. 归档作者自测 UT：命令、编译器、依赖、SHA、测试目标、总数、日志和失败重试。
3. 在 Linux CI 执行 `tent_transport_selector_test`、`tent_ub_core_test`、`tent_ub_native_data_path_test`、`tent_ub_teardown_test`，运行 `git diff --check`、clang-format/pre-commit。
4. 保持 RC 默认、pool/probe 关闭、RM gate fail-closed；验证负配置、身份缺失/重复/陈旧、错误 mode 不创建 native 资源。

准出：四个目标构建并通过，证据归档完整；否则 P1 保持阻塞。

### P1：RM legacy

1. 用真实 provider 配置 RM JFS/JFR、priority/tp_type、target import 和每 WR completion mapping，移除 RM 路径的 RC bind 依赖。
2. 接入真实 bootstrap producer/consumer、trusted peer metadata、generation compare-and-commit、LocalPortTarget 最小 owner。
3. 完成双端 CPU DRAM READ/WRITE、1B/4KiB/64KiB±1/1MiB、partial post、early CQE、completed bytes、flush、delete、shutdown。
4. 执行 30 分钟 RM legacy soak，记录 native handle、内存、错误、重启和回基线。

准出：真实 provider 全部强制用例 PASS；任何 provider/硬件未知项维持阻塞。

### P2：共享 Registry 与业务池

1. 新增 `LocalPortTarget`、`ImportedTargetRegistry`、`PathRegistry` 和静态容量账本。
2. 新增 `SendJettyPool`、LIFO lease、JFC credit、单 attempt 单 WR 和 `PostGate`；pool miss 只 defer，不增 retry/fault。
3. 接入健康/负载先选路、target/profile/epoch 校验、资源逆序回滚和一次性完成门。

准出：fake 交错、interposer 调用计数和真实 RM 数据正确；满负载不错误扩池。

### P3：异步 drain/delete/refill

1. 将错误处理拆为 `FAULTED→DRAINING→DELETE_READY→DELETING`，poll 热路径不等待 marker。
2. 新增 DeleteQueue、三态结果、JFC 排他 lease、quarantine、tombstone 和 ABA 防护。
3. 新增 RefillController，按 `DesiredServing-IDLE-IN_USE-Creating` 补池，受 native/control 预算与退避限制。

准出：故障注入覆盖 marker 丢失/重复、WR_UNHANDLED、delete retained/consumed/unknown、队列满、创建失败和迟到 CQE。

### P4：ProbeResource 与路径恢复

1. 每端口建立独立 ProbeResource 和受限目标槽位；实现 peer campaign 去重、backoff、fault_epoch。
2. 实现一字节 WRITE、timeout/stale/Unknown、成功阈值和恢复后 timer 取消；单 probe 故障不升级整个 Context。
3. 执行业务错误、无业务恢复、恢复与新故障交错及实机端口故障/恢复。

准出：准确作用域内隔离和恢复；旧代成功不能解封新故障；探测资源不挤占业务安全预算。

### P5：Runtime/Store、压测和灰度

1. 将 TENT/Store/staging 的实际 buffer owner、pin/unpin、CROSS/H2D/POST 和 shutdown 接入 `TransferLifetimeOwner`。
2. 联测取消、超时、Unknown WRITE、析构、freeBatch、staging 复用和进程退出。
3. 冻结 benchmark protocol：相同语义、数据大小、并发、native 预算和拓扑，对照 RC/bonding；采集吞吐、p50/p99、CPU、错误率、池缺口、quarantine 和资源水位。
4. 运行 4/6 口、非对称拓扑、30 分钟 legacy/pooled soak；确认 SLO 和 rollback trigger。

准出：所有强制物理用例 PASS，不能有 NOT_RUN；SRE/架构/Provider/Runtime Owner 签署。

## 灰度与回滚

- pooled admission、probe 和 force-disable 必须是启动前或受控开关；不做 RC↔RM、v1↔v2、pooled↔legacy 原地切换。
- 灰度按小流量、单拓扑、单版本批次推进，持续观察错误率、Unknown/quarantine、池缺口、p99、CPU 和 native 预算。
- 触发阈值、provider Unknown、DMA 安全不明、quarantine/队列达限或数据校验失败时，停止新 pooled attempt，保留责任 owner，按回滚手册回到已验收路径；不得强制删除仍有 DMA 责任的资源。

## 追踪关系

- 偏差事实：`supercache_jetty_pool_deviation_correction_register.md`
- Owner 决策：`supercache_jetty_pool_blocking_decision_register.md`
- 详细 P0-P5 用例和当前执行清单：`tent_rm_p0_p5_execution_checklist.md`
- 统一设计与状态边界：`supercache_jetty_pool_current_and_target_rm_concise.md`
