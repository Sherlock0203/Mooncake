# TENT UB RM 池化源设计结论汇编

| 项目 | 内容 |
| --- | --- |
| 状态 | 历史设计结论文档（只保留结论与差异表，原始长文不入库） |
| 日期 | 2026-11-18 |
| 来源 | 原仓库根目录 6 份 `supercache_jetty_pool_*` 与 `jetty_pool_test_report.md`、`tent_rm_p0_p5_execution_checklist.md` |
| 后续 | 主干落地计划见 `docs/analyze/tent-ub-rm-pool-trunk-consolidation-design.md` |

本文是拆分前的 V4 目标设计与两份登记表的结论摘要。它回答三件事：目标形态是什么、
当时的源码离目标差在哪、哪些结论已被主干实现验证或推翻。**实现以主干代码和
`tent-ub-rm-pool-trunk-consolidation-design.md` 为准**；本文只保留不随代码漂移的
结论与证据口径。

---

## 1. 目标形态（V4 结论段）

### 1.1 核心目标

1. 业务发送 Jetty 的创建、租用、完成归还、故障排空、异步删除和补池与 endpoint
   生命周期解耦。
2. 节点按配置端口发布共享 RM target；本地按 Context 导入，endpoint、业务和探测只持
   引用。
3. 每个 slice attempt 独立选路、独占一个业务发送 Jetty、最多一次 native post，且
   恰好一条 WR。
4. 复用专用 post/poll 线程框架，增加独立 control、delete、refill 职责；应用与
   adapter 不直接 native post/poll。
5. 业务错误触发受限 peer 核验，通过专用探测 Jetty 与一字节 WRITE 恢复路径；恢复后
   停止该路径周期任务。
6. 用完整身份、代次、完成门、异步屏障、tombstone 和实际 buffer owner 防止早到 CQE、
   迟到事件、native ID ABA、未知 WRITE 与提前释放。
7. 先完成真实 RM legacy 基线，再启用池化；不以当前 RC 实现、自动 bonding 或静默
   mode 切换作为 RM fallback。

### 1.2 强制约束（结论）

| 项目 | 结论 |
| --- | --- |
| 选路 | 每 attempt 独立选本地 Context/EID 与远端按口 target，不按 Group/Task 汇聚 Jetty |
| 发送池 | 每 Context/兼容 LocalSendProfile 一池；先选路再租一个空闲发送 Jetty |
| WR | 池化路径每 attempt 最多一次 native post，输入恰好一条 WR；重试是新 attempt |
| 独占 | 一个业务发送 Jetty 同时只服务一个 attempt；归还后可顺序服务其他兼容 peer |
| 空闲顺序 | 同池 LIFO，刚归还成员优先；不改变路径选择顺序 |
| 远端目标 | 每配置端口一个共享 target，业务与探测共用；endpoint 只引用 |
| 导入归属 | imported target 由本地 Context 的 Registry 持有，不归 probe Jetty 或 endpoint 独占 |
| 探测资源 | 每本地端口一个专用 RM 发送 Jetty，不进入业务发送池 |
| 拓扑 | 路径由端口清单和合法邻接计算；每口两邻居时 4 口 8 路、6 口 12 路 |
| 探测启动 | 初始化后 ELIGIBLE_UNVERIFIED 路径可承载业务；业务错误立即隔离出错路径并对该 peer 配置路径集合发起受限核验 |
| 探测停止 | 当前有效成功证据达到阈值且其余门禁满足后恢复，并停止该路径周期任务 |
| 错误 Jetty | 首次真实错误 CQE 对应的发送 Jetty 强制退役，不因后续探测成功回到业务池 |
| 排空 | 关闭 post 门禁、modify ERROR、持续正常 poll、识别 flush-done，再软件 flush 并结算剩余 WR |
| 删除 | 独立删除线程消费 DELETE_READY；不在 poll 热路径销毁，也不假定 delete 失败可盲目重试 |
| 补池 | 独立补池线程恢复静态 Serving；IN_USE 计入 Serving，不按 idle 缺口无限扩池 |
| 请求限额 | K 约束单个网络 UbTask 跨 Context 的准入 attempt，不是父 staging 请求窗口 |
| 可靠性 | timeout/cancel 不等于 DMA 结束；保留实际 buffer owner、quarantine、事件归属与 Unknown 结果 |
| 性能 | 不预设 RM 池化优于 RC/bonding；需在相同语义与 native 预算下测量 |

### 1.3 明确不引入

- 不引入 Group、window、producer Seal、RC 池化、原生业务 SEND/RECV/atomic。
- 不提供跨 Jetty 全局写顺序或请求原子可见性。
- 不把 `UbTask::seal` 当作 post 退出或 DMA 屏障。
- 池化路径不保留逐 endpoint 固定 Jetty 数组、两端 Jetty 数量相等校验或 RC bind。
- 不在 poll 热路径同步等待 flush-done，不以同步 quiesce 作为缺少异步能力时的池化
  fallback。
- 不通过名称猜测 PHYSICAL；不把 pool 空、probe Pending、任意 CQE 或 ACK timeout
  汇总成物理节点全坏。
- 不做运行中 RC↔RM、v1↔v2 或 pooled↔legacy 的原地切换。

### 1.4 单一口径的冲突裁决

| 主题 | 唯一口径 |
| --- | --- |
| target 与 probe import | target 是按口共享资源；import 按本地 Context+target 去重。`target_map` 描述邻接，不表示 probe Jetty 独占 import |
| 探测启动范围 | 一个业务错误只立即隔离实际出错路径，同时创建/合并该 peer 的 campaign 并快照全部合法路径；未出错路径不批量置 FAULT |
| 恢复与周期任务 | 当前代次连续成功达到阈值并满足全部门禁后恢复；默认阈值 1；恢复后停止本路径 timer |
| 正常归还 | 不是"收到成功 CQE 立即归还"；必须满足 post 返回、Accepted、真实成功 CQE、完成处理退出、无故障和一次性归还门 |
| post 错误 | 确定零接受且成员无故障可逆序回滚并安全归还；接受范围 Unknown 必须排空；真实错误 CQE 强制退役 |
| flush-done | 只是必要屏障之一；之后仍需 `urma_flush_jetty` 取空软件 SQ，并等待 native/handler/inflight/owner 收敛 |
| 同步 quiesce | 池化错误热路径禁止；缺少可验证异步事件能力时 pool gate 失败或 quarantine |
| delete 失败 | 使用 HANDLE_RETAINED/HANDLE_CONSUMED/UNKNOWN 三态；无证据时按 UNKNOWN 隔离，不统一退避重试 |
| reset/delete | reset 只在具体 provider 契约要求时执行，不能替代完整 flush 与归属核验 |
| 补池 | `Deficit = DesiredServing - IDLE - IN_USE - Creating`；不是 idle 低于 min 就创建 |
| probe timeout/stale | timeout 是责任 Unknown，保留 pin 并排空；stale 证据丢弃；未发出或探测资源错误记 Pending/Unknown |
| 故障升级 | 只有准确共享域证据才能形成 target/JFC/Context BAD、节点阻断或 Context 重建；归属不明可保守 quarantine 并停新 post，但不能伪造健康事实 |
| RM legacy 与 pool | legacy 可保留 vector/partial-post 作为对照；pooled 内部单 WR；二者不得混用完成与重试语义 |
| 容量配置 | pooled 使用静态 desired Serving 与全角色 native 预算；首版不启用 initial/min/max 自适应扩缩 |
| 共享 target 删除 | 必须撤销发布、暂停所有引用路径并取得 provider 定义的入站屏障；peer 停发 ACK 或超时不等于入站 DMA fence |
| native ID 与代次 | 软件 generation 不能自动消除无代次原生事件的 ABA；需要 tombstone、事件屏障与延迟发布 |
| 结果与 buffer | LogicalState、ExecutionOutcome、ResourceSafety 分层；Unknown WRITE 不透明重放，实际 buffer/staging pin 等待真实 DMA 安全 |

### 1.5 资源归属

| 对象（目标设计名） | 归属与责任 |
| --- | --- |
| PortBinding | 配置 port_id 到 device、EID/index、Context 的准确映射 |
| UbContextResource | Context、JFC/JFR profile、业务池和设备预算 |
| SendJettyPool | IDLE 栈、租用状态、LocalSendProfile 与容量账本；不持锁调用 provider |
| RmSendJetty | 唯一 native owner、generation、PostGate、JFC credit 与退役状态 |
| SliceJettyLease | 一个业务 attempt 对一个发送成员的独占使用权 |
| LocalPortTarget | 每端口一个本地共享 target，发布给多个 peer；拥有 target 原生依赖 |
| PeerTargetDescriptor | peer 按口发布的 target、EID/UASID/ID、RemoteTargetProfile 与 target_epoch |
| ImportedTargetRegistry | 按本地 Context 导入远端 target；多个 endpoint、业务与探测可引用 |
| UbEndpoint | 路径视图、目标与注册引用，不拥有或不直接销毁共享 target |
| ProbeResource | 每本地端口一个专用发送 Jetty、探测源内存、远端探测槽位引用与独立额度 |
| PathRegistry / PathHealth | 合法路径及各自业务资格、fault_epoch、探测结果与负载 |
| PeerProbeCampaign | peer 的一次核验轮次、待核验路径与周期任务去重 |
| Inflight / TransferLifetimeOwner | token、post/WR 责任、lease、额度、Task/batch 与实际 buffer pin |
| RecoveryRegistry | 排空、DELETE_READY、quarantine、tombstone 与删除任务 |
| DeleteQueue / RefillController | 唯一删除领取、删除结果登记、静态容量缺口与全角色预算 |

### 1.6 身份与代次

```text
ContextKey = device_identity + EID + context_incarnation
peer_incarnation = 对端 node_incarnation
LocalSendProfile = trans_mode(URMA_TM_RM) + priority + bind_tp_type_mask + local_capability_set
RemoteTargetProfile = trans_mode(URMA_TM_RM) + target_type + tp_type
LocalTargetKey = node_incarnation + port_id + target_epoch + RemoteTargetProfile
ImportedTargetKey = local ContextKey + peer LocalTargetKey + native EID/UASID/ID
PoolKey = ContextKey + LocalSendProfile
PathKey = PoolKey + peer_incarnation + remote_port_id + RemoteTargetProfile
PathVersion = PathKey + topology_epoch + target_epoch
LaneKey = ContextKey + native Jetty ID + lane_generation
AttemptKey = task_id + slice_id + attempt_generation
ProbeKey = PathVersion + fault_epoch + campaign_id + probe_sequence
```

`LocalSendProfile` 与 `RemoteTargetProfile` 是 TENT/adapter 的逻辑抽象，不是 UMDK
原生类型。CompletionToken 进程内唯一，只映射到业务 attempt 或 probe，不复用地址。
`eid_index` 不是端口号，native ID 不是全局身份。UASID 必须在线协议中显式存在。

### 1.7 端口/邻接与 target 拓扑

- 配置声明端口清单、每端口的 device/EID/index/Context 与 `adjacency[peer][port_i]`；
  启动时校验"非空、无重复、peer 与端口存在"，非法拓扑 fail-closed。
- 4 口每口两邻居 → 8 条路径；6 口 → 12 条。示例数值是目标/占位，不是当前运行配置。
- 每端口一个共享 RM target；`target_map` 只用于探测邻接选择，不创造额外 import。

---

## 2. 文档与源码差异（拆分前的登记结论）

下表合并原《偏差修正登记》D-01~D-10 与原《接口设计分析》X-01~X-11。**"现状"列描述
拆分时点**；标 ✅ 的条目已在主干实现中解决或部分解决。

| ID | 文档口径 | 源码事实（拆分时点） | 状态 |
| --- | --- | --- | --- |
| D-01 | 文档基线为旧分支/旧提交 | 分支 `tent/tent_rm`，HEAD `e1d39c4d` | 文档已改；后续以主干提交为准 |
| D-02 | 只写"自测通过" | 无可重放命令/构建参数/日志 | 证据口径保留：自测≠CI≠实机 |
| D-03 | 易把"v2 已实现"读成生产握手已切 v2 | `bootstrap_types.h`/`bootstrap.cpp` 有 v2 DTO/validator；`onBootstrap` 仍走 RC gate；endpoint 仍输出 v1 | 部分 ✅：主干已有 v2 契约与显式协议选择器；生产 v2 事务未接通 |
| D-04 | physical-only 被理解为硬件证明 | `device_selection.h` 从 UNKNOWN 起，按显式 identity 匹配 | 口径已改为"受信配置准入"；主干保持 |
| D-05 | 同步 quiesce 与目标异步排空混写 | `quiesceJetty` 是同步 fence | 主干已分 legacy 同步与 pooled 异步状态机 |
| D-06 | 把 endpoint 固定 Jetty 写成共享池能力 | 拆分时 `endpoint.cpp:prepare/bind` 固定 Jetty 且要求两端等长 | ✅ 主干已有 `SendJettyPool`，endpoint 不再拥有固定池 Jetty |
| D-07 | UMDK master 观察被当作 provider 保证 | 无可核验 UMDK SHA/ABI/驱动/固件 | 保留：验收时记录实际版本 |
| D-08 | 4/6 口、desired serving 被读成当前配置 | `UbParams` 默认 RC，pool/probe off | 保留：本版静态 `desired_serving` |
| D-09 | BONDP 编译依赖被写成已解除 | real 分支仍无条件包含 `urma_ubagg.h` | 保留：物理构建准出要求 |
| D-10 | 构建 fallback 跟踪浮动 UMDK master | `FindUrma.cmake` 用 `GIT_TAG master` | 保留：验收需冻结 SHA |
| X-01 | "批量 admission 少记 inflight"缺陷要求修复 | 全仓无 `tryAcquireOutstandingBatch`；现存单 WR `post(jetty,{work},...)` | 已过时，不应再作为门禁项 |
| X-02 | SDK 保留 `submitRead/submitWrite/drain/health/close` | 安装头只有 BatchID 模型；`UbTransport` 未 override `quiesce()` | 目标项属新增，不是保留 |
| X-03 | `ub_core/ub_transfer/ub_health/UbEngine` 组件 | 仓库只有 `include/tent/transport/ub/` 与 `src/transport/ub/` | 组件名不可直接作为目录落地 |
| X-04 | 7 个 adapter 接口、9 个内部接口、14 个组件 | 拆分时全部 0 命中 | 目标为建议新增 |
| X-05 | 配置面含 topology/send_pool/probe.local_ports 等 | 拆分时只解析 22 个键 | ✅ 部分：主干已扩展配置面；仍有命名与缺失项，见总设计附录 A |
| X-06 | v2 schema 含 schema_version/descriptor/access_capabilities | 主干的 v2 字段是 device_kind/physical_device_identity/incarnation/targets/probe_target_map | 主干契约以代码为准 |
| X-07 | 每 Context/LocalSendProfile 建池 + LIFO | 拆分时 `EndpointStore` 按 endpoint 缓存固定 Jetty，无池 | ✅ 主干已实现池与 LIFO |
| X-08 | 预热/探测资源完成前不得启动 post workers | `install` 构造完即 `workers->start()` | 保留：初始化顺序仍待改造 |
| X-09 | 池组件未实现 | `quiesceJetty` 为同步忙等；`RealJetty::initialize` 为 `URMA_TM_RC` | 池组件 ✅；RC 默认仍在 |
| X-10 | 各文档之间口径不一致 | 清单已修 X-01，主文档/执行计划未同步 | 以本文为准 |
| X-11 | DeviceKind 可来自"可验证 provider 元数据" | `VALIDATED_METADATA` 无赋值点，唯一生效来源是 `EXPLICIT_CONFIG` | 保留："受信配置准入"是唯一现状 |

---

## 3. 外部决策登记（B-01~B-10，结论保留）

这些不是代码阅读能关闭的项，必须由具名 Owner 提供版本化证据；仅当字段齐全
（负责人/角色、决定、适用 Mooncake/UMDK/驱动/固件版本或部署范围、日期、证据链接）
状态才可改为"已确认"。

| ID | 决策主题 | 推荐默认 |
| --- | --- | --- |
| B-01 | UMDK RM/JFS priority→RTP、JFR/JFC、RM WR 组合 | 只启用实测通过的设备/priority 组合，建立 provider 版本矩阵 |
| B-02 | 多发送 Jetty/多 peer/业务+probe 共享 target | 共享 target，以 provider 并发/撤销契约与压力结果为准 |
| B-03 | modify ERROR、flush-done、`urma_flush_jetty`、WR_UNHANDLED、AE ACK | pooled 使用异步 ERROR/marker/poll/flush/delete 屏障 |
| B-04 | delete 失败 handle 状态、JFC poll/delete/clean 并发 | 无法判定统一 UNKNOWN/quarantine；不盲重试 |
| B-05 | port inventory、port_id、邻接、非对称拓扑 | 先显式静态 inventory + checksum + epoch |
| B-06 | 池/probe/target/JFC/MR 替换预算 | 按设备 capability + 端口邻接计算并留控制/替换余量 |
| B-07 | TENT/Store/staging buffer pin/unpin 与 shutdown owner | 统一 owner token 与释放门 |
| B-08 | 一字节 WRITE 探测槽位、权限、并发、timeout Unknown | 每 LocalTarget 独立受限槽 + ProbeResource |
| B-09 | 性能负载、对照、SLO、灰度回滚阈值 | 冻结吞吐 + p99 + CPU + 错误率组合阈值 |
| B-10 | 自测 UT 缺可重放证据 | 归档自测并在正式 CI 重跑 |

**结论**：截至拆分时点，B-01~B-10 均未取得具名 Owner 回复或可核验外部证据；前置
代码、文档事实修正和不依赖这些决策的 fake/单元测试可以继续，真实 RM 数据面、灰度
上线必须按对应项阻塞。

---

## 4. 实施路线（已迁移）

原 P0~P5 执行计划与本文件 §3 的里程碑已被更细的 9 提交拆分取代；逐提交的文件、
接口、状态机、测试与回滚点见
`docs/analyze/tent-ub-rm-pool-trunk-consolidation-design.md` §3.2。

原根目录文件的去向：

| 原文件 | 处置 |
| --- | --- |
| `supercache_jetty_pool_current_and_target_rm_concise.md` | 结论并入本文 §1 |
| `supercache_jetty_pool_interface_design_analysis.md` | 结论并入本文 §2（X-01~X-11） |
| `supercache_jetty_pool_rm_legacy_detailed_design.md` | 结论并入本文 §1、§2（D-05/D-06） |
| `supercache_jetty_pool_p0_p5_execution_plan.md` | 里程碑并入本文 §4，指向 9 提交表 |
| `supercache_jetty_pool_blocking_decision_register.md` | 结论并入本文 §3 |
| `supercache_jetty_pool_deviation_correction_register.md` | 结论并入本文 §2 |
| `jetty_pool_test_report.md` | 过程产物，删除；有效结论并入主干设计 §10.4 |
| `tent_rm_p0_p5_execution_checklist.md` | 过时，删除；未完成项并入主干设计 §3.2 |
