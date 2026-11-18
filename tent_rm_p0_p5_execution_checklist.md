# TENT Physical UB RM：P0–P5 可执行落地清单与 RM Legacy 验收方案

> 基线：Mooncake `tent/tent_rm` / `e1d39c4d`（父提交 `0d3463c0`；在 `956af65e` 之上含 13 个上游提交，已回退 `1b4472d5..7f2d08f0` 共 6 次未验证性能提交）；UMDK 使用经 Provider Owner 选定并记录 SHA 的验证版本，不以浮动的 `master HEAD` 作为可复现基线；当前实际 ABI/驱动/固件组合待 P0-2 冻结。
> 生成日期：2026-11-08；参考答复整合：2026-11-09。
> 范围：non-bonding physical UB、URMA RM、CPU DRAM 单边 READ/WRITE。RC 仅作为独立 legacy 回归路径。
>
> 状态标记：`[x]` 表示验收已实际通过；`[~]` 表示实现或部分验证已完成，但仍有关联门禁未通过；`[ ]` 表示未开始或尚未实现。**不得仅因代码已写入就把任务改为 `[x]`。**
>
> 参考来源：`new 12.md` 对 V3 的 B1—B7、Q1—Q12 核查结论。该答复基于 Mooncake `956af65` / UMDK `09086da5` / V3，本文只采纳其问题收敛与证据边界，不采纳其过时基线；所有落地口径以 V4 和本清单为准。逐项采纳见 1.4，provider 契约用例见第 12 节，当前分支实现与剩余工作见第 13 节。

## 1. 全局执行规则

### 1.1 时间节点采用阶段门禁，不采用人日估算

- **P1 代码合入前**：P0 的规范、基线、构建与现有 WR 计数问题必须闭环。
- **P2 入口**：P1 的 RM legacy 双端真实 provider 验收必须全部通过；v2 编解码通过不等于 P1 通过。
- **P3 入口**：P2 的单 attempt、单 WR、LIFO、早到 CQE 和容量并发测试必须通过。
- **P4 入口**：P3 的 flush/delete/ABA/预算故障注入必须通过。
- **P5 入口**：P4 必须证明无业务流量时仍可探测恢复，且旧代次成功不能解封新故障。
- **生产启用前**：P5 的真实硬件、Store/staging、shutdown、soak 和回滚验收必须全部通过。

### 1.2 角色

| 角色 | 责任 |
|---|---|
| UB 架构 Owner | 统一规范、状态机、Key/epoch、阶段门禁和最终技术签字 |
| 控制面开发 | bootstrap、Registry、拓扑、代次事务、配置与元数据 |
| 数据面开发 | adapter、post/poll、pool、完成门、probe、buffer owner |
| 恢复路径开发 | drain/delete/refill/tombstone/JFC lease/quarantine |
| Runtime/Store 开发 | staging、实际 buffer、freeBatch、SDK 与关闭语义 |
| Provider/驱动 Owner | UMDK/驱动/固件兼容矩阵、native 屏障和异常返回解释 |
| 测试 Owner | fake/单元/集成/故障注入/soak、证据归档、通过判定 |
| SRE/实机 Owner | 双机与 4/6 口物理环境、版本一致性、链路与故障注入 |

### 1.3 共享资源依赖

| 资源 | 最低要求 | 用途 |
|---|---|---|
| CI 构建环境 | CMake、C++17 编译器、glog、gtest、项目依赖、clang-format 20、pre-commit | 所有软件 gate |
| Fake provider | 可控 posted_count、早到 CQE、marker、delete 失败和 native 调用计数 | 确定性竞态与回归 |
| Provider interposer | 可延迟 post return、指定 bad_wr、延迟/丢弃完成、记录 native 调用 | 真实 provider 的部分/早到/flush 验证 |
| P1 实机 | 2 台 Linux 物理机，至少 1 条 non-bonding physical UB 链路 | RM legacy |
| P2/P4 实机 | 双端 4 口；后续 6 口；支持合法非对称邻接配置 | 8/12 路径、共享 target、campaign |
| 版本证据 | Mooncake/UMDK commit、内核、驱动、固件、liburma ABI、编译参数 | 验收可复现与问题定位 |
| 预算证据 | Jetty/JFC/JFR/MR/import/token 上限及替换余量 | 防超建和 quarantine 背压 |

### 1.4 参考答复 B1—B7 / Q1—Q12 采纳总表

采纳状态：`已采纳` 表示口径已进入本清单；`部分实现` 表示代码已有但验收未闭环；`待实现` 表示仅口径确定；`待验证` 表示需 UMDK/provider 证据。

| 编号 | 事项 | 采纳方式 | 归入阶段/任务 | 关键注意事项 | 状态 |
|---|---|---|---|---|---|
| B1 | flush/JFC 并发 | 直接采纳 | P1-8、P3-1、P3-2 | marker 只是必要条件；必须 `flush_jetty` 取空软件 SQ；删除需 JFC 排他访问；`user_ctx` 无效 | 已采纳 / 待验证 |
| B2 | 共享 target 入站屏障 | 直接采纳，保留为核心阻断 | P1-6、P2-2 | 本地 unimport 不能替代入站 fence；普通引用释放不触发全 peer 撤销 | 已采纳 / 待验证 |
| B3 | 接受状态与错误归属 | 直接采纳 | P1-5、P2-5、P3-3 | 建立 Accepted/NotAccepted/Unknown 映射；覆盖早到 CQE、零接受冲突、AE 归属、ID 复用 | 已采纳 / 待实现 |
| B4 | RM legacy 基线 | 适配后采纳 | P1-5～P1-8、P0-5 | 不取消独立 RM legacy；改称“物理 RM 最小验证基线”；非池化 path 保留逐 slice 单 WR 与 adapter 级 partial-post 契约 | 已采纳 |
| B5 | physical-only 识别 | 适配后采纳，收窄 | P1-1、P1-2 | 代码已有显式身份与二次校验，禁止再描述为“名称猜测”；默认关闭，真实 provider 未验收 | 部分实现 |
| B6 | 设备/版本/权限 | 直接采纳 | P1-7、P5-4 | 缺证据记 NOT_RUN；不得推断设备或权限不存在 | 待验证 |
| B7 | 组件与目录边界 | 适配后采纳 | P2-1 前置、架构评审 | 主体架构已定，只落目录/CMake/抽离顺序；不得照搬答复中的设想路径 | 待实现 |
| Q1 | 接受状态 | 直接采纳并并入 B3 | P1-5、P2-5 | 输出“提交结果证据映射表”，不再重复讨论 Unknown 语义 | 已采纳 |
| Q2 | 删除安全条件 | 适配后采纳 | P3-2、P3-3 | `DELETE_READY` 条件可关闭设计讨论；删除失败三态和事件屏障不可关闭 | 已采纳 |
| Q3 | target 双端协调 | 直接采纳并并入 B2 | P1-6、P2-2 | 发布/epoch/旧导入留存已定；剩余为线协议、状态机与入站屏障 | 已采纳 |
| Q4 | 业务准入与恢复证据 | 适配后采纳 | P4-2、P4-3 | 状态名为 `ELIGIBLE_UNVERIFIED/HEALTHY/ISOLATED/RECOVERING`；补 `REVALIDATE` campaign | 已采纳 / 待实现 |
| Q5 | campaign 调度 | 直接采纳 | P4-2 | 保留 peer 合并、路径快照、同口单 probe、Pending、逐路径停止 | 已采纳 / 待实现 |
| Q6 | 熔断与阈值 | 适配后采纳 | P4-3、P4-4 | V4 已有 recovery/failure threshold 与 force_disable；仍须首次真实错误立即隔离退役 | 已采纳 |
| Q7 | 业务池配置 | 直接采纳设计语义 | P2-3、P3-4 | `DesiredServing-IDLE-IN_USE-Creating`；语义关闭不等于功能完成 | 已采纳 / 待实现 |
| Q8 | K/credit 范围 | 适配后采纳 | P2-4、P4-1 | 当前 K 是 legacy 取批大小，非目标准入上限；probe 不占业务 K 但占控制额度与 JFC credit | 已采纳 / 待实现 |
| Q9 | 结果与重试 | 直接采纳 | P1-7、P2-5、P5-1 | Unknown WRITE 不重放；READ 需旧 DMA 安全；WaitExpired 不释放 owner | 已采纳 |
| Q10 | 关闭与进程退出 | 直接采纳 | P1-8、P5-2 | 区分进程内 owner 保活、正常退出规则、崩溃后驱动清理；后者只能报 Unknown | 已采纳 / 待验证 |
| Q11 | 性能验收线 | 适配后采纳 | P5-3、P5-4 | 先记录同语义同预算基线；阈值不阻塞前期编码，但生产启用前必须确定 | 待定义 |
| Q12 | 默认参数 | 直接采纳为测试基线 | 测试配置 | 64 KiB、65536、4 MiB×64、每端 `min(chunk_count,16)` 仅为当前默认值 | 已核实 |

## 2. P0：规范、基线和前置缺陷

| 状态/ID | 执行主体与完成时点 | 操作步骤 | 文件、前置与资源依赖 | 可量化验收流程与通过判定 |
|---|---|---|---|---|
| `[~] P0-1 统一规范与数据字典` | UB 架构 Owner；P1 合入前 | 以 V4 为唯一口径合并冲突裁决；固定 attempt、PathVersion、ImportedTargetKey、错误分类、健康状态、配置键；同步 B4/B5/Q4/Q6/Q7/Q8/Q12 的修正结论 | V4 设计、两份输入设计、P0-2；架构评审 | 19 项冲突均有唯一裁决且指向 V4；无“Slice 只发送一次”“delete 失败统一重试”“idle 低即扩池”等旧表述；`new 12.md` 的过时基线不得作为执行依据 |
| `[~] P0-2 固定源码和 provider 基线` | 架构 Owner + Provider Owner；任何实现分支基线冻结前 | 选定可复现的 UMDK commit 并记录 SHA/ABI；验证 priority→tp_type 检查；建立支持/不支持矩阵 | Mooncake/UMDK 仓库、真实驱动和固件清单 | 交付版本清单和支持矩阵；缺失字段全部标 UNKNOWN；浮动 `master` 只能用于探索，不可作为验收版本 |
| `[~] P0-3 验证逐 WR inflight 计数配对（替代原批量计数项）` | 数据面开发；P1 RM legacy 运行前 | 为回退后的单 WR 路径补计数不变量测试：每 slice 一次 `addInflight`、一次完成一次 `removeInflight`；partial/flush/timeout 恰好一次 | `tent/src/transport/ub/endpoint.cpp`、`context.cpp`、`workers.cpp`；fake provider | **测试已编写，尚未运行**：`UbTeardown.ContextCountsEachSliceOnceAndDrainsToZero` 验证显式 add/remove 配对；`UbNativeDataPathTest.SingleWrRejectedPostRollsBackInflightExactlyOnce`（partial）、`SingleWrFlushCompletionReleasesInflightOnce`（flush）、`SingleWrTimeoutFenceReclaimsInflightOnce`（timeout）验证 worker 级恰好一次释放且计数归零。**回退后原批量少记缺陷不存在**：`addInflight(uint64_t)` 仍每次 +1，但每 slice 只调用一次。本机缺 gtest/CMake，需在完整 CI 环境执行后才能判为通过。K>1 仅在 P2 引入批量准入后另测 |
| `[~] P0-4 建立统一构建与证据框架` | 测试 Owner；P1 首次 CI 前 | 配置 CMake/CTest；收集 JSONL、native 调用计数、环境清单；区分 fake、interposer、物理结果 | `tent/tests/CMakeLists.txt`、CI、clang-format 20/pre-commit | **已部分完成**：`bootstrap.cpp` 已加入 `tent_ub_core_test` 目标并注册 CTest；仍缺完整 Linux 构建、证据归档、NOT_RUN 判定和 pre-commit/clang-format 20 执行。指定目标可构建、CTest 通过且证据归档后才算通过 |
| `[~] P0-5 明确功能开关与失败策略` | 架构 Owner + 控制面开发；P1 配置合入前 | 默认 RC legacy；pool/probe 默认关闭；gate 失败仅允许已验证 RM legacy，否则 unsupported；禁止自动 bonding/RC fallback | `params.h`、`ub_transport.cpp`、部署配置模板 | **配置与矩阵已实现，待 CI 运行**：`runtime_profile`、`trans_mode`、`jetty_pool/enable`、`jetty_pool/force_disable`、`probe/*` 已严格解析并在安装前经 `resolveUbRuntimeMode` 判定；默认 RC legacy、pool/probe 关闭；RC 下开启 pool/probe 或 RM profile 与 trans_mode 不一致均 InvalidArgument；RM legacy / pooled RM 在无对应 native gate 时返回 NotImplemented，adapter 初始化前即拒绝。`physical_only` 与 v1/v2 fail-closed 已完成。pool/probe/refill 生产路径仍不存在，配置矩阵测试已写入 `ub_core_test.cpp`；本机独立 smoke（真实 `params.h`/`config.cpp`/`status.cpp` 实现，仅 glog 打桩）矩阵 11 项 + 解析 17 项通过，完整 CTest 待 CI 执行后判为通过 |

**P0 出口判定：**P0-1～P0-5 全部为 `[x]`。任何一项未通过，P1 只能保留实验分支，不能声明达到 RM legacy 基线。

## 3. P1：DeviceKind、bootstrap v2、RM adapter 与 RM legacy

| 状态/ID | 执行主体与完成时点 | 操作步骤 | 文件、前置与资源依赖 | 可量化验收流程与通过判定 |
|---|---|---|---|---|
| `[~] P1-1 实现 DeviceKind 核心` | 控制面开发；本次软件交付 | 定义 UNKNOWN/PHYSICAL/BONDING/VIRTUAL/AGGREGATE；显式完整身份记录；UNKNOWN 默认拒绝；禁止名称推断物理性 | `urma_adapter.h`、`device_selection.h`、`params.h`；P0-1/P0-2 | 轻量 smoke 11 项已通过；CI 还需覆盖缺字段、重复身份、未知 kind、零 mask、空 allowlist；所有非法输入拒绝率 100% |
| `[~] P1-2 接入发现、元数据与 openContext 二次校验` | 控制面 + 数据面开发；P1 legacy 联调前 | physical_only 筛选；发布 kind/source；实时复核 name/path/EID/index/active/transport_modes；可信 kind 驱动 bonding 行为 | `ub_transport.cpp`、`topology_attrs.h`、`urma_adapter.cpp`；P1-1、fake/真实 provider | fake 安装测试必须打开且仅打开指定 PHYSICAL；真实环境逐项改变 path/EID/mask/active，native Context 创建次数均为 0；legacy 默认 RC 回归不失败 |
| `[~] P1-3 实现 bootstrap v2 线协议和 validator` | 控制面开发；本次软件交付 | v1 线格式不变；v2 使用结构化 target、EID/UASID/ID、五代次、数值 RemoteTargetProfile、transport_modes、probe map；必填字段不得从 v1 补值 | `bootstrap_types.h`、`bootstrap.h/.cpp`、`control_plane.h`；P0-1 | 独立 smoke **38/38 已通过**；完整 CTest 必须对每个必填字段执行删除测试；显式 UASID=0 接受，缺失拒绝；v1 golden JSON 不含 `rm`；旧 profile 字段与未知扩展拒绝 |
| `[~] P1-4 实现代次比较和 native 分配前 gate` | 控制面开发；P1 v2 联调前 | duplicate/stale/newer/conflict 分类；同父代次下较新 leaf 不掩盖旧 target；当前 RC runtime 对合法 v2 返回 NotImplemented；请求入口拒绝非空 reply_msg | `bootstrap.cpp`、`ub_transport.cpp`、`ub_native_data_path_test.cpp`；P1-3 | v2 RPC 后 createJetty/bindJetty 增量均为 0；stale/conflict 不修改 incumbent；混合“新 endpoint+旧 target”判 conflict；RC v1 自读写成功 |
| `[~] P1-5 实现 RM adapter profile 与 native 路径` | 数据面开发 + Provider Owner；P1 legacy 联调前 | JFR/JFS/import 同步切 RM；本地 `LocalSendProfile` 派生并校验 0..15 priority 与 RTP；RM import 后不 bind/unbind；每 WR 显式 tjetty；保留 posted_count 语义 | `urma_adapter.h/.cpp`；P0-2/P0-3/P1-2、真实 provider | **仅协议/派生 helper 与常量 static_assert 已完成**；native 路径未切换。调用记录显示 RM 路径 RC-only API 次数为 0；JFS/JFR/target 全为 RM；tp_type 不匹配在 post 前拒绝 |
| `[~] P1-6 接通真实 v2 producer/consumer 与最小 target owner` | 控制面开发；P1 双端握手前 | 从真实 node/context/topology/target 源生产代次；接收端 trusted metadata 交叉校验；candidate 完成后原子 compare-and-commit；最小 LocalPortTarget/ImportedTarget owner | `control_plane.h`、`ub_transport.cpp`、新增 target registry；P1-3/P1-4/P1-5 | **仅比较 helper 完成**；producer/consumer 与 target owner 未实现。双端 v2 握手成功；v1、错 mode、错身份、旧代次均无副作用拒绝；乱序 candidate 只有 newer 可发布 |
| `[ ] P1-7 完成 RM legacy fake 与真实 provider 基线` | 数据面 + 测试 + SRE；P2 入口前 | 先跑 fake，再跑第 7 节全部真实用例；保持 RM legacy 非池化；保存证据 | 第 6～8 节全部资源；P1-5/P1-6 | 所有强制用例 PASS/NOT_RUN 中不得有 NOT_RUN；数据 mismatch、丢重 token、非预期失败、hang 均为 0；结束后 inflight/outstanding/quota/native handle 回基线 |
| `[ ] P1-8 验证 RM legacy 回滚、flush 与 shutdown` | 恢复路径开发 + Provider Owner；P1 出口前 | 验证 modify ERROR→marker→flush_jetty→reset/unimport/delete；delete 三态；重复 shutdown 和失败重试；补进程退出契约 | `urma_adapter.cpp`、`workers.cpp`、`ub_teardown_test.cpp`；P1-7、interposer | flush 前 delete 次数为 0；WR_UNHANDLED 全部结算；UNKNOWN delete 不盲目重试；100 次带载 install/uninstall 无 hang/泄漏；崩溃退出只能报 Unknown |

**本阶段参考项：**
- **B1/Q2**：P1-8 必须区分 marker、软件 SQ flush、clean_jfc 和 delete 的所有权；marker 前 flush 返回 0 不算完成。
- **B2/Q3**：P1-6 只做最小本地 target owner；共享 target 入站回收留 P2，但不得把本地 unimport 当成远端撤销。
- **B3/Q1**：P1-5 必须产出提交结果映射表，先覆盖 legacy partial-prefix，再覆盖 pooled 单 WR。
- **B4**：P1-5～P1-8 是“物理 RM 最小验证基线”，非池化、显式选择、逐 slice 单 WR，adapter 层验证 partial-post 契约；独立 RM_ONLY 不得透明退回 RC/bonding。
- **B5**：P1-1/P1-2 已有部分实现，剩余是完整 CI 与真实 provider 身份复核。
- **Q9**：P1-7 验证 Unknown WRITE 不重放、READ 旧 DMA 安全和 WaitExpired owner。
- **Q10**：P1-8 只闭合进程内机制；崩溃/强杀后的驱动清理写入部署与 provider 契约。

**P1 出口判定：**P1-1～P1-8 全部为 `[x]`。当前 P1-1～P1-6 为 `[~]`（其中 P1-5/P1-6 仅完成协议与 helper，native 数据面未实现），P1-7/P1-8 为 `[ ]`，所以 **P1 尚未通过，P2 不得启动为正式数据面实现。**

## 4. P2：共享 Registry、静态池与单 WR 完成门

| 状态/ID | 执行主体与完成时点 | 操作步骤 | 文件、前置与资源依赖 | 可量化验收流程与通过判定 |
|---|---|---|---|---|
| `[ ] P2-1 建立 Port/Path/Topology Registry` | 控制面开发；P2 首个迭代 | 建立 PortBinding、PathKey/Version、动态邻接；A→B/B→A 分校验；支持非对称 | 新增 registry、`params.h`、topology；P1 全部、4/6 口配置 | 规则双端口每口 2 邻居分别得到 8/12；非对称按实际边数；重复/缺失/错 EID 100% 拒绝；代码无固定 4/8 掩码 |
| `[ ] P2-2 建立共享 target/import owner` | 控制面 + Provider Owner；P2 数据面接入前 | 每本地端口发布共享 target；Context+target 去重 import；endpoint/probe 持引用；epoch 退役和入站 fence | 新增 LocalPortTarget/ImportedTargetRegistry、adapter；P2-1、真实 provider | 多 endpoint/probe 的 import 数等于 Context×target 数，不随引用增长；endpoint 淘汰不 unimport；共享故障暂停全部引用路径 |
| `[ ] P2-3 实现 Context LIFO 静态业务池` | 数据面开发；P2 单 WR 接入前 | IDLE 栈、IN_USE、CREATING、预算、LIFO 租借；禁止临时绕预算扩池 | 新增 SendJettyPool、Context；P1/P2-2 | 并发 10000 次租还无重复 lease；最近归还优先；全 IN_USE 时 Deficit=0；Creating/retiring/quarantine 不穿透预算 |
| `[ ] P2-4 实现单 attempt 单 WR 与原子准入` | 数据面开发；P2 完成门前 | target/Segment 先准备；K/quota/credit/lease 非阻塞获取；gate 复核代次；失败逆序回滚；pool miss 不耗 retry | `workers.cpp`、pool、adapter；P2-3 | 每 attempt 最多一次 post 且 size=1；注入各类失败后无部分资源残留；pool miss 的 retry_count 不变、path fault 不增加 |
| `[ ] P2-5 实现一次性完成归还门与分层 owner` | 数据面 + Runtime 开发；P2 出口前 | post_returned+accepted+真实成功 CQE+handler 退出+CAS；错误不返池；target/segment/buffer 分层释放 | `workers.cpp`、pool、buffers/runtime；P2-4 | 10000 次强制早到 CQE 无旧 handler 访问新 attempt；重复归还为 0；timeout/flush/错误 CQE 回 IDLE 数为 0；staging pin 无提前释放 |
| `[ ] P2-6 按 ImportedTargetKey 聚合负载` | 数据面开发；P2 性能验收前 | 替代 endpoint_outstanding 的目标压力口径；保留 device/Context/rail 维度；排除排队/staging 时间 | `workers.h/.cpp`、Registry | 两路径共享 target 时 WR/bytes 汇总等于两路径之和；endpoint 淘汰不归零；同物理端口邻接不被计为双倍独立带宽 |

**本阶段参考项：**
- **B2/Q3**：P2-2 的入站屏障是独立证据项，本地 unimport 不算完成；普通 endpoint 引用释放不触发全 peer 撤销。
- **B7**：先冻结目录、CMake target 和抽离顺序，再动 P2-1；单 Engine、单数据面、TENT adapter 不复制资源图。
- **Q7**：P2-3 只实现静态 `DesiredServing` 语义，不引入 initial/min/max 自适应扩缩。
- **Q8**：P2-4 的目标 K 是单网络 UbTask 跨 Context 准入上限，不是现有 legacy 取批大小。
- **Q1/B3**：P2-5 使用 Accepted/NotAccepted/Unknown 与 handler 退出门，禁止以成功 CQE 单项作为归还条件。

**P2 出口判定：**P2-1～P2-6 全部通过，且相同 native 预算下吞吐/p99/CPU 已记录；性能只形成基线，不预设必然优于 RC。

## 5. P3：异步排空、删除、补池与 ABA 防护

| 状态/ID | 执行主体与完成时点 | 操作步骤 | 文件、前置与资源依赖 | 可量化验收流程与通过判定 |
|---|---|---|---|---|
| `[ ] P3-1 拆分异步错误状态机` | 恢复路径开发；P3 首个迭代 | FAULTED→DRAINING→DELETE_READY→DELETING；poller 不阻塞等待 marker；统一 dispatcher | RecoveryRegistry、workers/adapter；P2 全部 | 注入 beginError 延迟时其他 JFC 完成仍被消费；poll 热路径无同步 quiesce；重复错误只触发一次 ERROR epoch |
| `[ ] P3-2 实现完整 flush 屏障` | 恢复路径 + Provider Owner；删除线程接入前 | 匹配 send flush-done；随后 flush_jetty 取空；WR_UNHANDLED 统一结算；handler/AE 条件入 DELETE_READY | UMDK jfc/jetty 语义、RecoveryRegistry | marker 前 flush 返回 0 不算完成；marker 不按 poll 数扣 credit；丢 marker 时 delete=0；所有软件 SQ token 100% 结算 |
| `[ ] P3-3 实现独立 delete 与三态结果` | 恢复路径开发；P3 删除验收 | Engine 级有界队列；JFC 消费/删除 lease；RETAINED/CONSUMED/UNKNOWN；禁止 blanket retry | DeleteQueue、adapter、JFC owner；P3-2 | delete 不自行 poll；provider 返回失败后，无证据时重试 ioctl=0；队列满不丢 owner；并发测试无其他 Jetty CQE 被吞 |
| `[ ] P3-4 实现独立 refill 与全角色设备账本` | 恢复路径 + 数据面开发；P3 容量验收 | DesiredServing-IDLE-IN_USE-Creating；业务/probe/target/legacy/旧代次分角色汇总；退避和 replacement reserve | RefillController、预算模块；P3-3 | 全忙扩池为 0；删除 1 个只补 1 个；quarantine 达上限创建停止；4 口/6 口总账分别可解释且无穿透 |
| `[ ] P3-5 实现 tombstone 和未排空关闭` | 恢复路径 + Runtime 开发；P3 出口前 | native ID+Context incarnation+ERROR epoch+AE/handler 状态入 tombstone；未证明安全时移交存活 owner | Registry、shutdown、runtime lease；P3-3/P3-4 | 迟到 marker/AE/CQE 不误入新对象；tombstone 达预算停止新资源；1000 次 ID 复用注入无 ABA；shutdown 无最后 owner 丢失 |

**本阶段参考项：**
- **B1**：P3-1/P3-2 必须异步推进 beginError，不在 poll 热路径同步等待 marker；删除需 JFC 排他访问。
- **Q2**：`DELETE_READY` 条件已定，剩余是删除失败三态、事件屏障和并发验证。
- **B3**：P3-3 的 UNKNOWN 默认不重试；只有 provider 明确证明句柄保留才归 RETAINED。
- **Q7**：P3-4 使用 `Deficit = DesiredServing - IDLE - IN_USE - Creating`，退役与 quarantine 仍占预算。

**P3 出口判定：**P3-1～P3-5 全部通过；任何 fence/归属不明均保持 quarantine，不得以超时强制 delete。

## 6. P4：故障触发探测与逐路径恢复

| 状态/ID | 执行主体与完成时点 | 操作步骤 | 文件、前置与资源依赖 | 可量化验收流程与通过判定 |
|---|---|---|---|---|
| `[ ] P4-1 建立每端口专用 ProbeResource` | 数据面开发；P4 首个迭代 | 每端口 1 个发送 Jetty、独立 JFC/配额和注册探测区；标准一字节 WRITE；同资源最多 1 个在途 | ProbeResource、adapter、provider；P3 全部 | 业务租用 probe 数为 0；WR 字段逐项匹配；源 buffer 不变；不写业务地址；超时未排空前重发数为 0 |
| `[ ] P4-2 实现 peer campaign 和 PathHealth` | 控制面 + 数据面开发；P4 恢复联调前 | 错误路径立即隔离；peer campaign 合并；快照合法路径；未出错路径保持资格；逐路径 timer 停止 | PathRegistry/Health、Campaign；P4-1 | 一个错误仅创建一个 peer campaign；N 条错误不创建 N 倍 timer；未出错路径业务不中断；恢复后无业务时周期 WR 数为 0 |
| `[ ] P4-3 实现代次证据和 Unknown 语义` | 数据面 + 测试 Owner；P4 故障注入 | ProbeKey 含 PathVersion/fault_epoch/campaign/sequence；stale 丢弃；timeout Unknown；资源错误不批量 BAD | Probe Inflight、Health；P4-2 | 旧成功解封新故障次数为 0；stale 改变当前健康次数为 0；probe 资源失败时其他路径均 Pending；永久权限错误不进入周期循环 |
| `[ ] P4-4 验证共享故障域与恢复阈值` | 架构 + Provider + 测试；P4 出口前 | target/JFC/Context 故障按共享域处理；恢复阈值作为策略 gate；端口/节点锁存使用准确事实 | TargetRegistry、RailMonitor、节点隔离；真实 4/6 口环境 | target ERROR 暂停全部引用路径；任一合格 GOOD 解除全坏锁存但旧 epoch 不恢复；阈值 1/3 均按配置精确执行 |

**本阶段参考项：**
- **Q4**：状态统一为 `ELIGIBLE_UNVERIFIED/HEALTHY/ISOLATED/RECOVERING`；force-disable 后重开使用 `REVALIDATE` campaign，不复用旧证据。
- **Q5**：保留 peer 合并、合法路径快照、同口最多一个 probe、Pending 和逐路径停止。
- **Q6**：首次真实业务错误立即隔离并退役发送 Jetty；failure_threshold 不单独证明 BAD，probe 不能解除管理员禁用。
- **Q8**：P4-1 的 probe 不占业务 K，但占控制额度和真实 JFC credit。
- **B1/B2**：probe 自身 flush 与共享 target 入站恢复沿用同一屏障语义。

**P4 出口判定：**P4-1～P4-4 全部通过，包含“恢复后无业务”和“新故障与旧成功交错”。

## 7. P5：TENT/Store 接入、系统验收与启用

| 状态/ID | 执行主体与完成时点 | 操作步骤 | 文件、前置与资源依赖 | 可量化验收流程与通过判定 |
|---|---|---|---|---|
| `[ ] P5-1 接入真实 buffer/staging/Store 生命周期` | Runtime/Store + 数据面开发；P5 首个迭代 | free_requested、deferred teardown、BufferHandle、H2D/POST 与网络完成分层；Borrowed pin 契约 | `proxy_manager.cpp`、`transfer_engine_impl.cpp`、Store；P4 全部 | freeBatch/析构/timeout 交错下 use-after-free=0；staging 只有在网络及 H2D 均安全后复用；未排空 owner 100% 可追踪 |
| `[ ] P5-2 完成 SDK 与关闭语义` | Runtime/控制面开发；P5 系统联调前 | register/import、submitRead/Write/Transfer、status/wait/cancel/drain/health/close；Draining/Unproven 返回；最终 URMA uninit 最后 | UbEngine/API、runtime；P5-1 | cancel/wait timeout 不释放 DMA owner；close 可重复；多 Engine 共存无提前 uninit；公共 API 无 native JFC 暴露 |
| `[ ] P5-3 完成观测、灰度与回滚` | SRE + 测试 + 架构；生产灰度前 | 指标/日志、告警、force_disable、cooldown；保留已验证 RM legacy；禁止在线 native mode 切换 | 全模块、部署配置；P5-2 | 指标可解释 IDLE/IN_USE/creating/retiring/quarantine、K/pool 等待、campaign、早到 CQE；force disable 后新 pooled post=0，已有责任安全收敛 |
| `[ ] P5-4 完成全矩阵验收与生产门禁` | 全体 Owner；生产启用前 | 执行 UT/Integration/ST、4/6 口、非对称、故障注入、30 分钟 soak、性能对照和回滚 | 全部资源与证据；P5-3 | 强制用例无 FAIL/NOT_RUN；数据错误、hang、泄漏、ABA、提前释放均为 0；性能数据完整归档；各 Owner 签字后才打开 pooled flag |

**本阶段参考项：**
- **Q9**：P5-1 区分 LogicalState、ExecutionOutcome、ResourceSafety；Unknown WRITE 不透明重放。
- **Q10**：P5-2 明确 close 返回 Draining/Unproven；崩溃/强杀由内核驱动撤销，业务只能报 Unknown。
- **Q11**：P5-3/P5-4 先形成同语义同预算基线，再确定生产性能阈值；性能不得替代正确性 gate。
- **B6**：P5-4 的版本矩阵、故障注入环境和权限证据缺一即 NOT_RUN。
- **B2**：target/MR 注销和 backing buffer 释放必须等入站 DMA 安全。

**P5 出口判定：**P5-1～P5-4 全部通过；强制用例无 FAIL/NOT_RUN，且各 Owner 签字后才打开 pooled flag。

## 8. RM legacy 基线专项分析

### 8.1 真实基线结论

**当前 `956af65e` 不存在 RM legacy。**现有可运行基线是 RC/CTP、bootstrap v1、每 endpoint 固定 Jetty 数组、逐 slice 单 WR post。工作树新增的 v2/DeviceKind/两类 Profile 只提供协议、准入和派生 helper，不构成 RM legacy。

| 维度 | 当前已有 | 当前没有或限制 |
|---|---|---|
| 数据类型 | CPU DRAM→DRAM，READ/WRITE，单 SGE | 无显存 staging、通知、SEND/RECV/atomic |
| 切片/调度 | 默认 64 KiB、多 rail、quota、retry、cancel；逐 slice 单 WR post | 无批量接纳、无 WR chain、无 K 参数；pooled 单 attempt 单 WR 与目标 K 均待实现 |
| native 模式 | JFR/JFS/rjetty 均为 RC/CTP | 无 TransportMode、RM/RTP profile、RM target owner |
| 握手 | bootstrap v1，EID/ID/UASID 数组、endpoint/segment generation | 无真实 node/context/topology/target 代次来源 |
| 生命周期 | inflight 预登记、partial prefix、quiesce/flush/reset/delete | 无异步池、独立 delete/refill、数据面 probe |
| 测试 | fake core/native-data-path/teardown | 无 TENT 真实 provider 双端 RM 验收；Classic UB 测试不能替代 |

### 8.2 存量数据结构与运行状态

| 结构/状态 | 当前职责 | RM legacy 必须处理的问题 |
|---|---|---|
| `DeviceInfo/DeviceCapabilities` | native name/path/EID/active/caps | 此次新增 kind/source；真实 RM 仍需校验 RM/RTP priority |
| `UbBootstrapDesc` | v1 RC 身份与 Jetty 数组 | 此次新增 v2；真实 producer 尚需补全五代次和 target owner |
| `JettyOptions/RemoteJettyInfo` | depth/priority、EID/ID/UASID/token | 无 transport mode/profile；RM import 后不得走 RC bind |
| `RealJetty` | CREATED/BOUND/ERROR/RESET，拥有 remote_ | RM 要解除“必须 BOUND 才能 post”和 per-Jetty remote 所有权 |
| `UbEndpoint` | Uninitialized→Prepared/Binding→Ready→Failed/Destroying | RM legacy 可复用一次性 incarnation，pool 不得直接复用该所有权 |
| `UbSlice/UbAttemptToken` | attempt/generation、防旧完成、retry | pooled 改为每 attempt 单 WR；不能混淆 kPosted 与 native accepted |
| `Inflight/WorkRequest` | token、segment、quota、lane、posted_count | 保留 prefix 语义；pool 还需 post_returned/handler gate |
| `UbContext` | Active/Failed/Draining/Closed、outstanding | 逐 WR `addInflight`/`removeInflight` 配对是恢复屏障前置条件 |
| `RealJfc/RealJetty quiesce` | marker、flush_jetty、segment 清理 | 当前同步持 JFC 锁等待；P3 必须异步化 |

### 8.3 历史遗留问题清单

1. JFR、JFS、rjetty 三处 RC 硬编码，不能只改一个常量。
2. 默认 priority=15 与 CTP 特判不能证明 RM/RTP 可用。
3. RC bind/unbind 与 postable=BOUND 强耦合，RM 必须拆分。
4. endpoint 独占 native import，不能直接支持 Context 共享 target。
5. 批量接纳与 K 参数属于 P2 新增；回退后基线不存在批量少记问题，但新增时必须同步修正 Context 计数。
6. inflight 虽预登记可防丢 token，但没有完整完成归还门。
7. quiesce 同步持 JFC 锁等待 marker，不能原样搬入独立线程。
8. 成功 CQE 当前按 slice 全长记账，真实 provider 验收需增加 completed_bytes 一致性检查。
9. v1 UASID 缺省与显式零不可区分，RM 不得复用。
10. fake adapter 不证明 RM、tp_type、真实 DMA fence 或物理端口健康。

### 8.4 RM legacy 最小补充范围

- 单进程显式 `rc|rm_legacy`，默认 RC，禁止运行中切换 native mode。
- non-bonding physical-only、RM/RTP、真实 priority/profile/capability 校验。
- JFR/JFS/import 全 RM；RM 路径不调用 bind/unbind。
- v2 producer/consumer、最小 target owner、真实代次事务发布。
- 保留 READ/WRITE、切片、retry、quota、单 WR post 和 shutdown 框架；补逐 WR 计数配对测试。
- RM legacy 可保留非池化 endpoint 与逐 slice 单 WR 作为对照；pooled 路径必须另行强制 size=1，不得据回退前 WR-chain 实现设计验收。
- 不在 P1 引入池化、补池、主动周期探测或 Group。

## 9. 真实 provider 验收准备

### 9.1 环境配置要求

1. 两台物理 Linux 主机， non-bonding physical UB 链路互通；P1 至少一口，后续阶段扩展 4/6 口。
2. 两端 Mooncake、UMDK/liburma、内核、驱动、固件和编译 ABI 一致；记录完整 commit 与版本。
3. 设备/EID active，provider 报告 RM capability，至少一个 priority 支持目标 RTP/tp_type。
4. 控制面地址和端口双向互通；测试 buffer 位于可注册 CPU DRAM。
5. 构建启用 `WITH_TE=ON`、`USE_TENT=ON`、`USE_UB=ON`、`BUILD_UNIT_TESTS=ON`；真实构建确认 `TENT_HAS_REAL_URMA=1`。
6. 部署测试 interposer，支持 bad_wr 选择、post-return 延迟、CQE 顺序记录和 native 调用计数；没有它时 partial/early 用例必须记 NOT_RUN。
7. physical_only 使用经审核的完整身份 allowlist；对端身份来自受信任元数据，而非对端自报。
8. 每轮测试前记录 native handle 基线、inflight/outstanding/quota、队列和日志位置；测试后再次采集。

### 9.2 验收执行顺序

```text
版本/身份 preflight
  → fake 全部回归
  → 真实 RM adapter capability gate
  → 双端 v2 握手与负向身份用例
  → RM legacy READ/WRITE 正确性
  → partial post / early CQE interposer
  → flush/delete/shutdown/fault
  → 30 分钟 soak 与资源回基线
  → 性能记录和证据归档
```

任一阶段出现数据 mismatch、未知 native 所有权或无法解释的 handle 增长，立即停止后续性能测试，保留现场并判 FAIL。

## 10. 真实 provider 测试用例集

| ID/用例 | 配置与步骤 | 校验指标 | 通过规则 |
|---|---|---|---|
| `ENV-01 身份与版本` | 读取双端 commit、ABI、设备、EID、active、priority/tp_type、transport_modes | 所有字段与 allowlist/support matrix 一致 | 缺项、UNKNOWN、bonding、mask 不一致均 FAIL，不进入数据面 |
| `MODE-01 模式门禁` | RM↔RM、RC↔RC、RM↔RC、v1→RM、v2→当前 RC runtime | 成功/拒绝路径及 create/bind 计数 | 仅合法同模式成功；非法组合 100% 在 native target 分配前拒绝 |
| `BOOT-02 代次事务` | duplicate、逐级 stale、newer、同代次不同内容、乱序 candidate | incumbent 和 watermark | stale/conflict 不改变 incumbent；native 失败不推进；仅 newer commit |
| `RW-01 READ 正确性` | 双向 READ；1B、4KiB、64KiB±1、1MiB；batch 1/16；线程 1/6；每组合 1000 次 | 数据、token、completed_bytes、状态 | mismatch/丢重 token/非预期 FAILED/TIMEOUT/hang 均为 0；completed_bytes 与契约一致 |
| `RW-02 WRITE 正确性` | 同 RW-01，双向 WRITE，源数据保持不变 | 目的数据、源数据、token、状态 | 全部正确；非 inline 源不变；结束时 inflight/outstanding=0 |
| `POST-01 深度与 defer` | Jetty depth=2，提交 8 slices，逐步释放完成 | accepted/defer/retry | 超深度部分 defer；retry 不增加；无提前归还或资源泄漏 |
| `POST-02 partial post` | adapter 级直接提交多 WR 链；worker 路径每 slice 只提交单 WR | posted_count、前缀完成、后缀回滚 | 已接受前缀只完成不重发；未接受后缀可回滚；该用例验证 adapter 契约，不代表 worker 批处理存在 |
| `POST-03 早到 CQE` | 强制 CQE 先于 post return，执行 10000 次 | post/handler/lease 事件顺序 | pool 归还前四个条件全部满足；旧 handler 访问新 attempt=0；最终结算恰好一次 |
| `FENCE-01 flush 分类` | 注入普通错误、FLUSH_ERR、FLUSH_ERR_DONE、WR_UNHANDLED、重复/早到 marker | dispatcher 分类和 credit | marker user_ctx 从不解引用；WR_UNHANDLED 不算成功；marker 不按 poll 条数扣 credit |
| `FENCE-02 软件 SQ 排空` | marker 前调用 flush、marker 后分批 flush、丢 marker | flush_done、SQ CI/PI、delete | marker 前返回 0 不得删除；marker 后取空且责任结算才 DELETE_READY；丢 marker 保持 quarantine |
| `DEL-01 delete 三态` | 注入 retained、consumed、unknown、AE 未 ACK | 旧指针、重试、预算/tombstone | 无证据时盲目重试=0；UNKNOWN 不解引用；native 预算不提前释放 |
| `DEL-02 poll/delete 并发` | delete 与 poll/clean_jfc 交错，其他 Jetty 持续完成 | JFC lease、CQE 归属 | 无死锁；其他 Jetty CQE 丢失=0；delete 线程 poll 调用=0 |
| `SHUT-01 带载关闭` | 100 次 install/submit/uninstall；注入 fence/delete/shutdown 失败 | 重试、owner、native handle | 无 hang；失败可重试；最终 handle 回基线；quarantine 有完整 owner |
| `FAULT-01 链路与恢复` | 链路断开/恢复、target 重建、旧事件延迟注入 | 路径代次、重试、旧事件归属 | 旧 epoch 不复活；无旧 DMA 写新目的；参数/权限错误不无限重建 |
| `SOAK-01 稳定性` | READ/WRITE 混合、可变 batch、周期 install/uninstall，运行 30 分钟 | hang、mismatch、队列/inflight/handle 趋势 | 错误和 hang 为 0；结束资源回基线；handle/队列无单调无界增长 |
| `PERF-01 对照记录` | 同机同语义同预算比较 RC 与 RM legacy | 吞吐、p50/p99、CPU、pool/queue 等待 | 不预设性能方向；原始 JSONL 完整，预算一致，结果可复现；不得以性能替代正确性 gate |

## 11. 本次 P1 软件实现与实际验证记录

### 11.1 已写入的实现

- `tent/include/tent/transport/ub/bootstrap_types.h`：轻量 v1/v2 wire 类型和严格 JSON 编解码。
- `tent/include/tent/transport/ub/bootstrap.h`、`tent/src/transport/ub/bootstrap.cpp`：v2 validator、trusted peer identity、mode gate、代次比较。
- `tent/include/tent/runtime/control_plane.h`：继续导出原 `UbBootstrapDesc`，现有调用方无需改名。
- `tent/src/runtime/control_plane.cpp`：严格 v2 解码失败时仍返回可解析的 v2 error envelope。
- `tent/src/transport/ub/ub_transport.cpp`：v2 在 endpoint/Jetty 分配前拒绝；物理筛选和 kind 元数据。
- `tent/include/tent/transport/ub/urma_adapter.h`、`device_selection.h`、`params.h`：DeviceKind、身份配置、`LocalSendProfile` 派生、RM 能力/priority 掩码和 fail-closed resolver。
- `tent/include/tent/transport/ub/topology_attrs.h`、`tent/src/transport/ub/urma_adapter.cpp`：kind 元数据、能力采集和 openContext 实时复核。
- `transport_selector_test.cpp`、`ub_core_test.cpp`、`ub_native_data_path_test.cpp`：协议、设备、安装、profile 派生、v2 零分配及 RC 回归用例。

### 11.2 已实际执行

| 校验 | 实际结果 |
|---|---|
| `clang++ -std=c++17 -fsyntax-only` 独立检查 `bootstrap.cpp` | PASS |
| 独立 bootstrap smoke（真实 clang++ 编译运行） | **38/38 PASS** |
| 独立 DeviceKind/config smoke（真实 clang++ 编译运行） | **11/11 PASS** |
| `git diff --check` | PASS |
| 新增协议文件 clang-format 17 | 已执行；项目要求的 clang-format 20/pre-commit 仍待 CI |
| 第一步 physical RM 前置代码自测 UT | **按本次用户报告：全部通过**；本会话未取得可重放命令、构建参数和逐测试日志，不能替代 CI 证据 |

轻量 smoke 仅对 `Status` 的日志头使用临时 stub；被测协议、JSON、身份、代次、resolver 和配置函数均为仓库真实实现，未替换其逻辑。

### 11.3 尚未执行，不得宣称通过

- 本机没有 CMake，首次构建在工具检查阶段停止。
- 本机没有 glog/gtest，完整 `tent_transport_selector_test`、`tent_ub_core_test`、`tent_ub_native_data_path_test` 未在本会话复跑；用户报告的自测通过需由 CI 日志复现。
- pre-commit 与 clang-format 20 不可用，PR 范围 hooks 未运行。
- 当前没有真实 RM adapter、v2 producer/target owner 和 RM legacy 数据面。
- P0-3 已改为逐 WR 计数配对验证；回退后的基线不再存在批量少记缺陷。回归测试已编写（`ub_teardown_test.cpp` 的配对用例，`ub_native_data_path_test.cpp` 的 partial/flush/timeout 用例）。用户报告第一步自测 UT 全部通过，但本会话未取得完整可重放日志，仍需 CI 归档。
- 未执行真实 provider、双端 RM、物理 fence、4/6 口或性能测试。

### 11.4 CI 中必须执行的软件验收命令

以下命令在依赖完整的 Linux 构建环境执行，路径按实际 build dir 调整：

```bash
cmake -S . -B build \
  -DWITH_TE=ON -DWITH_STORE=OFF \
  -DUSE_TENT=ON -DUSE_UB=ON -DBUILD_UNIT_TESTS=ON
cmake --build build --parallel \
  --target tent_transport_selector_test tent_ub_core_test \
  tent_ub_native_data_path_test tent_ub_teardown_test
ctest --test-dir build --output-on-failure \
  -R 'tent_(transport_selector|ub_core|ub_native_data_path|ub_teardown)_test'
pre-commit run --files \
  mooncake-transfer-engine/tent/include/tent/runtime/control_plane.h \
  mooncake-transfer-engine/tent/include/tent/transport/ub/bootstrap_types.h \
  mooncake-transfer-engine/tent/include/tent/transport/ub/bootstrap.h \
  mooncake-transfer-engine/tent/include/tent/transport/ub/device_selection.h \
  mooncake-transfer-engine/tent/include/tent/transport/ub/params.h \
  mooncake-transfer-engine/tent/include/tent/transport/ub/topology_attrs.h \
  mooncake-transfer-engine/tent/include/tent/transport/ub/urma_adapter.h \
  mooncake-transfer-engine/tent/src/runtime/control_plane.cpp \
  mooncake-transfer-engine/tent/src/transport/ub/bootstrap.cpp \
  mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp \
  mooncake-transfer-engine/tent/src/transport/ub/urma_adapter.cpp \
  mooncake-transfer-engine/tent/tests/CMakeLists.txt \
  mooncake-transfer-engine/tent/tests/transport_selector_test.cpp \
  mooncake-transfer-engine/tent/tests/ub_core_test.cpp \
  mooncake-transfer-engine/tent/tests/ub_native_data_path_test.cpp
```

**通过判定：**四个 CTest 目标全部构建并 100% 通过，pre-commit 无错误；否则 P1-1～P1-6 保持 `[~]`，不得改成 `[x]`。

## 12. 需向选定 UMDK 验证版本验证的 provider 契约用例

本节只覆盖参考答复中标记“可直接复用”的 B1、B2、B3、Q10，以及它们依赖的最小环境。用例必须在 P0-2 选定并固定的 UMDK commit、驱动、固件和 liburma ABI 上执行；源码阅读只能作为预期，不得替代运行证据。

### 12.1 前置环境用例

| ID | 目的 | 步骤 | 证据 | 通过判定 | 归属 |
|---|---|---|---|---|---|
| `UC-ENV-01` 版本与能力快照 | 固定契约解释版本 | 记录 Mooncake/选定 UMDK commit、内核、驱动、固件、liburma ABI、编译参数；导出 device attr 的 transport_modes、rm_tp_cap、priority_info[16].tp_type | 版本清单、attr dump | 字段完整；缺项标 UNKNOWN，不进入后续数据面用例 | P0-2 |
| `UC-ENV-02` priority/RTP 可用性 | 确认本地 JFS 可覆盖 RTP target | 对每个候选 priority 创建 RM JFS，尝试提交 RTP target WR | create/post 返回、native 计数 | 至少一个 priority 的 bind mask 含 RTP 位；不匹配必须在 post 前拒绝 | P0-2 / P1-5 |

### 12.2 B1：flush、JFC 与删除并发

| ID | 目的 | 步骤 | 证据 | 通过判定 | 归属 |
|---|---|---|---|---|---|
| `UC-FLUSH-01` marker 前软件 flush | 证明空 poll 不等于排空 | modify ERROR 后立即调用 `urma_flush_jetty`，再 poll marker | flush 返回值、SQ CI/PI | flush 返回 0 不得判 DELETE_READY；marker 到达后才可进入软件 flush | P1-8 / P3-2 |
| `UC-FLUSH-02` marker 后取空 | 证明剩余软件记录必须提取 | marker 后循环 `urma_flush_jetty` 至 0，记录每条 CR | CR 数量、status、user_ctx、token | 返回记录为 `WR_UNHANDLED` 且 user_ctx 有效；全部 token 被结算 | P1-8 / P3-2 |
| `UC-FLUSH-03` marker 字段语义 | 防止解引用无效 user_ctx | 构造 marker 早到、重复、带脏 user_ctx 的场景 | CR status/local_id/user_data | marker 的 `user_ctx` 从不被解引用；不按 poll 条数扣 WR/JFC credit | P1-8 |
| `UC-FLUSH-04` 共享 JFC 隔离 | 证明排空不吞其他 Jetty 完成 | 同一 send JFC 上运行多个 Jetty，退役其中一个并持续 poll | 完成归属、丢失计数 | 其他 Jetty 的真实 CQE 丢失为 0；`clean_jfc` 只清理目标 local_id | P3-2 / P3-3 |
| `UC-FLUSH-05` JFC 排他访问 | 证明 poll/delete 协调可实现 | 删除线程与 poll 线程交错访问同一 JFC；分别测试 `lock_free=0` 和 `1` | 死锁、丢失 CQE、native 调用序列 | 无死锁；无重复消费；`lock_free=1` 时上层必须自行串行化 | P3-3 |
| `UC-FLUSH-06` 删除失败后的对象表 | 证明 delete 失败不能盲目重试 | 注入 ioctl 失败、AE 未 ACK、partial delete | 对象表项、handle、返回码 | 失败后不假定句柄完好；默认归 UNKNOWN；只有明确保留证据才归 RETAINED | P3-3 |

### 12.3 B2：共享 target 与入站屏障

| ID | 目的 | 步骤 | 证据 | 通过判定 | 归属 |
|---|---|---|---|---|---|
| `UC-TGT-01` Context 级共享 import | 证明同 Context 多 JFS 可复用 target | 同一 Context 内用业务 JFS 与 probe JFS 提交指向同一 imported tjetty 的 WR | import 次数、WR 成功数 | import 次数不随发送成员增长；两种 WR 均成功 | P1-6 / P2-2 |
| `UC-TGT-02` 跨 Context 隔离 | 防止共享越界 | 用另一 Context 的 imported target 提交 WR | 返回值、错误码 | 必须失败；不得跨 Context 复用 handle | P2-2 |
| `UC-TGT-03` 普通引用释放 | 区分引用与授权撤销 | 释放一个 endpoint/probe 引用，但保留其他引用并继续发送 | import 生命周期、后续 WR | 普通引用释放不 unimport、不中断其他路径 | P2-2 |
| `UC-TGT-04` 入站 DMA 屏障 | 证明撤销授权后入站停止 | 撤销发布、停止新 WR、执行候选 provider 屏障操作，再由 peer 发起旧授权写入 | 入站完成时间、屏障返回、内存变化 | 必须给出“旧授权入站 DMA 已终止”的 provider 证据；peer ACK/超时不算 | P2-2 / P5-1 |
| `UC-TGT-05` 无响应 peer | 证明无 peer 配合仍可安全退役 | 阻断 peer 响应后撤销 target/MR | 操作返回、残留 DMA、quarantine 状态 | 无屏障证据时进入 quarantine，不释放 MR/backing buffer | P3 / P5 |
| `UC-TGT-06` MR 注销与 basebuffer | 证明注销覆盖所有授权 | 多 target/peer 共享同一 MR，注销后检查远端访问 | 远端读写结果、注销返回 | 注销前所有授权访问已撤销；不得只撤一个 target | P5-1 |

### 12.4 B3：接受状态、事件归属与 ABA

| ID | 目的 | 步骤 | 证据 | 通过判定 | 归属 |
|---|---|---|---|---|---|
| `UC-ACC-01` posted_count 语义 | 固定 legacy 前缀契约 | 多 WR 链在中间 WR 注入 `bad_wr` | posted_count、前缀完成、后缀回滚 | 已接受前缀只完成不重发；未接受后缀可回滚；token 不重复 | P1-5 / P1-7 |
| `UC-ACC-02` 零接受与 CQE 冲突 | 识别契约冲突 | 让 post 返回确定零接受，同时注入该 token 的成功 CQE | CQE、返回码、inflight 状态 | 按契约冲突隔离，不归还、不重试、不重复结算 | P1-5 / P2-5 |
| `UC-ACC-03` 早到 CQE | 证明 CQE 可先于 post 返回 | interposer 延迟 post return，强制 CQE 先到 | 事件顺序、归还门 | post 未返回前不归还；最终只结算一次 | P2-5 |
| `UC-ACC-04` marker 归属 | 固定 marker 归属字段 | 多 Jetty 同 JFC，制造 flush-done | local_id、user_data、JFC/epoch | 归属到正确 Jetty；无法归属时保守隔离 | P1-8 / P3-2 |
| `UC-ACC-05` AE 生命周期 | 证明 delete 等待 AE ACK | 制造 AE，未 ACK 时删除；再 ACK 后重试 | AE 计数、delete 返回 | 未 ACK 时不得释放对象内存；register/ack 顺序符合契约 | P1-8 / P3-3 |
| `UC-ACC-06` native ID 复用 | 证明迟到事件不会误路由 | 删除 Jetty 后复用同 ID，注入迟到 RX CQE/AE | CQE 归属、对象表 | 迟到事件不进入新对象；必要时延迟发布新 ID | P3-5 |
| `UC-ACC-07` timeout 与 Unknown | 固定 timeout 责任 | 提交后停止完成，超时后再注入成功 CQE | 状态、owner、结果 | timeout 不直接判物理 BAD；责任保留到 fence；Unknown WRITE 不改写为成功 | P1-7 / P4-3 |

### 12.5 Q10：关闭与进程终止

| ID | 目的 | 步骤 | 证据 | 通过判定 | 归属 |
|---|---|---|---|---|---|
| `UC-EXIT-01` 正常 close 未排空 | 验证 Draining/Unproven 返回 | 带在途 WR 调用 close，保留 owner 后观察 | close 返回、owner 状态 | 返回 Draining/Unproven；不提前释放 buffer/target | P1-8 / P5-2 |
| `UC-EXIT-02` 重复 close | 验证幂等 | 连续调用 close 两次以上 | 返回码、原生调用计数 | 幂等；不重复 uninit、不二次释放 | P5-2 |
| `UC-EXIT-03` 强杀进程 | 验证崩溃边界 | 带在途 WR 执行 `SIGKILL`，观察驱动/内核回收和对端行为 | 对端 CQE/超时、内存状态、重启后资源 | 明确哪些由驱动撤销、哪些只能报 Unknown；不得宣称业务结果确定 | P5-2 / 部署契约 |
| `UC-EXIT-04` 多 Engine 共存 | 验证 uninit 顺序 | 两个 Engine 共享 runtime，先后关闭 | uninit 计数、原生调用顺序 | 最后一次 uninit 晚于全部原生依赖释放 | P5-2 |

**统一记录要求：**每个用例必须记录 commit/ABI、设备、EID、priority/tp_type、native 调用计数、开始/结束资源基线；任何环节无法执行即记 NOT_RUN，不得推断 PASS。

## 13. 编码计划：已完成、部分完成与未完成

### 13.1 当前分支代码已覆盖

| 能力 | 状态 | 说明 |
|---|---|---|
| DeviceKind 与受信身份准入 | 已实现，未验收 | `device_selection.h`、`params.h`、`urma_adapter.cpp` openContext 复核 |
| physical_only 筛选与元数据发布 | 已实现，未验收 | `ub_transport.cpp`；默认关闭 |
| bootstrap v1/v2 线协议 | 已实现，未验收 | v1 不变；v2 数值 profile、严格白名单、必填字段 |
| 代次比较与 native 分配前 gate | 已实现 helper，未接入事务 | duplicate/stale/newer/conflict；真实 producer/consumer 未接 |
| Local/Remote Profile 拆分 | 已实现 | 本地派生 0..15 priority 与 RTP 掩码；远端只含 RM/JETTY/RTP |
| 能力采集与 native 常量校验 | 已实现 | `rm_tp_cap`、16 项 priority 掩码；UMDK 头下 static_assert |
| 测试 | 作者报告自测通过，正式证据待补 | 38 项 profile smoke、11 项 DeviceKind smoke；完整 CTest/CI 日志需归档 |

### 13.2 尚未完成，按依赖排序

| 顺序 | 工作 | 依赖 | 交付物 |
|---|---|---|---|
| 1 | P0-3 逐 WR inflight 计数配对回归 | 无 | 单 WR 路径 add/remove 配对测试与 partial/flush/timeout 用例 |
| 2 | P0-4 完整构建与证据框架 | 1 不必等待 | CTest/pre-commit/证据归档脚本与首轮报告 |
| 3 | P0-5 完整开关与失败策略 | 1、2 | 键与矩阵已实现（`params.h`、`ub_transport.cpp`、`ub_core_test.cpp`）；待 CI 执行矩阵测试 |
| 4 | P1-5 RM native adapter | 1、3、真实 provider | JFR/JFS/import 全 RM、免 bind、逐 WR tjetty、提交结果映射 |
| 5 | P1-6 真实 v2 事务与最小 target owner | 3、4 | producer/consumer、compare-and-commit、LocalPortTarget/ImportedTarget |
| 6 | 第 12 节 UC-FLUSH/ACC/TGT/EXIT 用例 | 4、5、实机 | provider 契约证据报告 |
| 7 | P1-7 RM legacy 双端与 30 分钟 soak | 4、5、6 | 真实 READ/WRITE、partial、early、资源回基线 |
| 8 | P1-8 flush/delete/shutdown | 6、7 | 三态删除、JFC 排他、进程退出契约 |
| 9 | P2-1～P2-6（含目标 K 与批量准入） | P1 全部 | Registry、共享 import、LIFO 池、单 WR 完成门 |
| 10 | P3-1～P3-5 | P2 全部 | 异步 drain/delete/refill、tombstone |
| 11 | P4-1～P4-4 | P3 全部 | probe、campaign、逐路径恢复 |
| 12 | P5-1～P5-4 | P4 全部 | Store/staging、SDK、灰度回滚、全矩阵 |

### 13.3 可并行工作

- 第 12 节 `UC-ENV` 与 `UC-FLUSH-01/02/03`、`UC-ACC-01` 可在 P1-5 前用现有 RC/legacy 路径和 UMDK 小工具先行验证。
- interposer、fake provider 和证据归档脚本可在 P0-4 阶段搭建。
- B7 的目录/CMake 冻结可与 P1-5 并行，但不得提前引入 P2 组件骨架到生产路径。

### 13.4 禁止提前项

- P1 未全部 `[x]` 前，不实现 pool/probe/refill 生产路径，不打开 pooled flag。
- 不用 fake 或 smoke 结果替代真实 provider、物理 fence、4/6 口与 soak。
- 不把“设计已明确”写成“实现完成”，也不把“代码已写入”写成“验收通过”。

## 14. 紧接着的执行顺序

1. 在完整 CI 环境跑第 11.4 节，先关闭所有编译、格式和 RC 回归问题。
2. 完成 P0-3，用单 WR 计数配对和 partial/flush/timeout 用例证明恢复计数正确。
3. 实施 P1-5/P1-6，禁止用常量 1 伪造 node/context/target 代次。
4. 按第 9～10 节部署双端 provider 和 interposer，完成 P1-7/P1-8。
5. 只有 P1 全部签署通过后，才启动 P2 的共享 target 和 Context 池。
