# Physical UB RM Legacy 设计分析与详细设计

日期：2026-11-10  
范围：仅覆盖 TENT UB 的 non-bonding physical URMA RM Legacy，从显式模式准入到双端 READ/WRITE、错误排空和正常关闭。采用基准设计中 P1 的“物理 RM 最小验证基线”。不包含 Jetty 池化、共享池 refill、周期探测、peer campaign、P2-P5 Store/灰度功能。

## 1. 设计分析

### 1.1 当前未实现现状

当前分支 `tent/tent_rm`，HEAD `e1d39c4d` 已包含 RM Legacy 的前置准入与协议脚手架，但生产数据面仍只有 RC Legacy：

| 模块 | 当前实现 | RM Legacy 缺口 |
|---|---|---|
| 配置/模式 | `UbParams` 支持 `rc_legacy`、`rm_legacy`、`rm_pool` 和 `trans_mode`；默认 RC，pool/probe 关闭 | `resolveUbRuntimeMode` 对 well-formed RM Legacy 返回 `NotImplemented`；缺少显式、可运行的 RM Legacy gate |
| DeviceKind/profile | 受信 `device_identities`、physical-only 筛选、openContext capability 复核；`LocalSendProfile` 验证 RM/RTP、priority 0..15 和 RTP mask | profile helper 尚未驱动实际 JFS/JFR/Jetty 配置 |
| Bootstrap | v1 DTO 保持 RC；v2 DTO、严格解析、RM/RTP 校验、peer identity 校验和 generation comparison helper 已存在 | 生产 endpoint 仍生成 v1；RM 请求在 endpoint/native 副作用前被 RC runtime gate 拒绝；没有真实 producer/consumer、原子提交和 target owner |
| Native adapter | 可注入 URMA 边界、真实 RC JFS/JFR/Jetty/import/bind/post/poll、同步 quiesce | 缺 RM JFS/JFR/import/post 配置和按 WR target 传参；当前 import 后 bind；缺 provider 结果映射和已验收的 RM teardown |
| Endpoint/worker | 每 endpoint 创建固定数量 Jetty；remote Jetty 数组等长逐项 bind；worker 实际每 slice 调一次 `post({work})`；endpoint/context 有 outstanding 计数和 retry | endpoint 的 RC bind 与 RM 模式冲突；正常/错误完成、completed bytes、timeout、partial 接受和关闭需由 RM provider 验证 |
| 验收 | 新增 fake/UT 和轻量 smoke；用户报告第一步自测 UT 全部通过 | 无本次可复现 UT 日志、完整 Linux CI/CTest 证据、双端 RM provider、真实 DMA fence 或 soak 结果 |

RM Legacy 必须独立于 pooling 存在。它是验证 native RM、协议身份、逐 WR 接受语义和资源关闭的前置运行模式；不能因“legacy”而复用 RC 的 native profile 或 bind 语义。

### 1.2 与基准设计预期的差距

1. **模式生命周期未接通**：基准要求 `rc_legacy` 默认、显式 `rm_legacy`、安装前 fail-closed；目前 RM Legacy 的配置被拒绝，不能启动。
2. **RM native profile 仅有数据模型**：本地能力检查已经可做，但 `RealJfc`、`RealJetty` 仍创建 RC 资源；RM/RTP priority mask 没驱动 native 创建/提交。
3. **连接语义不兼容**：当前 `UbEndpoint::bind` 需要一一对应的 Jetty ID 并调用 bind；RM Legacy 必须按 RM target import/引用工作，不能调用 RC bind/unbind。
4. **握手仅有格式和 validator**：v2 缺真实本端 descriptor 生产、对端 trusted metadata 匹配、candidate 与现有状态的原子 compare-and-commit，以及失败回滚。
5. **提交/完成语义待证**：当前 worker 对单 WR 使用 `posted_count`，完成按 slice 全长结算；RM 需明确定义 native 返回状态如何区分 NotAccepted/Accepted/Unknown，并验证 CQE 字节长度/状态。
6. **关闭只有通用同步路径**：endpoint timeout/retire 依赖同步 quiesce；RM provider 的 modify ERROR、marker、flush、reset 适用条件、unimport/delete 顺序及失败 handle 行为尚无版本化证据。

### 1.3 影响范围

- **数据正确性**：READ/WRITE 的 RM target、priority/tp_type、MR 权限和地址范围错误会造成 post 失败或错误结果。
- **资源安全**：接受状态不清、timeout 后重试、过早 unimport/delete 或释放 buffer 都可能导致重复写入、DMA use-after-free 或 native 资源泄漏。
- **兼容性**：RM Legacy 配置必须失败关闭；RC v1/v1 endpoint 生命周期保持现状，不能自动切换或误走 RM。
- **后续池化准入**：只有 RM Legacy 双端读写、flush/delete/shutdown 的 provider 基线通过，P2 池化才有可依赖的 RM 语义。
- **部署**：仅允许有经审计 identity 的 non-bonding physical 设备；fake 与 UMDK 源码阅读不代表物理 provider 支持。

### 1.4 关键约束

- 默认运行配置不变：`rc_legacy` + RC + pool/probe 关闭。RM 必须显式配置 `runtime_profile=rm_legacy`、`trans_mode=rm`。
- 所有模式/身份/profile gate 在任何 native 资源创建前执行；非法组合为 `InvalidArgument`，配置合法但 provider/runtime gate 不满足为明确 `NotImplemented`/unsupported，绝不以 RC 或 bonding 静默兜底。
- 本阶段限定 RM/RTP + `URMA_JETTY`，只覆盖 CPU DRAM 单边 READ/WRITE；只允许受信 PHYSICAL，拒绝 BONDING/VIRTUAL/AGGREGATE/UNKNOWN。
- 沿用 endpoint-owned 固定 Jetty set 和现有 worker/poll 调度；一个 slice attempt 对应一个 native WR。adapter 保留通用 vector/`posted_count` 合约用于边界验证，但生产 worker 不组成多 WR chain。
- RM 不调用 `bindJetty`/`unbindJetty`；post 必须使用本次 WR 显式指定的 imported target，不保留永久绑定 remote Jetty。
- bootstrap v1 wire bytes/JSON 与 RC 行为不变；v2 是 RM 独立版本，禁止从 v1 默认值补齐缺字段。UASID 显式 0 合法与否由已选 provider contract 决定；编码上缺失和 0 必须可区分。
- READ 超时重试前必须证明旧 DMA 已结束；WRITE 的接受状态 Unknown 时不自动重放。逻辑失败不是资源安全完成。
- owner 只在 WR 完成或 provider fence 证明不再访问后释放；unimport、Jetty/JFC/Context delete 只能在依赖引用和完成责任清空后执行。
- 当前 `urma_adapter.cpp` real build 仍无条件包含 `urma_ubagg.h`；物理构建移除 BONDP 依赖是本阶段准出项。

### 1.5 风险及待确认项

| ID | 风险/问题 | 默认处理 | 关闭条件 |
|---|---|---|---|
| R1 | UMDK、liburma ABI、内核、驱动、固件版本未知；源码观察可能过时 | 不把未指定版本的“master”当作契约；未测记 NOT_RUN | Provider Owner 固定可复现 SHA/ABI/驱动/固件并给支持矩阵 |
| R2 | priority 到 RTP 的能力字段可能不完整或与运行硬件不符 | capability 查询和 `LocalSendProfile` 双重校验；不合格在 post 前拒绝 | 每种目标设备至少一个 priority 的 RM JFS/RTP 实际 WR 成功 |
| R3 | RM import target 和 RC bind 的关系/参数可能因 provider ABI 不同 | RM descriptor 独立 import；adapter 明确不调用 bind；未经验证拒绝启动 | 双端 RM target import、READ/WRITE 和调用计数证据 |
| R4 | post 返回与 CQE 竞态、partial accepted、返回值矛盾 | token/owner 先登记；Accepted 仍等 CQE；Unknown 不重放并保留 owner | fake/interposer 顺序测试及真实 provider 测试通过 |
| R5 | marker/flush/delete 的版本语义、CQE 归属、JFC 并发未知 | 同步 legacy fence 作为 P1 gate；不能证明 fence 则 quarantine、不 delete | B-03/B-04 对选定 provider 版本验证通过 |
| R6 | delete 失败后 native handle 是否还有效未知 | 默认 UNKNOWN，不解引用、不盲重试；保留预算/owner | provider 返回语义经错误注入和对象生命周期检查确认 |
| R7 | P1 target 最小 owner 与目标共享 target 架构边界不清 | 只实现 endpoint-incarnation 范围 owner；不推导跨 endpoint 去重/共享能力 | 架构 Owner 明确 P1 target 生存期、引用及失败回滚契约 |
| R8 | 用户报告 UT 全过但无本次可复现报告 | 记作作者报告通过，不提升到 CI PASS | 归档命令、SHA、编译参数、测试总数和日志，Linux CI 重跑 |
| R9 | 本轮选型若触及启动失败/回滚，需确认 TENT 是否允许尝试下一 transport | 本文 RM 本身 fail-closed；向上返回原始 unsupported/error，不自行重试 RC | Runtime/Store Owner 确认上层 fallback 既有策略及错误码映射 |

本设计分析已覆盖仓库可确定的事项；R1-R9 中 provider、架构和上层策略项仍需对应 Owner 关闭。本轮没有外部沟通渠道，故不能将这些项目标为已确认。

## 2. 详细设计

### 2.1 目标运行模型

```text
配置与 trusted identity
  → RM Legacy 安装前 gate
  → 设备/JFC/Context 初始化
  → endpoint 预备本地 RM Jetty set
  → bootstrap v2 交换 peer identity、generation 和 RM target descriptor
  → 接收端校验并原子安装 endpoint-incarnation 的 imported target 引用
  → 每个 slice attempt 选择 endpoint/target，预登记责任后提交一个 RM WR
  → poller 分发真实 CQE；正常成功结算，错误/timeout 进入 endpoint retire+同步 fence
  → 结清 WR 和 buffer owner 后执行 native 清理
```

RM Legacy 的 target owner 仅保证一条 endpoint incarnation 从 prepare/bootstrap 到 retire 的生命周期；不承诺 target 在多个 endpoint 或 probe 间共享，也不实现 pooled Registry。

### 2.2 模块边界与职责

| 模块 | 本阶段职责 | 本阶段不承担 |
|---|---|---|
| 配置与运行模式 (`params.h`, `ub_transport.cpp`) | 默认 RC；显式 RM Legacy 解析；mode/device/profile/provider gate；native 创建前选择/拒绝 | 运行时切换、pool/probe 开关语义扩展、自动 fallback |
| Device selection | trusted identity 与发现项一对一匹配；physical-only 筛选；openContext 前复核 EID/index/path/mode/capability | 自动证明硬件物理类型、bonding 管理 |
| Bootstrap/control plane | RC v1 原样兼容；RM v2 DTO 校验；生产 descriptor 生成/消费；trusted metadata 比较；代次 compare-and-commit | pooled topology、campaign、动态共享 registry |
| RM adapter | RM JFC/JFR/JFS/Jetty、target import、WR target 提交、CQE 映射、flush/quiesce/reset/delete；native API 与类型映射集中在此 | 调度策略、重试政策、池化和健康判断 |
| Endpoint/incarnation owner | 持有本地 Jetty set、每条 endpoint 所需 remote target import、代次与 outstanding；串行 prepare/bootstrap/retire | 跨 endpoint 的 LocalPortTarget/import 去重、Jetty lease 复用 |
| Worker/poller | 当前逐 slice attempt/路径/quota；每个 attempt 单 WR；token 先登记；poll 统一完成结算；超时调用同步 fence | 多 WR worker batch、异步删除线程、probe、静态补池 |
| Buffer manager | 按 path 注册/查找本地 segment、导入远端 segment；Inflight 强引用延长生命周期 | 新的 Store/staging owner API |
| 测试/证据 | fake、interposer、双端 provider、资源账本和结果归档 | 用 fake 冒充 DMA/provider 证明 |

### 2.3 上下游、输入与输出

| 边界 | 输入 | 输出/错误 |
|---|---|---|
| TENT Runtime → UB install | `runtime_profile`、`trans_mode`、`physical_only`、`device_identities`、现有 worker/JFC/slice 参数 | 安装成功进入 `RM_LEGACY_READY`；结构冲突 `InvalidArgument`；provider/模式未支持明确 unsupported/`NotImplemented` |
| Topology/provider discovery → gate | native name/path、EID/index、active、transport_modes、RM/RTP/JFC/WR capability | 唯一映射的 PHYSICAL DeviceInfo 和本地 profile；未知/陈旧/重复/mask 不符拒绝 |
| Endpoint → ControlService v2 | 本端 segment/NIC identity、node/context/topology/endpoint generation、target descriptor 和 profile | 对端 v2 reply；协议错、身份错、generation stale/conflict 时失败且 incumbent/native owner 不变 |
| Worker → adapter post | Jetty handle、单个 WorkRequest、local/remote segment、operation、target、address/length、token | submit status + accepted count/acceptance classification；accepted WR 后续必须有真实 CQE 或 fence 结算 |
| Poller/adapter → Worker | CQE category、token、native Jetty ID、completed bytes、实体 marker/AE | 完成门一次性更新 attempt、quota、outstanding、用户 callback；未知 token/marker 不能误结算业务 |
| Retire/shutdown → adapter | endpoint owner、关联 Jetty/JFC、未完成 token、timeout | 成功 fence 返回剩余 completion 并允许清理；失败返回 Draining/Unknown，资源和 buffer owner 保留 |

### 2.4 主要状态

#### 运行模式

```text
CONFIG_RC_DEFAULT → RC_LEGACY_READY
CONFIG_RM_LEGACY → GATE_CHECKING
  ├─ invalid identity/profile/config → REJECTED
  ├─ provider/API/ABI unsupported → UNSUPPORTED（无 native 分配）
  └─ all gates pass → RM_LEGACY_INITIALIZING → RM_LEGACY_READY
```

运行中的 RC/RM 不可切换。RM install 任一步失败时按已取得的 native dependency 逆序清理；清理未成功则保留 owner，install 返回失败且 transport 不得宣告 ready。

#### Endpoint incarnation

```text
UNINITIALIZED → PREPARING → PREPARED → HANDSHAKING → READY
      │             │          │            │
      └─────────────┴──────────┴──失败────→ FAILED/RETIRING
READY → RETIRING → FENCING → CLEANING → DESTROYED
                         └─ fence/delete 不确定 → DRAINING/QUARANTINED
```

每个 endpoint generation 一次性使用。FAILED/RETIRING incarnation 不原地 reconnect；重试新建 generation。RM Legacy 可保留固定 Jetty array，但状态/owner 必须整体归 endpoint incarnation。

#### WR 与结果

| 维度 | 状态/不变量 |
|---|---|
| 接受状态 | `NOT_ACCEPTED`、`ACCEPTED`、`UNKNOWN`。只有确定 NotAccepted 可在无其他 native 责任时回滚；Accepted 必须等 CQE/fence；Unknown 不重放并保留 token、buffer、segment 和 endpoint owner |
| 逻辑 attempt | `QUEUED→POSTING→POSTED→COMPLETED/FAILED/TIMEOUT/CANCELED`；旧 attempt/token 的迟到 CQE 不得完成新 attempt |
| 成功条件 | 成功 CQE 属于当前 token，provider completion status 成功，completed bytes 符合 API/provider 定义；只结算一次 |
| 错误资源条件 | retryable 业务结果与 ResourceSafe 分离；READ 重试需要旧 DMA fence，WRITE Unknown 禁止透明 replay |
| outstanding | 每实际接受 WR 一次登记/一次释放；确定未接受的 post 失败一次回滚；flush marker 不作为业务 WR 完成 |

### 2.5 关键设计决策

1. **模式名和门禁**：`rm_legacy` 表示明确 RM/RTP native profile、非池化 endpoint ownership；不存在“RM label 下仍跑 RC”的状态。结构性配置错返回 `InvalidArgument`，能力不支持在创建 native handle 前返回 unsupported。
2. **profile 来源**：LocalSendProfile 只从本地 provider capability 和配置 priority 派生；不接受远端指定本地 priority/bind mask。RemoteTargetProfile 只承载 `URMA_TM_RM`、`URMA_JETTY`、`URMA_RTP` 等 target 事实。
3. **target 身份**：v2 显式传 EID/UASID/ID、target identity/epoch、peer node/context/topology generation 和 endpoint/segment generation。Native UASID 的缺省语义必须在 schema 中可表达，不能缺失补零。
4. **v2 事务**：先解析和验证完整 candidate，再对照受信 segment/device metadata，最后在 endpoint owner 锁/事务边界内比较 generation 并提交；任何 validation/native 准备失败不推进 incumbent/watermark。Duplicate 幂等返回；Stale/Conflict 无副作用；Newer 只在 commit 成功后替换。
5. **P1 target ownership**：每 endpoint incarnation 有明确的本地 target 发布对象和远端 imported target 引用；本地 endpoint 删除不能越权释放仍有 WR 使用中的 segment/target。跨 endpoint 的去重和共享不在本阶段。
6. **Native 创建顺序**：Context → send/receive JFC/JFCE/JFR → segment registration/import → RM Jetty/endpoint → v2 ready。具体依赖以 UMDK contract 为准；回滚按逆序，部分成功返回的 handle 必须 adoption 到 owner 后清理。
7. **RM post**：每次 worker submit 一个 WR，目标句柄显式附在该 WR；不预先 bind。token、segments、quota、endpoint outstanding 在调用 native post 前建立 owner 记录，以处理 CQE 先于 post 返回。
8. **partial-post 契约**：当前 worker 的实际调用是 size=1；adapter 的 vector/posted_count 仍保留一般语义并用 fake/interposer 直接测 `N>1` 前缀，但不能把该 adapter 用例描述成 worker 批量行为。size=1 返回 `posted_count=0` 且 status 错误可确定回滚；status/count 矛盾或 provider 无法说明接受状态则按 Unknown/隔离处理。
9. **完成长度**：不要沿用“CQE success 即按 slice 全长成功”的假设。先冻结 provider 字节数语义：若 completion 明确报告长度，校验相等；若 API 对特定成功 WR 不报告有效长度，则 adapter 必须映射出经验证的成功长度语义并由测试覆盖，不能以零值误判为传输 0 bytes。
10. **同步错误 fence**：P1 沿用 endpoint quiesce 作为最小 legacy drain，不在本阶段引入异步 DeleteQueue。必须证明 modify ERROR/marker/flush 返回后无后续 DMA，再派发 flush completion、归还 owner，随后按已验收顺序 unimport/delete；仅当该 provider 的契约明确要求时才 reset，不能把 reset 作为通用前置步骤。fence 无法证明时状态保持 DRAINING/QUARANTINED，禁止释放依赖或 retry。
11. **删除三态**：API 返回失败不等于 handle retained。默认 `UNKNOWN`；只有 provider 证据允许重试才记 `HANDLE_RETAINED`；明确已消费才记 `HANDLE_CONSUMED`。Unknown 不解引用、不盲重试、不释放 native budget。
12. **错误映射**：参数/权限/profile 错误不是链路故障，不通过无限重建重试；本地设备/JFC 错误作用于对应 Context/endpoint；远端路径错误只按当前 endpoint/attempt 策略处理，不在 RM Legacy 引入新健康 scheduler。

### 2.6 异常处理与兼容要求

| 场景 | 动作 | 返回/资源策略 |
|---|---|---|
| RC 默认配置 | 使用既有 v1、RC JFS/JFR、bind 和 RC endpoint | 线格式、默认参数及 RC 测试不变 |
| `runtime_profile=rm_legacy` 但 `trans_mode=rc` | 配置 gate 拒绝 | `InvalidArgument`，native create count=0 |
| RM mode 但 trusted identity 缺失/冲突/陈旧 | install gate 拒绝 | unsupported/identity error；不得退到 bonding/RC |
| 设备不支持 RM 或没有 RTP priority | profile gate 拒绝 | native Jetty/create/post count=0 |
| Bootstrap v1 请求进入 RM endpoint | 协议版本不匹配 | 在 endpoint 建立/remote import 前拒绝 |
| v2 缺字段、未知扩展、错误 profile、零/格式错误 EID、target 重复 | validator 拒绝 | incumbent 不变、无 native 副作用 |
| v2 duplicate | 重用相同 endpoint incarnation | 不重复 create/import；返回幂等成功 |
| v2 stale/conflict | 拒绝 candidate | 不回滚/覆盖 newer endpoint，不改 generation watermark |
| 本端/对端 RM profile 不兼容 | native post 前拒绝 | 不计链路错误；target owner 保留或按构建阶段回滚 |
| post 明确未接受 | 撤销预登记、quota/outstanding/segment 引用恰好一次 | retry 作为新 attempt；不把旧 token 留在 map |
| CQE 早于 post return | 先记录完成，待 post call result 确认接受后完成 gate | 不提前释放/复用 endpoint；用户 callback 恰好一次 |
| post 返回错误但报告已接受 | 仍等待真实 CQE/fence | 不重发；status 作为诊断，不抹掉接受证据 |
| post accepted 状态不明 | retire/fence 或 quarantine | WRITE 不自动重放；保留实际 buffer 和 segment owner |
| 成功 CQE 长度异常/状态非法 | adapter/worker 标记 protocol/provider error | 不记全长成功；隔离当前 attempt/endpoint，留证 |
| Remote READ/WRITE permission denied | 返回参数/权限错误 | 不标物理链路坏、不无限重建 Jetty |
| timeout 且同步 quiesce 成功 | 分发 fence 返回的所有真实 completion，再安全结算并 retire endpoint | READ 可在 owner/fence 门后创建新 attempt；WRITE 仍按结果是否 Unknown 决定重试 |
| timeout/quiesce/marker 失败 | 留在 DRAINING/QUARANTINED | 不 delete/unimport、不释放 pin、不开始可能重叠的 WRITE |
| delete API 失败 | 根据已确认 provider contract 分 retained/consumed/unknown | 默认 unknown；不确定时零盲重试 |
| install 部分创建失败 | 逆序清理已取得资源 | 清理失败保留 retryable owner，install 失败且禁止 ready |
| shutdown 时仍有 accepted WR | 停止新 admission，正常 poll/fence，结清所有 owner | bounded shutdown；未证明 safe 时返回 Draining/Unknown，不强制析构 native handle |
| 进程崩溃/强杀 | 不声称进程内 fence 已执行 | 记录部署/provider 的驱动清理契约；结果标 Unknown，不把 crash 当成 clean shutdown |

### 2.7 关键场景与边界例

**场景 A：成功的单 WR WRITE**

1. `rm_legacy` mode 和 trusted identity gate 通过，发现 PHYSICAL device 支持 RM/RTP，派生合格 LocalSendProfile。
2. endpoint 创建固定本地 RM Jetty set；v2 peer target descriptor 校验完成后 import remote target，不调用 bind。
3. worker 选定当前 endpoint/path，取得 quota，持有 local/remote segment；插入唯一 token 的 Inflight 并预登记 endpoint/context outstanding。
4. adapter 用显式 target 提交一条 WRITE WR。假设 native 返回 accepted，但 CQE 在 post 函数返回前到达；poller 暂存/解析该 token，直到提交状态与真实成功 CQE 两个证据齐备后才结算。
5. 校验成功状态与 byte semantics，写完成一次，释放 outstanding/quota/segment owner，callback 一次。

**场景 B：size=1 post 明确未接受**

provider 返回 error、accepted count=0，且无早到 CQE。worker 原子移除 Inflight，撤销 endpoint/context outstanding 与 quota，释放 segment 引用一次。attempt 可按现有 retry policy 进入新的 attempt/token；此情形不记作 native accepted WR，也不触发重复 completion。

**场景 C：WRITE 接受状态 Unknown**

interposer 令 provider 在 native 接受后返回无法分类的错误。系统不得立即重发；保留目标 buffer、segment、endpoint owner，标记执行结果 Unknown 并尝试同步 fence。fence 成功后可确认旧 DMA 已结束，但不能据此推断远端写入是否执行完成；是否由上层请求语义允许重新执行必须独立判断，RM Legacy 不做不透明 replay。fence 失败则保留 quarantine owner。

**场景 D：新旧 bootstrap 并发**

先收到 generation 12，再迟到 generation 11。validator 解析二者；事务比较判定 11 为 stale，不能替换 12 的 peer identity/target，也不触发 import/unimport。相同 generation 但 descriptor 内容冲突则拒绝为 conflict；同一 descriptor 重试为 duplicate，不创建第二套 Jetty/import。

**场景 E：flush marker 后仍有软件 SQ 记录**

marker 是实体控制事件，不是 WR completion。worker/poller 不对 marker 的 `user_ctx` 做 token 查找；adapter 按当前 UMDK contract 调用 software flush，逐条返回未处理 WR 并通过正常 token dispatcher 结算。所有 accepted token 和 provider handler 退出前不 delete Jetty/JFC、不 unimport target。

**场景 F：shutdown 时一个 fence 失败**

transport 停止接受新 task；成功 drain 的 endpoint 正常清理，失败 endpoint 保留 context、JFC、Jetty、import、segments 与 Inflight owner，关闭返回 Draining/Unknown。不得为了进程内“清理完成”而清空容器或释放 pinned buffer。重复 close 继续操作同一 owner，不创建新 incarnation 覆盖旧资源。

### 2.8 RM Legacy 明确不实现

- SendJettyPool/LIFO lease、跨 endpoint Jetty 复用、静态 Serving/refill。
- 共享 LocalPortTarget/ImportedTargetRegistry 的全局去重和多 peer import 共享。
- 主动周期 ProbeResource、peer campaign、PathHealth 恢复状态机。
- pooled 单 attempt 完成门、目标 K 跨 Context gate、P3 异步 delete 线程。
- Runtime/Store 新 buffer owner API、灰度/正式上线指标策略。

这些能力不得作为 RM Legacy 的隐式依赖或阶段准出项；RM Legacy 为之后功能提供可验证的 RM native 基线。

## 3. 编码落地节奏

使用阶段门禁，目标日期由 Owner 在看板登记，不在设计中虚构人日。Phase L0 是当前已有的前置代码；L1-L5 构成 RM Legacy 最小闭环。Owner 决策未闭环时可继续不依赖它的 fake/validator 工作，但不得越过相应 native/hardware gate。

| 阶段 | 优先级 | 任务拆分与交付 | 依赖/准入 | 里程碑与验收口径 |
|---|---|---|---|---|
| L0：前置代码与证据归档 | P0 | DeviceKind/identity/profile、v2 DTO/validator/generation helper、安装前 gate；归档用户报告的自测 UT | 当前 `e1d39c4d` | 自测 UT 作为作者报告记录；补命令、SHA、环境和日志；Linux CI 运行 selector/core/native data path/teardown 四目标；默认 RC 与 RC v1 回归通过。未归档前保持验证状态，不称 RM Legacy ready |
| L1：接口/模式契约冻结 | P0 | 固定 mode/error matrix、RM/RTP profile、target DTO 最小字段、接受状态映射、资源 owner 与 teardown 顺序；提出 B-01/B-03/B-04/R7/R9 决策 | L0；架构 Owner 和 Provider Owner 参与 | 决策表中每个字段有 producer、validator、消费者和错误行为；选定 UMDK SHA/ABI/driver/firmware；`urma_ubagg.h` 物理编译依赖处置方案明确 |
| L2：RM capability gate 与 native adapter | P1 | 实现 RM JFC/JFR/JFS/Jetty profile；target import；RM WR 显式 target；CQE/accepted mapping；移除 RM bind/unbind；native create 失败逆序回滚 | L1 provider 支持矩阵；P0 CI 可复现 | Fake 精确断言 RM 全链路参数、RC-only API 调用次数为 0、priority/tp_type 错时 post=0；TENT_HAS_REAL_URMA 构建通过；不合格设备 native handle 创建数为 0 |
| L3：RM bootstrap v2 事务与 endpoint owner | P1 | v2 双端 producer/consumer；受信 metadata 校验；generation compare-and-commit；endpoint-incarnation target/import owner；bootstrap 失败回滚 | L2；控制面 schema 冻结 | golden v1 不变；RM v2 合法双端握手成功；缺字段/错 mode/错 identity/stale/conflict 在 target/post 副作用前拒绝；duplicate 不重复创建/import；乱序 candidate 不覆盖 newer |
| L4：单 WR 数据面与正确性回归 | P1 | worker 对每 slice 单 WR；accepted/token/early CQE 完成门；字节数与错误映射；partial contract 独立 adapter 测试；READ/WRITE retry 责任 | L2、L3；Runtime现有 buffer/segment owner 可用 | fake/interposer 覆盖 NotAccepted/Accepted/Unknown、早到/重复 CQE、size=1 count/status 矛盾、adapter N-WR partial prefix、Read/Write 方向和 permission；所有 token/outstanding/quota 恰好释放一次 |
| L5：真实 provider drain/关闭与 RM Legacy 验收 | P1 | 双端物理 RM READ/WRITE；error/timeout/flush/reset/unimport/delete/shutdown；install/uninstall soak；结果归档 | L4；B-01/B-03/B-04 关闭；两台 Linux 实机和可注入 interposer | 真实 provider 强制用例全部 PASS、NOT_RUN=0；READ/WRITE 数据与完成长度正确；partial/early 行为符合契约；未知接受状态不重放；100 次带载 install/uninstall 无 hang/泄漏；30 分钟 soak 错误/hang=0、资源回基线或每个残留均有 owner/解释 |

### 3.1 优先级与并行关系

- **P0 阻塞**：L0 CI/证据与 L1 provider/API 决策。可并行完成 fake case、identity/bootstrap 负向矩阵、interposer 设计和测试结果归档模板。
- **P1 核心实现**：L2 native adapter 先于 L3 真实 v2 install 消费；二者可并行做接口 stub/schema 测试，但不得合并生产接线直到边界冻结。
- **P1 数据面**：L4 依赖 L2/L3；RC v1 golden 与 RC native regression 可并行维护。
- **P1 验收**：L5 依赖所有前序实现及实机资源；硬件 unavailable 记 NOT_RUN，不以 fake 替代。
- **禁止越级**：L5 未通过前 RM Legacy 不作为可支持运行模式；RM Legacy 未通过前不得开始以它为基线的 Jetty pooling 验收。

### 3.2 全阶段共用验收规则

1. 每个阶段交付 SHA、构建 flag、依赖/硬件版本、测试命令、通过数、失败数、NOT_RUN 清单和日志链接。
2. fake 验证控制流和调用契约；interposer 验证 native 接受/返回/CQE 时序；真实 provider 验证 URMA ABI 与 DMA fence。三类证据独立记录。
3. 模式负向用例必须断言 native create/import/post 调用计数为 0；不能只断言错误码。
4. 任何 DMA/handle 归属 Unknown、数据 mismatch、completion 丢重、unbounded owner、hang 都阻止当前阶段准出。
5. 只在全部 L5 必需项通过后将 `rm_legacy` 从 unsupported gate 提升为可用；保持 RC 默认且不引入运行时 native mode 切换。

## 关联资料

- 基准设计：[supercache_jetty_pool_current_and_target_rm_concise.md](supercache_jetty_pool_current_and_target_rm_concise.md)
- 阶段执行清单：[tent_rm_p0_p5_execution_checklist.md](tent_rm_p0_p5_execution_checklist.md)
- Provider/Owner 决策登记：[supercache_jetty_pool_blocking_decision_register.md](supercache_jetty_pool_blocking_decision_register.md)
- 当前源码依据：`mooncake-transfer-engine/tent/{include,src}/transport/ub/` 与对应 `tent/tests/*ub*`。
