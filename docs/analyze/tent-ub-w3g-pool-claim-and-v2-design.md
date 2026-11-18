# TENT UB W3g 详细设计：池化侧 claim、bootstrap v2 字段加法与物理口身份导入

> 日期：2026-11-18。定位：**W3 之后的三项增量切片**（池化侧合并结论的落地）。
> 上游文档（冲突时以上游为准）：父文档《tent-ub-post-poll-threadpool-design》（下称**总设计**），关键条款 §5.5（claim 权威在池化侧）、§7.4（完成门）、§9.4（探测区 descriptor 由 bootstrap v2 显式发布）、§11（接口 1/4/5）；W2 详设（完成门）、W3 详设（post 路径，R-1/R-3）、探测详设（§15 v2 字段与回退策略）。
> 代码基线：`757fbf00`（W3 + W3f 池化后台执行器）。本文行号均对该基线。
> 状态：**已实现（2026-11-18）**。W3g-a–W3g-e 全部落地，实现记录与偏差见 §12.1；两项受限项（探测懒激活、physical-only 解析）在 §12.1 明确登记。三项仍各自可独立回退。
> 阅读顺序建议：§0（决策）→ §2（心智模型）→ §6/§7/§8（三项设计）→ §12（切片）。

## 0. 已确认决策与锁定默认

池化侧（`930/Mooncake@ef9ba164`）与我们（`probe` / `tent/ub_rm_jetty`）是两套实现。经过对比，结论是：**保留本侧三件套**（`CompletionRecord` 完成门 / Task 级 K / RM 逐 WR target），**只吸收两项能力**（池化侧 claim、bootstrap v2 字段）+ 一项装配增强（物理口身份导入）。

| 编号 | 决策 | 锁定默认 | 依据 |
| --- | --- | --- | --- |
| D-1 | 本文只做三件事：**R-1 池化侧 claim**、**bootstrap v2 字段加法**、**物理口身份导入**。不替换完成门、不改 K 归属、不改 RM 逐 WR target | 是 | 总设计 §5.5/§11；W3 D-6；三件套已冻结 |
| D-2 | claim 的权威判定落在**池的同步域**内；本侧保留 epoch 快照采样，但只作为**输入**，不再作为唯一判据 | 是 | 总设计 §5.5："是否允许 post 的权威判定在池化侧 claim" |
| D-3 | claim 只回答"**允不允许发**"，不产生结算、不释放 owner；结算仍由 W2 完成门唯一负责 | 是 | 否则 W2 的"一次性认领"被架空，接口 5 失效 |
| D-4 | claim 的判据只用池内可原子读取的状态：`stopping_`、成员状态与代次、成员 `error_epoch`、租约归属。**不得在池锁内回调 PLM/Quota** | 是 | 锁顺序：本侧锁不得与池锁嵌套（总设计 §4）；PLM 的 epoch 以标量形式在**取样时**读好 |
| D-5 | claim 失败 = **原子回滚**：不消耗 retry、不记路径故障、不判 slice 失败；按"路径不可用"短定时 defer | 是 | W3 §8.4 逆序回滚表；W3-IN-3 |
| D-6 | bootstrap v2 采用**纯字段加法**：v1 字段与语义全部保留，v2 只新增可选对象；对端未声明 v2 时按 v1 处理，**探测在没有 v2 字段时保持关闭而非降级猜测** | 是 | 探测详设 §15（v2 字段与 v1 兼容/回退策略）；总设计 §9.4 "不猜地址" |
| D-7 | v2 的线上协商用 `capabilities` 能力串 + `schema_version`；未知字段**忽略而不拒绝**（前向兼容），但**类型非法必须拒绝**（fail-closed） | 是 | 接口 4 的"enabled 且供给不全 → 安装失败" |
| D-8 | 物理口身份导入只服务 **RM physical-only 装配**：把 `(native_device_path, eid)` 归一成稳定 `device_key`，并允许对端未发布完整拓扑时按导入身份建立 target | 是；`rc_legacy` / `rm_legacy` 路径完全不动 | 池化详设 P0-D；W3 §3.2 的 identity 缺口 |
| D-9 | 三项各有独立开关与降级路径：claim 可用 `transports/ub/claim/enabled=false` 退回今日的"本侧 epoch 复核"；v2 用 `bootstrap/protocol=auto|v1|v2`；identity 用 `identity/import=off|on`（默认 on） | 是 | 灰度和真机回退需要 |
| D-10 | 所有新增字段/接口默认值必须让**今天的行为逐位一致**；任何行为差异必须能在日志里用一行摘要区分 | 是 | 与 W2/W3/W4 的兼容纪律一致 |

## 1. 范围与不做什么

### 1.1 做什么

| 项 | 内容 |
| --- | --- |
| R-1 | `SendJettyPool::tryClaimPost()`：在池同步域内复核 stopping / 成员 / 代次 / `error_epoch`，返回 `Claimed` 或 `GateClosed(原因)`；workers 用它替换 `pool->stopping()` 的两行复核 |
| v2 字段 | `UbBootstrapDesc` 增加 `schema_version`、`capabilities` 扩展、远端 target profile、探测区 descriptor（address/length/token/epoch）、`topology_epoch`/`target_epoch`；`ProbePeerDescriptorSource` 的正式来源（bootstrap）落地 |
| identity | `makePhysicalDeviceIdentity(native_device_path, eid)` + 导入缓存；physical-only RM 的对端 target 按导入身份建立 |
| 观测 | claim 拒绝分因、v2/v1 对端计数、未知字段拒绝、identity 导入成功/失败 |
| 测试 | 池 claim 单测、fake 集成、双端兼容矩阵（v1×v1 / v1×v2 / v2×v2）、真机闸门 |

### 1.2 不做什么（明确拒绝）

| 不做 | 归属 |
| --- | --- |
| 引入 `AttemptGate` 作为第二套完成门 | 永不；W2 门是唯一结算者（D-3） |
| 进程级 `TaskPermitLedger` | 永不；W3 D-6/RK3 已判为正确性缺陷 |
| 用 v2 替换 v1 契约（`bindRm` 全量协议） | 不做；只加字段（D-6） |
| 探测调度/健康门禁语义调整 | 已冻结（W1） |
| 软件 flush 提取服务（接口 10）、关闭顺序重排 | W6 |
| 超时结构重构（删同步 quiesce） | W5 |

## 2. 心智模型

### 2.1 一次 attempt 的九个时刻（claim 插在第 ⑧ 步之前）

```text
① 路由解析        → 本地设备 × 对端设备
② 候选生成        → 边 = (本地口, 对端, 对端口)
③ 健康门禁        → 只做减法（PLM）
④ 打分排序        → NUMA/负载/带宽/重试轮转
⑤ 端点解析        → 懒建 + bootstrap（本文 §7 扩字段）
⑥ 准入            → quota / outstanding / Task K
⑦ 租借            → 池成员 + JFC credit（PoolKey 分桶）
   ── 采样 fault_epoch（标量）──
⑧ claim（本文 R-1）→ 池同步域内复核：stopping / 成员 / 代次 / error_epoch
                     ├─ Claimed   → ⑨
                     └─ GateClosed → 原子回滚（不消费 retry，短定时 defer）
⑨ 发布记录 + post  → RM 逐 WR target（不变）
```

**claim 要补的正是 ⑦→⑨ 之间那一段窗口**：今天本侧只复核"池是否 stopping + PLM epoch 是否变化"，而"这个成员在租出去之后是否已被退役/代次已变"只有池自己知道。

### 2.2 v2 是"加法"，不是"换协议"

```text
v1 描述符（今天）: protocol_version, segment_name, nic paths, device id/eid,
                   jetty_ids[], jetty_uasids[], endpoint/segment_generation,
                   capabilities[]
v2 追加（可选对象）: schema_version, target_profiles{trans_mode,target_type,tp_type},
                   probe_area{descriptor,address,length,token,epoch},
                   topology_epoch, target_epoch
```

- 对端 `capabilities` 不含 `bootstrap_v2` → 本侧只发 v1 字段，行为与今天一致；
- 对端声明 v2 但本侧 `protocol=v1` → 只发 v1；
- 任一方缺 `probe_area` → **探测保持关闭**，业务不受影响（D-6）。

### 2.3 identity 导入解决的是"对端没告诉我完整拓扑"

physical-only RM 下，对端可能只发布 `(eid, jetty id, uasid)` 而未发布设备拓扑。此时按"名字+路径+eid"归一出的稳定身份建立 target；身份不可得时 endpoint 不 ready（fail-closed），绝不按"相近设备"猜。

## 3. 现状基线与差距

### 3.1 现状（代码事实，基线 `757fbf00`）

| 位置 | 现状 |
| --- | --- |
| [workers.cpp#L1355](../../mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L1355) `claimPost()` | 两支标量判断：`pool->stopping()` 与 `probe_manager_->businessFaultEpoch(path) != snapshot`；无锁、不阻塞 |
| `SendJettyPool` | 有 `mutex_` 保护的 `buckets_`、`stopping_`、成员状态与代次；**没有** claim 接口 |
| `PoolMember` | 有 `lane{jetty_id, generation}`、`state`、`retire_reason`；**没有** `error_epoch` |
| `UbBootstrapDesc`（[control_plane.h#L65](../../mooncake-transfer-engine/tent/include/tent/runtime/control_plane.h#L65)） | v1 字段齐全（含 `jetty_ids`/`jetty_uasids`/`capabilities`），**无** v2 字段 |
| `ProbePeerDescriptorSource` | 只有测试实现；生产 `UbTransport` 构造时未注入（探测因此装不上） |
| 物理口身份 | budget `device_key` 由 `native_device_name:path:eid` 直接拼；无归一化/导入 |

### 3.2 差距与影响

| 差距 | 影响 | 本文对应 |
| --- | --- | --- |
| claim 只在本侧复核 | 成员在"租出→post"之间被退役/换代时，可能向一个已退役成员发 WR（窗口内泄漏上界=每 posting 线程 1 次） | §6 |
| 无 v2 字段 | 探测区无法显式发布 → W1 真机不可用；对端 target profile 无法协商 | §7 |
| 无身份归一 | physical-only RM 在对端拓扑不完整时 endpoint 不可用；budget 键跨重启不稳定 | §8 |

### 3.3 对生产的可观察变化

三项都默认关闭/兼容：claim 默认开启但行为只在"成员被退役"这一竞态下更严格；v2 默认 `auto`，对端不声明则逐位不变；identity 默认 on，只影响 physical-only 装配。**不改变结算、不改变重试策略、不改变探测语义。**

## 4. 模块与文件

| 文件 | 动作 | 内容 | 规模 |
| --- | --- | --- | --- |
| `include/tent/transport/ub/send_jetty_pool.h` | 修改 | `ClaimVerdict`/`ClaimRequest`、`tryClaimPost()`、`PoolMember::error_epoch`、拒绝计数 | +80 |
| `src/transport/ub/send_jetty_pool.cpp` | 修改 | claim 实现（池锁内复核）、退役时 bump `error_epoch` | +70 |
| `src/transport/ub/workers.cpp` | 修改 | `claimPost()` 改调池 claim，保留 PLM 快照作为输入 | +40 |
| `include/tent/runtime/control_plane.h` | 修改 | `UbBootstrapDesc` 追加 v2 字段与 `to_json`/`from_json` | +90 |
| `include/tent/transport/ub/bootstrap_v2.h` | 新增 | `UbRemoteTargetProfile`、`UbProbeAreaDescriptor`、能力串常量 | ~120 |
| `src/transport/ub/endpoint.cpp` | 修改 | v2 字段解析、`rmTarget()` 支持 profile、探测区发布 | +80 |
| `include/tent/transport/ub/identity_import.h` | 新增 | `makePhysicalDeviceIdentity`、导入缓存 | ~100 |
| `src/transport/ub/ub_transport.cpp` | 修改 | 注入 `ProbePeerDescriptorSource`（bootstrap 实现）、identity 装配 | +60 |
| `include/tent/transport/ub/params.h` | 修改 | `claim/enabled`、`bootstrap/protocol`、`identity/import` | +25 |
| `tests/ub_pool_claim_test.cpp` | 新增 | claim 并发/退役/换代/stopping 用例 | ~250 |
| `tests/ub_bootstrap_v2_test.cpp` | 新增 | 字段序列化、能力协商、v1 回退 | ~250 |
| `tests/ub_identity_import_test.cpp` | 新增 | 归一化、去重、失败 | ~150 |
| `tests/ub_pool_integration_test.cpp` | 修改 | claim 拒绝回滚、v2 双端矩阵 | +150 |

## 5. 数据模型

### 5.1 claim（R-1）

```cpp
enum class ClaimClosed : uint8_t {
    kNone = 0,
    kPoolStopping,     // 池已 beginStop
    kMemberRetired,    // 成员已退役/隔离/删除中
    kGenerationMoved,  // 成员代次与租约不一致（池内重建过）
    kErrorEpochMoved,  // 成员 error_epoch 已被抬高（退役/故障）
};

struct ClaimRequest {
    AttemptKey attempt;          // 租约身份（成员性质同 W3）
    uint64_t lane_generation;    // 租约中记录的成员代次
    uint64_t error_epoch_snapshot;  // 租借时快照，池内比较
};

struct ClaimVerdict {
    ClaimClosed closed{ClaimClosed::kNone};
    bool claimed() const noexcept { return closed == ClaimClosed::kNone; }
};
```

约束：

- `tryClaimPost()` **不产生副作用**（不改变成员状态、不租不还）；它只回答"能不能发"。
- 拒绝后的回滚由 workers 按 W3 §8.4 逆序执行（credit → 成员 → outstanding → quota → K → defer）。
- `error_epoch` 由池在**每次** `retireMemberLocked()` 时原子递增；租约携带租借时的快照值。

### 5.2 bootstrap v2 字段

```cpp
struct UbRemoteTargetProfile {
    uint16_t trans_mode{0};   // 镜像 urma_transport_mode_t
    uint8_t  target_type{0};  // 镜像 urma_target_type_t
    uint8_t  tp_type{0};      // 镜像 urma_tp_type_t
};

struct UbProbeAreaDescriptor {
    std::string descriptor;   // 对端探测页的 SegmentDescriptor（不透明字节）
    uint64_t address{0};
    uint64_t length{0};
    uint32_t token{0};
    uint64_t area_epoch{0};
    uint32_t uasid{0};
    uint32_t jetty_id{0};     // 对端 probe jetty
};

struct UbBootstrapDesc {
    // ...v1 字段原样保留...
    uint32_t schema_version{1};                    // v2 对端填 2
    std::vector<UbRemoteTargetProfile> target_profiles;  // 与 jetty_ids 等长
    std::vector<UbProbeAreaDescriptor> probe_areas;      // 可空
    uint64_t topology_epoch{0};
    uint64_t target_epoch{0};
};
```

规则：

1. **长度必须对齐**：`target_profiles` 非空时必须与 `jetty_ids` 等长，否则拒绝（fail-closed）。
2. **未知字段忽略**、**非法类型拒绝**（D-7）。
3. `probe_areas` 为空或与对端已发布端口不匹配 → 该对端探测保持关闭，并在日志中计数。

### 5.3 物理口身份

```cpp
struct PhysicalDeviceIdentity {
    std::string native_device_path;  // /dev/uburma/...
    std::string eid;                 // eidN（非空）
    std::string device_key;          // 归一化键：path + ":" + eid
};
[[nodiscard]] PhysicalDeviceIdentity makePhysicalDeviceIdentity(
    const std::string& native_device_path, const std::string& eid);
```

- `device_key` 是 budget 账本键、也是探测图 edge 的稳定性依据；
- 归一化规则：路径去重、eid 统一小写、拒绝空 eid（空 eid = 装配失败，不猜）。

## 6. R-1：池化侧 claim

### 6.1 接口

```cpp
// 池同步域内的一次只读复核。不阻塞、不回调外部组件（D-4）。
[[nodiscard]] ClaimVerdict SendJettyPool::tryClaimPost(
    const PoolKey& pool_key, const AttemptKey& attempt,
    const ClaimRequest& request) const;
```

实现要点：

- 一次 `std::lock_guard(mutex_)` 内完成全部判据，判完立刻返回（不跨 provider 调用）；
- 判据顺序固定：`stopping_` → `buckets_[pool_key]` 存在 → 成员状态 ∈ {kInUse} → `lane.generation == request.lane_generation` → `member->error_epoch == request.error_epoch_snapshot`；
- 任一不满足即返回对应 `ClaimClosed`。
- **可达性说明（复审 2026-11-18）**：当前"单租约独占"模型下只有 `kPoolStopping` 有真实触发路径（成员在租借期内只能由持有者自己退役）。`kMemberRetired`/`kGenerationMoved`/`kErrorEpochMoved` 是**防御性断言**：为未来的多 owner/后台退役模型保留完整判定，并用单测锁定语义；它们不代表当前存在该竞态，也不应为它们增加额外的锁。

### 6.2 workers 侧接线

```text
⑦ 租借 → 采样 fault_epoch（PLM，锁外）→ 采样 error_epoch（池内随租约返回）
⑧ claim:
     if (!params_.claim.enabled)        → 退化为今天的 claimPost()（D-9 回退）
     else if (!pool)                     → legacy 路径不做池 claim
     else verdict = pool->tryClaimPost(...)
          拒绝 → 逆序回滚 + 计数 + 1ms 定时 defer（kPathUnavailable）
⑨ 发布 + post（不变）
```

`PoolMember` 的 `error_epoch` 在租约返回时一并带出（`LeaseWithCredit.error_epoch`），避免再取一次锁。

### 6.3 与今天实现的差异（以及为什么更符合设计）

| 项 | 今天 | R-1 之后 |
| --- | --- | --- |
| 复核位置 | 本侧两行标量判断 | 池同步域内，与退役/重建同锁 |
| 覆盖"成员被退役" | 不能 | 能（成员状态 + error_epoch） |
| 覆盖"成员重建换代" | 不能 | 能（lane generation） |
| 结算权威 | 完成门 | 不变（D-3） |

### 6.4 拒绝后的回滚（沿用 W3 §8.4，顺序固定）

```text
credit 释放 → 成员 returnOnce(gate_passed=true) → endpoint outstanding
→ quota 释放 → Task K 释放 → slice deferAttempt → 1ms 定时 defer
```

不消费 retry、不记路径故障、不判失败。

## 7. bootstrap v2：字段加法

### 7.1 协商

```text
本端 capabilities 追加: "bootstrap_v2"、"probe_area_v1"、"target_profile_v1"
对端 capabilities 含 "bootstrap_v2" 且 bootstrap/protocol != v1 → 发 v2 字段
否则只发 v1 字段；对端发来的未知能力串忽略
```

`schema_version`：本侧发 1 或 2；对端 `schema_version>2` 时按"未知字段忽略"处理，但若缺少 v1 必需字段则拒绝。

### 7.2 探测区的正式来源

- `UbTransport` 构造时**注入 bootstrap 驱动的 `ProbePeerDescriptorSource`**（今天只有测试实现，这是 W1 真机不可用的根因）；
- 该 source 把对端 `probe_areas` 转成 `ProbePeerDescriptor`（含 area descriptor/length/token/epoch、remote jetty、三个 gate 位、endpoint_generation）；
- **没有 `probe_area` 的对端不产生 edge**（不猜地址），探测对这些 peer 保持关闭并计数。

### 7.3 兼容矩阵

| 本端 | 对端 | 结果 |
|---|---|---|
| v1 | v1 | 与今天逐位一致；无探测 |
| v1 | v2 | 只发 v1；对端 v2 字段被忽略；无探测 |
| v2 | v1 | 只发 v1 字段；无探测 |
| v2 | v2 | 全字段；探测在该 peer 上可用 |

`target_profiles` 缺失时 `rmTarget()` 退回今天的行为（`(eid, id, uasid)`）。

## 8. 物理口身份导入

### 8.1 用途与时机

1. **budget 键**：`device_key = makePhysicalDeviceIdentity(path, eid).device_key`（今天用 `name:path:eid`，跨重启/别名不稳定）；
2. **physical-only RM 装配**：对端未发布完整拓扑时，按对端 `local_device_name/local_eid` + 导入的 target 建立 endpoint，状态仍要求 ready 才可用；
3. **探测 filter**：`probe/device_filter` 的 `native:eidN` 形式复用同一归一化函数，避免两套解析。

### 8.2 失败处理

| 场景 | 行为 |
|---|---|
| eid 为空 / 路径为空 | 装配失败（fail-closed），计 `identity_import_fail` |
| 同一 `(path,eid)` 归一化结果冲突 | 拒绝并记录，不覆盖已缓存身份 |
| 对端声明 v2 但身份不可解析 | endpoint 保持未 ready，业务侧表现为该 peer 不可用（不猜） |

## 9. 配置与观测

### 9.1 配置键

| 键 | 默认 | 语义 |
|---|---|---|
| `transports/ub/claim/enabled` | true | false 退回今天的本侧 epoch 复核 |
| `transports/ub/bootstrap/protocol` | auto | `auto\|v1\|v2`；v1 强制不发 v2 字段 |
| `transports/ub/identity/import` | on | off 时用今天的 `name:path:eid` 键 |

交叉校验：`protocol=v2` 且对端全为 v1 时只告警不失败；`claim/enabled=false` 时计 `claim_disabled`。

### 9.2 指标

| 指标 | 含义 | 期望 |
|---|---|---|
| `claim_rejected_stopping` / `_member` / `_generation` / `_error_epoch` | claim 拒绝分因 | 只在竞态/停机时非零 |
| `claim_disabled` | 走回退路径的次数 | 配置关闭时等于 lease 次数 |
| `bootstrap_v2_peers` / `v1_peers` | 对端协商结果 | 稳定 |
| `bootstrap_rejected_profile` | profile 长度/类型非法 | 恒 0 |
| `probe_area_missing` | 对端无 probe area 的 edge 数 | 未开探测时为 0 |
| `identity_import_ok` / `_fail` | 导入结果 | fail 恒 0 |

## 10. 不变量与异常矩阵

| # | 不变量 |
|---|---|
| IN-1 | claim 只读、无副作用、不跨 provider 调用 |
| IN-2 | claim 拒绝后资源全量回滚，且不消费 retry |
| IN-3 | 结算唯一入口仍是完成门（claim 不结算） |
| IN-4 | v2 缺字段时退回 v1 行为，绝不猜地址/身份 |
| IN-5 | `target_profiles` 非空时必须与 `jetty_ids` 等长 |
| IN-6 | 身份归一化幂等；空 eid 一律拒绝 |

| 异常 | 必须证明 |
|---|---|
| 租借后成员被退役 | claim 拒绝、回滚完整、无 WR 发出 |
| 租借后池 beginStop | 同上，且计 `claim_rejected_stopping` |
| 对端只支持 v1 | 全链路正常，探测关闭 |
| 对端 v2 缺 probe_area | 业务正常，探测关闭并计数 |
| 身份导入失败 | endpoint 不 ready，不发出任何 WR |

## 11. 测试矩阵

| 层 | 用例 |
|---|---|
| 池单测 | claim 四类拒绝、并发退役、代次推进、stopping 交错、claim 只读（状态不变） |
| 集成（fake） | claim 拒绝 → 回滚不变量（credit/member/quota/K 全回）、无 retry 消耗 |
| v2 单测 | 序列化往返、未知字段忽略、非法类型拒绝、profile 长度不齐拒绝、能力协商矩阵 |
| v2 集成 | v1×v1、v1×v2、v2×v2 三种握手；探测图在缺 area 时不产生 edge |
| identity | 归一化幂等、冲突拒绝、空 eid 拒绝、budget 键稳定 |
| 真机 | install + 双端 v2 握手 + 探测区发布（需 node2/node4 与真实 URMA） |

回归门槛：`ctest -R "^tent_"` 全绿；`one_shot_violations` / `handler_ref_leak_on_finalize` / `double_return` / `leaked_leases` 恒 0。

## 12. 实施切分

| 片 | 内容 | 落点 | 验收 | 依赖 |
|---|---|---|---|---|
| **W3g-a（已实现）** | R-1 claim（池接口 + workers 接线 + 回退开关） | `send_jetty_pool.*`、`workers.cpp`、`params.h` | claim 单测 + 回滚集成；`claim/enabled=false` 逐位回退 | 无 |
| **W3g-b（已实现）** | bootstrap v2 字段 + 能力协商 + v1 回退 | `control_plane.h`、`bootstrap_v2.h`、`endpoint.cpp` | 兼容矩阵单测/集成 | 无 |
| **W3g-c（已实现）** | 探测区 descriptor 的正式来源（bootstrap → `ProbePeerDescriptorSource`），含安装后懒激活 | `ub_transport.cpp`、`bootstrap_peer_source.*`、`probe_provider_standalone.*` | 缺 area 不建 edge；安装时无 peer 武装等待，首个 area 后 `kEdgeAdded` 激活且 `probe_scheduled ≥ 1`（本机闭环）。**真机探测启用仍待 E 阶段**（`probe/enabled` 默认关闭 + provider 版本门槛，同 W1 RK11） | W3g-b |
| **W3g-d（已实现）** | 物理口身份导入 + budget 键归一 | `identity_import.*`、`ub_transport.cpp` | 归一化/失败用例 | 无 |
| **W3g-e（已实现）** | 观测、日志、文档回写 | `workers.cpp`、本文 §15 | 摘要行 + 上游回写 | A–D |

顺序建议：a → b → c → d → e；a 与 d 可并行，b 与 c 串行。

### 12.1 落地记录与偏差（2026-11-18）

代码基线 `757fbf00`（W3 + W3f）。新增文件：`bootstrap_v2.h`、`bootstrap_peer_source.h/.cpp`、
`identity_import.h/.cpp`；修改 `send_jetty_pool.*`、`workers.*`、`control_plane.h`、`endpoint.*`、
`ub_transport.cpp`、`params.h`、`urma_adapter.h`。新增测试目标 `tent_ub_bootstrap_v2_test`、
`tent_ub_bootstrap_peer_source_test`、`tent_ub_identity_import_test`（claim 用例并入既有
`tent_ub_post_claim_test` / `tent_ub_pool_integration_test`）。`ctest -R "tent_|ub_"` 79/79。

| # | 本文写法 | 落地写法 | 理由 |
| --- | --- | --- | --- |
| 1 | `ClaimRequest{attempt, lane_generation, error_epoch_snapshot}` | 另加 `const PoolMember* member` | 池没有 in-use 索引（成员只由 lease 持有），传成员指针让 claim 保持 O(1) 且无副作用（IN-1）；生成代次仍按 `lane_generation` 复核 |
| 2 | claim 拒绝计数在池头文件 | 计数放在 `UbWorkers`（`claim_rejected_*` / `claim_disabled`） | `tryClaimPost()` 必须是纯读（IN-1）；拒绝是调用方事件，计数随调用方 |
| 3 | `UbProbeAreaDescriptor.descriptor` 为不透明 string | 用结构化 `SegmentDescriptor` 并在 JSON 中展开 | 目录导入需要 schema/API/ABI 校验；字符串会让"非法类型拒绝"退化成不透明字段 |
| 4 | v2 对端 `schema_version>2` 按未知字段忽略 | 仅当 `schema_version == 2` 解析 v2 块；`>2` 只解析 v1 必需字段 | 前向兼容与 fail-closed 的折中；未知版本不猜测数组语义 |
| 5 | `probe_areas` 每对端 "可空" | 一次 bootstrap 信封必须携带**恰好 0 或 1** 个 area：0 → 无 edge + 计数；>1 → 拒绝 | 一个信封描述一个对端端口；多 area 无法确定端口映射，猜即违反 IN-4 |
| 6 | §7.2 注入 bootstrap source 后"真机探测可启用" | **已落地（第二轮）**：安装时无对端也保持 provider/directory/PLM/scheduler/probe 槽武装（PLM 允许显式空边），首个业务 bootstrap 带来 `probe_area` 后，`refreshRemoteEdges()` 以接口 7 `kEdgeAdded`（同 path 换代用 `kTargetEpochChanged`/`kAreaReplaced`）挂上 edge 并唤醒调度器；无 area 的对端仍不建 edge | 注入空图（测试/池化侧供给 bug）仍保持 fail-fast；只有 bootstrap 回退允许空图 |
| 7 | §8 physical-only RM 按对端 `local_device_name/local_eid` 建 target | 身份归一化落地（budget 键 + v2 身份/gate 校验）；**未做**"对端拓扑缺失时按导入身建立 target" | 对端拓扑缺失时 segment 元数据里没有任何可路由的 UB NIC 名，唯一可用输入是无从验证的自述 EID；按 D-8/IN-4 不猜 |
| 8 | `samePeer` 只比 identity | 保持 identity 比较；Ready 状态下的幂等重绑**刷新 v2 元数据**而不是报"另一代" | 集成测试暴露：v1→v2 的二次握手曾被判成 peer generation 冲突，导致自读写全链路 FAILED |
| 9 | `bootstrap/protocol=v1` 只约束发送 | **接收侧同等生效**：`UbEndpoint::bind()` 在开关关闭时丢弃对端 v2 块（再计 `bootstrap_v2_ignored`），transport 侧也做同一净化以维护全局计数 | 回退开关必须是本地硬锁，否则对端仍可通过 v2 profile/area 改变本侧行为 |
| 10 | 探测边的 `target_epoch` 取对端 v2 值 | 取**本侧** endpoint generation / context incarnation；对端值仅用于协商校验 | 业务证据 `UbPostPath.endpoint_generation` 是本侧 generation；照搬对端值会让每次成功都被计成 `probe_evidence_unmapped` |

受限项汇总：**physical-only RM 解析**未实现（登记为后续切片）；其余 W3g 合同（claim、v2 字段/协商/回退、身份归一、探测懒激活、观测）全部落地。

## 13. 风险与缓解

| 风险 | 影响 | 缓解 |
|---|---|---|
| claim 与租约语义错位（拿错成员的 error_epoch） | 误拒/误放 | `error_epoch` 随租约返回；单测覆盖并发退役 |
| v2 字段被旧对端误读 | 握手失败 | 只用能力串启用；未知字段不发送给未声明方 |
| 探测区 descriptor 造假/错配 | 写错内存 | 只接受对端显式发布；epoch 校验；不猜地址 |
| identity 归一化与 budget 账本键不一致 | 预算重复计 | 单一函数、单测锁定；装配期断言只有一处生成键 |
| claim 拒绝导致抖动 | 吞吐下降 | 拒绝原因可观测；短定时 defer 而非立即重试 |

## 14. 待确认问题

| # | 问题 | 默认处理 |
|---|---|---|
| Q1 | `target_profiles` 是否必须与 `jetty_ids` 一一对应，还是允许"缺省 profile = v1 行为" | 允许缺省（空数组 = 全部按 v1），非空则必须等长 |
| Q2 | `probe_area` 与对端探测 Jetty 的绑定粒度（每口一页 vs 每边一页） | 每口一页（探测详设 §2.3） |
| Q3 | 身份导入是否也要覆盖 `rc_legacy` | 不覆盖（D-8） |
| Q4 | claim 拒绝时是否要区分"池停止"与"成员退役"的 defer 桶 | 均为 `kPathUnavailable`，靠计数区分 |

## 15. 对上游文档的回写清单

| 上游 | 回写 | 状态 |
|---|---|---|
| 总设计 §5.5 | R-1 落地：claim 由本侧复核升级为池同步域内 `tryClaimPost` | 已回写（2026-11-18） |
| 总设计 §11 接口 1 | 补 `tryClaimPost` 的本侧形态与回退开关 | 已回写 |
| 总设计 §9.4 / 探测详设 §16 项 1 | v2 字段集合、能力串、v1 回退矩阵、探测源/懒激活 | 已回写 |
| 总设计 §4 资源模型 | budget `device_key` 改为归一化身份 | 已回写 |
| 总设计 §12 配置 / §15 观测 | 新增 claim/bootstrap/identity 键与指标 | 已回写 |
| 总设计 §13 | W3g 实现状态段（含懒激活、受限项） | 已回写 |
| W3 详设 §8.2 / §21.5 / §22.4 | R-1 由"过渡形态"改为"已落地"；补配置键；更新认领回归缺口说明 | 已回写 |
| W3 详设 §13 观测 | claim/bootstrap/identity/`probe_path_count` 指标 | 已回写 |

## 16. 术语表

| 术语 | 含义 |
|---|---|
| claim | 下发前由池同步域给出的"允许发送"判定 |
| GateClosed | claim 的拒绝结果（池停止 / 成员退役 / 换代 / error epoch 抬高） |
| error_epoch | 池内成员的退役计数，用于 claim 竞态判定 |
| v2 字段 | 在 v1 描述符上追加的可选对象（target profile / probe area / epochs） |
| 物理口身份 | `(native_device_path, eid)` 归一后的稳定键 |

## 17. 落地记录与验收现状（2026-11-18）

> 复审要求的"落地记录"章节；偏差明细见 §12.1，此处只给形状、验收与已知边界。

### 17.1 落地形状

| 项 | 落地 | 文件 |
| --- | --- | --- |
| claim | `SendJettyPool::tryClaimPost` 池锁内纯读判定 + `PoolMember::error_epoch` + `LeaseWithCredit::error_epoch`；workers 两输入（池 verdict + PLM epoch）与分因计数；`claim/enabled` 回退 | `send_jetty_pool.h/.cpp`、`workers.h/.cpp`、`params.h` |
| v2 字段 | `schema_version`/`target_profiles`/`probe_areas`/epochs 纯加法；能力串协商；v1 对端逐位不变；接收侧 `protocol=v1` 净化；`rmTarget()` profile 透传 | `bootstrap_v2.h`、`control_plane.h`、`endpoint.h/.cpp`、`urma_adapter.h` |
| 探测来源 | `BootstrapProbePeerDescriptorSource` 吸收每次成功 bootstrap；`StandaloneProbeResourceProvider::refreshRemoteEdges()` 以接口 7 激活；无 area 不建 edge | `bootstrap_peer_source.h/.cpp`、`probe_provider_standalone.*`、`ub_transport.cpp` |
| 惰性激活 | 安装时允许空图（仅 bootstrap 回退），武装本地端口/PLM/scheduler/probe 槽；首个 edge 到达后经 wake callback 立即调度；注入空图仍 fail-fast | `probe_link_manager.h/.cpp`、`ub_transport.cpp`、`probe_provider_standalone.*` |
| identity | `makePhysicalDeviceIdentity` + 缓存；budget 键单点归一；`identity/import=off` 回退 | `identity_import.h/.cpp`、`ub_transport.cpp` |
| 观测 | `claim_rejected_*`/`claim_disabled`、`bootstrap_v2_peers/v1_peers/v2_ignored`、`probe_area_missing`、`identity{import,devices,ok,fail}`；`UbTransport::probePathCount()`/`probeStats()` | `workers.cpp`、`ub_transport.*` |

### 17.2 验收现状

- `ctest -R "tent_|ub_|bounded_mpsc"`：**79/79**；`tent_ub_*` 22 个目标全绿。
- 新增单测：`tent_ub_bootstrap_v2_test`（11）、`tent_ub_bootstrap_peer_source_test`（5）、`tent_ub_identity_import_test`（5）、claim 用例并入 `tent_ub_post_claim_test`（9）。
- 关键集成：claim 正常/回退、v1/v2 绑定矩阵、探测源喂图、**install 无 peer → 业务 bootstrap → `probePathCount()>=1` 且 `probe_scheduled>=1`（懒激活真实顺序）**、无 area 武装等待、注入空图 fail-fast。
- 真机：仍受 `probe/enabled` 默认关闭与 provider 版本门槛约束（同 W1 RK11）；bootstrap v2 的双端行为目前由 fake adapter + 本地 RPC 闭环证明。

### 17.3 已知边界

- physical-only RM 的"对端拓扑缺失仍建 target"未实现（§12.1 #7）。
- 探测边的门禁位暂用"握手成功 + v2 能力"代理，未接池化侧显式 gate 字段（`bootstrap_peer_source.cpp` 注释）。
- 同 path 同版本的探测页刷新按 `area_epoch`/`area_address` 判定；若对端只改 page 内容而不 bump epoch，本侧不会重导入。
- **反向探测不对称（决策 b，2026-11-18）**：首次 bootstrap 的 request 不带本端 `probe_areas`——请求阶段还不知道对端是否声明 `bootstrap_v2`，而 D-6 禁止向未声明 v2 的对端发送 v2 字段。因此纯单向业务（只 A 写 B）时，B 侧不会出现指向 A 的探测边；反向边要等 B 自己发起一次 bootstrap（通常由反向业务触发）。该行为已由 `UbBootstrapV2Test.BootstrapResponseSendsV2OnlyWhenBothSidesAllowIt` 在 request 边界钉住。若将来需要单向业务也对称探测，必须先在能力协商层引入"预声明/探测专用握手"，不能把 v2 字段贸然塞进首次请求。
- **验收口径**：`probePathCount()` 是 PLM 路径行数（任何健康状态），不是"健康记录数"。懒激活用例在 `probe/test/periodic_all=true`（验证专用周期探测）下断言 `probe_scheduled ≥ 1` 且 `probe_posted ≥ 1`，证明调度器真的发出探测而不只是接口层挂上了边；默认策略下 ELIGIBLE_UNVERIFIED/HEALTHY **不做周期探测**（只在 campaign 中核验，探测详设 §8.1），所以不开这个开关就观察不到调度动作——这也说明"边到达后自动开始探测"只对**已隔离/恢复中**的路径成立。
