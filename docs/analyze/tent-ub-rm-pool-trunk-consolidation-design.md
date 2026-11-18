# TENT UB RM 池化主干收敛详细设计

| 项目 | 内容 |
| --- | --- |
| 状态 | 详细设计（待评审），不含代码改动 |
| 日期 | 2026-11-18 |
| 主干 | `work/merge-ub-rm-jetty` |
| 上游基线 | `xdlequ/main` = `261dc2b2` |
| 共同祖先 | `f73dfdac` |
| 设计依据 | 《TENT UB RM SEND Jetty 池化与链路探测设计（最终版）》（下称**总设计**，12 节 764 行）；`docs/analyze/tent-ub-w3g-pool-claim-and-v2-design.md` |
| 范围 | 物理 UB + URMA_TM_RM + CPU DRAM 单边 READ/WRITE；不引入业务 SEND/RECV/atomic |
| 目的 | 把两条并行实现收敛到一条可评审、可验收的主干，并给出每个改动的位置、接口、状态机、测试与回滚点 |

本文所有"事实"均给出可复核命令或源码锚点（附录 D）。标注 **待核** 的条目表示本次未逐行验证，实施前必须先确认。

---

## 1. 结论与选型

### 1.1 两分支本质

| 维度 | `tent/ub_rm_jetty_post`（当前） | `work/merge-ub-rm-jetty` |
| --- | --- | --- |
| 相对共同祖先 | +2 提交（`757fbf00` W3f、`e9cb1b2f` W3g） | +16 提交（`d985f2f6` 28-commit squash、`ef9ba164`、merge `0233bfaf`、13 个上游 main） |
| 改动面 | 33 文件 +3256/−65，全部在 TENT UB | 154 文件（含上游噪声），TENT UB 内 26 文件 +4406/−669 |
| 相对 `xdlequ/main` | 落后 13、领先 3 | 落后 0、领先 4 |
| 地基（RM 模式 / physical gate / TargetDescriptor / v2 契约） | **全缺** | **齐备** |
| 形态 | 局部增量补丁 | 完整实现 + 已合上游 |

### 1.2 选 `work/merge-ub-rm-jetty` 作主干

依据是总设计的启用条件（总设计 §9.2）是一个合取式，其中三项在当前分支**根本不存在**：

```
pooled_rm_enabled =
     config.enable && trans_mode == RM
  && device_kind == PHYSICAL && physical_gate == PASS        ← 当前分支无
  && rm_adapter_gate == PASS                                 ← 当前分支无
  && rm_bootstrap_v2_gate == PASS                            ← 当前分支是另一套（见 §1.3）
  && rm_legacy_baseline == PASS                              ← 当前分支无
  && probe_config_gate == PASS && provider_event_drain_gate == PASS
  && business_probe_target_budget_gate == PASS
```

且总设计 §1.4 明确"当前 RC TENT 实现不能作为 RM fallback"，而当前分支 `params.h` 的默认值仍是 `runtime_profile = kRcLegacy`、`trans_mode = kRc`。

### 1.3 一个必须记录的方向性冲突

当前分支的 bootstrap v2 头文件首行自述：

> `W3g-b: bootstrap v2 is a field addition, not a new protocol.`

它保留 v1 的 `jetty_ids` 数组，并要求 `target_profiles` 与 `jetty_ids` **等长**。总设计 §4.2 首句恰是相反的硬约束：

> RM bootstrap v2 不交换逐 lane Jetty 列表，不要求本端业务 Jetty 数和对端 target 数相等。

**结论：W3g-b 的"字段加法"形态不进入主干**。当前分支只保留其"运行期协议选择器"的语义（见 §6），契约本体沿用主干已有的 target/epoch/adjacency 形态。

### 1.4 非目标

- 不改业务 SEND/RECV/atomic 语义，不引入 BONDP 私有 WR。
- 不在 CRM/UM 路径上做清理（总设计 §10.1 P2 限定"只清理 RM 分支 RC 残留"）。
- 本期不实现自适应扩缩容（总设计 §10.1 明确首版静态 `desired_serving`）。

---

## 2. 事实基线

### 2.1 主干已具备、**不要重做**的能力

下列能力经源码核对已在主干实现，本设计不得重复引入（早期评估中曾误判为"当前分支独有"，此处更正）：

| 能力 | 锚点 | 说明 |
| --- | --- | --- |
| `UbRuntimeProfile{kRcLegacy,kRmLegacy,kRmPool}` | `params.h:45-49` | 三档运行剖面，对齐总设计 §1.4 |
| physical-only 准入 + `DeviceKind` 五分类 | `ub_transport.cpp:106/270/275/398`、`device_selection.h:28-145` | UNKNOWN 永不过滤；`physical_only` 缺 `device_identities` 直接拒绝（`params.h:216-218`） |
| `TargetDescriptor` / `RemoteTarget` / `importRemoteTarget` | `urma_adapter.h` 等 6 文件 | 对齐总设计 §4.1 |
| v2 契约字段 | `bootstrap_types.h:82-173` | `UbBootstrapDesc{protocol_version, endpoint_generation, segment_generation, capabilities}`、`UbRmTargetDescriptor`、`UbRmBootstrapDesc{transport_mode,node_incarnation,context_incarnation,target_identity,topology_epoch,target_epoch,provider_capabilities}` |
| 门禁函数族 | `bootstrap.h` + `bootstrap.cpp` | `validateUbBootstrap` / `gateUbBootstrapForMode` / `validateUbBootstrapV2PeerIdentity` / `compareUbBootstrapV2` |
| `makePhysicalDeviceIdentity(path,eid)` → `"<path>#<首个EID组>"` | `bootstrap.cpp` | 与总设计 §2.2 `ContextKey` 一致；**当前分支的同名函数并未更强**——其头文件自述支持 trim/lower 归一与 `PhysicalDeviceIdentityCache`（实现未逐行核对，**待核**），但主干已在 5 处共用同一函数，先保证唯一性 |
| 池：weak 租约 / LIFO / Serving 语义 / 双执行器 / 关停屏障 | `send_jetty_pool.h:151-393` | RC-03/RC-04/RC-08 加固已在内；`desired_serving` 是 IDLE+IN_USE（对齐总设计 §7.3） |
| 完成门 `AttemptGate` + `CompletionRecord` | `attempt_gate.h`、`slice.h` | `ExecutionOutcome`/`ResourceSafety`/`Settlement` 即总设计 §7.4 三维度与 §5.2 归还条件 |
| 任务级 K | `slice.h:599` `UbTask::tryAcquireAttemptSlot()` | 对齐总设计 §1.1.5 / §5.3 `task_K_credit` / §9.1"跨 Context 统一计数" |
| 邻接驱动的探测 | `probe_types.h:59`、`probe_resources.h:73` | "One probe link is one legal adjacency edge"，对齐总设计 §2.2/§6.1 |
| native 预算按物理设备记账 | `ub_transport.cpp:529-556` | 键为 `makePhysicalDeviceIdentity`，对齐总设计 §5.3 `N_jetty(device)` |
| RM legacy 复用池 | `jetty_pool.{h,cpp}` | 总设计 §1.4 的 RM legacy 基线 |

### 2.2 主干确实缺的（本设计要补）

| 编号 | 缺口 | 证据 | 对应总设计 |
| --- | --- | --- | --- |
| **P1** | claim 不是池内同步判定，只有 `stopping()` + PLM epoch 两个独立标量；`PoolMember.error_epoch` 字段从未被写 | `workers.cpp:1777` 的 `claimPost(bool)`；`send_jetty_pool.cpp` 中 `error_epoch` 命中 0 次 | §5.2 规则 3 |
| **P2** | 无 v1/v2 运行期协议选择器（`gateUbBootstrapForMode` 由 mode 隐式决定） | `params.h` 50 个键中无 `bootstrap/*` | §4.2 末段、§9.2 |
| **P3** | 进程级 `TaskPermitLedger` 与其自身注释矛盾，仍在 post 路径上取券 | `admission.h:12-17` 声明"deliberately left behind"，但 `ub_transport.cpp:558` 构造、`workers.cpp:1557` 调用 | §1.1.5 / §5.3 / §9.1 |
| **P4** | 配置键与 §9.1 不一致：10 个缺失、3 个需改名、1 个重复 | 见附录 A | §9.1 |
| **P5** | §10.2 要求的 5 个组件文件一个都没有 | `git ls-tree` 核对 | §10.2 |
| **P6** | 池状态机缺 `FLUSHING`、`BACKOFF` | `send_jetty_pool.h:91-101` 九个状态 | §5.1 状态图 |
| **P7** | 无 tombstone、无 `quarantine_budget`/`tombstone_budget`/`queue_budget` 归一 | 全仓命中 0 | §7.2、§5.3、§11.4 |
| **P8** | `probe_link_count` 无对应访问器；`delete_pending`/`unknown_token` 等指标缺失 | 主分支无 `probePathCount` 等价物 | §12.1 |
| **P9** | 仓库卫生：根目录 8 个散落 md；`ub_native_data_path_test.cpp` 3110 行 | `git ls-tree`；`wc -l` | AGENTS.md"PR 精简" |

### 2.3 合并冲突面（供排期参考）

`git merge-tree` 预演：当前分支与主干直接合并产生 **12 个冲突文件**（`control_plane.h`、`endpoint.h/.cpp`、`identity_import.h/.cpp`、`params.h`、`send_jetty_pool.h/.cpp`、`urma_adapter.h`、`ub_transport.cpp`、`tent/tests/CMakeLists.txt`、`ub_core_test.cpp`）。

因此本设计**不做分支合并**，只做**选择性移植**：以主干为基线，逐个提交重建 P1/P2 的增量（本文件 §5、§6），冲突面降为 2 个文件（`send_jetty_pool.{h,cpp}` + `workers.cpp`）。

---

## 3. 总体路线

### 3.1 阶段与依赖

```
D0 卫生与可评审性（独立，无代码语义变更）
  └─> D3 K 语义收敛（删除冗余券，行为等价，可独立验证）
        └─> D1 池化 claim 链（唯一实质行为新增）
              └─> D6 池状态机对齐（依赖 D1 的 error_epoch 契约）
                    └─> D5 组件拆分（纯搬移，放在语义稳定之后）
D2 bootstrap 协议选择器（与 D1 并行，不共享文件）
D4 配置面归一（依赖 D2/D3 的键名，最后做）
D7 tombstone 与三类预算（依赖 D6）
D8 观测补齐（贯穿，最后统一收口）
```

原则：**先用行为等价的改动（D0/D3）压低 diff 噪声，再上唯一的行为变更（D1），最后做纯搬移（D5）与配置重命名（D4）**。这样每个提交都能单独回滚，且 reviewer 只需在 D1 上投入语义注意力。

### 3.2 提交拆分

| # | 提交 | 类型 | 触及文件 | 可独立合入 |
| --- | --- | --- | --- | --- |
| 1 | `[TENT][UB] Move pool design notes out of repo root` | 卫生 | 8 个根目录 md → `docs/analyze/` 或删除 | 是 |
| 2 | `[TENT][UB] Drop the redundant process-wide attempt permit ledger` | 等价 | `admission.{h,cpp}`、`workers.{h,cpp}`、`ub_transport.cpp` | 是 |
| 3 | `[TENT][UB] Gate pooled posts on a pool-synchronized claim verdict` | 行为 | `send_jetty_pool.{h,cpp}`、`workers.cpp`、`workers.h` | 是（依赖 2 仅为顺序） |
| 4 | `[TENT][UB] Add FLUSHING/BACKOFF pool member states` | 结构 | `send_jetty_pool.{h,cpp}` | 是 |
| 5 | `[TENT][UB] Select the bootstrap protocol explicitly` | 行为 | `params.h`、`bootstrap.{h,cpp}`、`ub_transport.cpp` | 是 |
| 6 | `[TENT][UB] Split retire/refill executors and target registry` | 搬移 | `send_jetty_pool.*` → `jetty_retire_worker.*`/`jetty_refill_worker.*`/`business_jetty_pool.*`；`target_registry.*` | 否（依赖 3/4） |
| 7 | `[TENT][UB] Normalize pooled config keys to the design names` | 配置 | `params.h`、`config.cpp`、各测试 | 是（末尾） |
| 8 | `[TENT][UB] Add tombstones and bounded recovery budgets` | 行为 | 新增 `tombstone.h`、`send_jetty_pool.cpp`、`params.h` | 否（依赖 4/7） |
| 9 | `[TENT][UB] Expose pooled observability counters` | 观测 | `ub_transport.{h,cpp}`、`workers.{h,cpp}` | 是 |

> 主干历史上把 28 个提交压成 `d985f2f6`，丢了 bisect 粒度。本设计反向操作：**新工作按上表 9 个提交推进**，不再 squash。

---

## 4. 详细设计 D0 / D3（行为等价的两步）

### 4.1 D0 仓库卫生

**问题**：主干树内 **8 个**非仓库类 md 位于仓库根：`supercache_jetty_pool_current_and_target_rm_concise.md`（1302 行）、`supercache_jetty_pool_{interface_design_analysis,rm_legacy_detailed_design,p0_p5_execution_plan,deviation_correction_register,blocking_decision_register}.md`、`jetty_pool_test_report.md`、`tent_rm_p0_p5_execution_checklist.md`。另有 `docs/verification/ub/rm_pool_full_link_verification.md`，位置正确，保留。

**处置**：

1. 把 6 个 `supercache_jetty_pool_*` 合并为**一份** `docs/analyze/tent-ub-rm-pool-source-design.md`（只保留结论段与差异表），原始长文不入库。
2. `jetty_pool_test_report.md` 属过程产物，删除；其中有效结论并入本文件 §10.4。
3. `tent_rm_p0_p5_execution_checklist.md` 与总设计 §10.1 的阶段编号重复且已过时，删除；未完成的检查项并入本文件 §3.2 的 9 提交表。
4. `rm_pool_full_link_verification.md` 保留（在 `docs/verification/` 下，符合目录语义）。
5. 遵循 `docs/AGENTS.md`：只动这 8 个根目录文件 + 上述 4 个目标位置，不重写无关页面。

**验收**：`git ls-tree --name-only HEAD | grep -c '\.md$'` 在仓库根只应剩 `README.md`、`AGENTS.md`、`CLAUDE.md`、`CONTRIBUTING.md` 等既有文件。

### 4.2 D3 删除冗余的进程级券

**问题复述**（事实，非推测）：`admission.h:12-17` 的注释明确写 `TaskPermitLedger` 被"有意留下未接"，但代码里：

- `ub_transport.cpp:558`：`task_permits = std::make_shared<ub::TaskPermitLedger>(params.send_pool.task_max_inflight_attempts);`
- `workers.cpp:1557-1561`：`if (task_permits_) { pooled_permit = task_permits_->tryAcquire(); if (!pooled_permit.valid()) defer... }`
- `workers.h:197/415`：构造参数与成员。

即：**K 被算了两次**——一次是进程级（错的那次），一次是任务级 `UbTask::tryAcquireAttemptSlot()`（对的那次）。K=4 时进程级券会把整个进程的并发 attempt 卡在 4，与总设计 §1.1.5"单个网络 UbTask 跨 Context 的并发 attempt 受 K 限制"直接冲突。

**改法**：

```cpp
// admission.h：删除 TaskPermitTicket、TaskPermitLedger 两个类型，
// 头部注释降级为一段历史说明（保留 D-6/R-3 决策记录，防止有人再加回来）。

// workers.h：删除
//   std::shared_ptr<TaskPermitLedger> task_permits_ = nullptr;   // :415
//   std::shared_ptr<TaskPermitLedger> task_permits = nullptr,    // :197
//   TaskPermitTicket pooled_permit;                              // :94/:1547

// workers.cpp:1557-1561：删除整段 pooled_permit 获取；
// K 的唯一闸门保持在 pending.task->tryAcquireAttemptSlot()。
```

**注意**：删除券获取点后，`defer_pooled_attempt()` 的既有分支顺序不变；`TaskPermitTicket` 析构即释放的语义由 `releaseAttemptSlot()` 承担，**不得遗漏**——这是本步唯一的行为敏感点。

**测试**：`ub_admission_test.cpp` 中针对 `TaskPermitLedger` 的用例删除，保留 `UbTask::tryAcquireAttemptSlot` 的 K 用例（`:301-325` 已覆盖并发与跨 task 独立）。新增一条回归断言：单进程两个 task 各 K=2 时允许 4 个并发 attempt（证明不再被进程级券截断）。

**风险**：低。若下游有代码依赖 `send_pool/task_max_inflight_attempts` 的行为，改名步骤（D4）一并处理。

---

## 5. 详细设计 D1：池化 claim 链

这是**唯一实质行为新增**，也是总设计 §5.2 规则 3 的落地：

> 发布 token 和全部 owner；PostGate ticket 与 close 线性化，复核 stopping、PathVersion、fault_epoch、target/注册代次后认领唯一 post。

### 5.1 现状与目标

| | 现状（主干） | 目标 |
| --- | --- | --- |
| 判定位置 | `UbWorkers::claimPost()` 内，两次独立读：`pool->stopping()` + `probe_manager_->businessFaultEpoch(path)` | 池内一次加锁的**全序判定**，返回带因由的枚举 |
| 判定项 | 2 项 | 5 项：stopping → bucket → member state → lane_generation → error_epoch |
| 观测 | 只有 `claim_after_isolation_` | 按拒绝原因分桶计数 |
| `error_epoch` | 字段存在但从不递增 | 退役路径原子递增，租约携带快照 |

**为什么必须做**：现状的 `pool->stopping()` 与真正的 post 之间存在"判定→提交→发布→post"窗口，窗口内池可能开始关停而成员仍被判定为可用。总设计的契约要求该判定与成员状态变化**线性化**（同一把锁内比较全部代次），否则 §11.4"shutdown 幂等且不把已接受请求切到 legacy"无法证明。

### 5.2 数据结构

```cpp
// send_jetty_pool.h

// 拒绝因由。顺序即判定顺序，不可重排（调试与测试都依赖它）。
enum class ClaimClosed : uint8_t {
    kNone = 0,          // 允许 post
    kPoolStopping,      // beginStop() 发生在 tryLease() 之后
    kMemberRetired,     // 成员已不在 kInUse（retired/quarantined/未知）
    kGenerationMoved,   // lane_generation 不再匹配租约快照
    kErrorEpochMoved,   // 租约期间成员至少退役过一次
};

// 入参：全部来自租约自身，池内不保留 in-use 索引（O(1)）。
struct ClaimRequest {
    AttemptKey attempt;
    uint64_t lane_generation = 0;
    uint64_t error_epoch_snapshot = 0;
    const PoolMember* member = nullptr;  // 租约持有者保证生命周期
};

struct ClaimVerdict {
    ClaimClosed closed = ClaimClosed::kNone;
    [[nodiscard]] bool claimed() const noexcept {
        return closed == ClaimClosed::kNone;
    }
};

// LeaseWithCredit 增加一个字段（主干原来没有）：
struct LeaseWithCredit {
    SliceJettyLease lease;
    size_t jfc_index = 0;
    uint64_t jfc_credit_token = 0;
    uint64_t error_epoch = 0;   // 【新增】租约生效瞬间的快照
};
```

成员侧新增接口：

```cpp
// const 方法：只读判定，不改状态、不计数、不调 provider、不加额外锁。
[[nodiscard]] ClaimVerdict tryClaimPost(const PoolKey& pool_key,
                                        const AttemptKey& attempt,
                                        const ClaimRequest& request) const;
```

### 5.3 判定算法（固定顺序）

```cpp
ClaimVerdict SendJettyPool::tryClaimPost(const PoolKey& pool_key,
                                         const AttemptKey& attempt,
                                         const ClaimRequest& request) const {
    if (!attempt.valid() || request.member == nullptr) {
        return {ClaimClosed::kMemberRetired};   // 无身份可担保，按退役处理
    }
    std::lock_guard<std::mutex> lock(mutex_);
    if (stopping_)                          return {ClaimClosed::kPoolStopping};
    auto bucket = buckets_.find(pool_key);
    if (bucket == buckets_.end())           return {ClaimClosed::kMemberRetired};
    const PoolMember& member = *request.member;
    if (member.pool_key != pool_key ||
        member.state != PoolMemberState::kInUse) {
        return {ClaimClosed::kMemberRetired};
    }
    if (member.lane.lane_generation != request.lane_generation) {
        return {ClaimClosed::kGenerationMoved};
    }
    if (member.error_epoch != request.error_epoch_snapshot) {
        return {ClaimClosed::kErrorEpochMoved};
    }
    return {ClaimClosed::kNone};
}
```

**可达性说明（必须写进头文件注释）**：当前是单租约所有权模型——一个 leased member 只能由它的租约持有者操作，因此实践中只有 `kPoolStopping` 可达；其余三个分支是**防御性断言**，为将来多所有者/后台退役模型把判定做成全序，并由单测逐一钉住。**不得把它们当作现存竞态的证据，也不要为此加锁。**

### 5.4 `error_epoch` 的递增契约

```cpp
// 唯一的递增点：成员离开 kInUse 的退役路径（retireFaulted / 泄漏回收 /
// 创建所有权不明转 quarantine）。
void SendJettyPool::retireMemberLocked(std::shared_ptr<PoolMember> member,
                                       const std::string& reason) {
    member->error_epoch += 1;          // 原子性由 mutex_ 保证
    member->state = PoolMemberState::kFaulted;
    member->retire_reason = reason;
    ...
}
```

不变量：**同一次退役内只递增一次**；`tryLease()` 成功时把当前 `error_epoch` 写入 `LeaseWithCredit::error_epoch`；`returnOnce(gate_passed=true)` 不递增（正常归还不是退役）。

### 5.5 调用点与逆序回滚

主干 `workers.cpp` 的 pooled post 路径改为：

```
① 任务级 K          pending.task->tryAcquireAttemptSlot()
② tryLease          pool->tryLease(...)            → 拿 lease + jfc credit + error_epoch
③ 池化 claim        pool->tryClaimPost(...)        → 【新增】
④ PLM epoch 复核    probe_manager_->businessFaultEpoch(path) != fault_epoch_snapshot
⑤ AttemptGate       attempt_gate_->registerAttempt(...)
⑥ Record + commit   makeRecord / tryCommitPost
⑦ publish + post
```

顺序理由：③ 必须在 ⑤ 之前——被拒绝的 claim 不应产生 gate token，否则要多一次 `discard()`；③ 与 ④ 必须都在"最后一个回滚点"之后、native post 之前，二者都无锁无分配。

`pool` 为 `shared_ptr`，③④ 任一拒绝走同一条逆序回滚（总设计 §5.2 规则 2"失败逆序回滚，不持部分资源等待"）：

```cpp
if (!claim.claimed()) {
    // 计数（按因由分桶，见 §5.6）
    // 逆序释放：credit 先回（绑定成员 JFC），再成员、在途计数、quota、K 槽
    credits->release(leased.jfc_index, leased.jfc_credit_token);
    (void)pool->returnOnce(std::move(leased.lease), /*gate_passed=*/true);
    endpoint->releaseOutstanding(length);
    (void)quota_->release(reservation);
    pending.task->releaseAttemptSlot();
    (void)pending.slice->deferAttempt(attempt);   // 不消费 retry、不记路径故障
    deferPending(pending, DeferReason::kPathUnavailable, epochs.path,
                 deadlineAfter(steadyNowNs(), kTimedDeferNs));
    return;
}
```

**关键约束**：回滚时 `returnOnce(gate_passed=true)` 是正确的——没有 WR 越过 native 边界，成员是干净的，必须回到 LIFO 头部继续服务，不能退役。

### 5.6 配置与观测

配置（归一到 D4 的名称后）：

| 键 | 默认 | 语义 |
| --- | --- | --- |
| `transports/ub/jetty_pool/claim/enabled` | `true` | 关闭时退回"只复核 stopping"的移植前行为，用于二分定位 |

观测（总设计 §12.1 要求"池借还、path 故障、probe 状态、flush marker、delete 和 refill 日志必须带 generation 和原因"）：

```cpp
uint64_t claim_rejected_stopping, claim_rejected_member,
         claim_rejected_generation, claim_rejected_error_epoch,
         claim_disabled, claim_after_isolation;
```

周期摘要输出形如 `claim{stopping=0,member=0,gen=0,epoch=0,disabled=0,isolated=3}`。

### 5.7 测试

| 用例 | 断言 |
| --- | --- |
| `ClaimAllowsTheLeasedMember` | 正常租约 → `kNone` |
| `ClaimRefusedAfterBeginStop` | `beginStop()` 后 → `kPoolStopping`，且成员被 `returnOnce(true)` 归还到 LIFO 头 |
| `ClaimRefusedAfterRetireBumpsEpoch` | 手工把成员退役（经测试钩子）→ `kErrorEpochMoved` |
| `ClaimRefusedOnGenerationMismatch` | 快照代次与成员不一致 → `kGenerationMoved` |
| `ClaimIsPureRead` | 连续两次调用后 `stats()` 各计数不变 |
| `ClaimDisabledFallsBackToStoppingOnly` | `claim/enabled=false` 时仅 stopping 生效且 `claim_disabled` 递增 |
| 集成 `PooledClaimDefersWithoutRetry` | claim 拒绝后：retry 计数不变、路径健康不变、1ms 内重新入队 |

### 5.8 与 `AttemptGate` 的分工（不重叠）

| 关口 | 时间点 | 负责的问题 |
| --- | --- | --- |
| `tryClaimPost` | post **之前** | 我现在还能不能用这个成员/这条路径（资源与代次） |
| `AttemptGate` | post **之后**到结算 | 这次 WR 的结果是什么、责任怎么归（`ExecutionOutcome` / `ResourceSafety`） |

两者不共享状态、不互相调用。评审时如看到任一实现被要求"顺便"做对方的事，应退回。

---

## 6. 详细设计 D2：bootstrap 协议选择器

### 6.1 目标

给总设计 §4.2 末段"v1、RM/RC 或 RM/UM mode 不一致必须明确拒绝或进入已验证 RM legacy 分支，不能静默切换"提供一个**运行期显式开关**，同时不引入 §1.3 所述的方向性冲突。

### 6.2 接口

```cpp
enum class UbBootstrapProtocol : uint8_t {
    kAuto = 0,  // 默认：按 runtime_profile 推导（rm_pool/rm_legacy → 必须 v2）
    kV2,        // 强制 v2；对端是 v1 直接拒绝
    kV1,        // 强制 v1；仅在 RM legacy 已验证时可服务，否则 NotSupported
};

// params.h
UbBootstrapProtocol bootstrap_protocol = UbBootstrapProtocol::kAuto;  // 读 transports/ub/bootstrap/protocol
```

落到主干已有的 `gateUbBootstrapForMode`。现有签名为两参 `(expected, desc)`，本设计**新增第三参** `requested`（其余调用点同步改，`kAuto` 保持旧行为）：

```cpp
Status gateUbBootstrapForMode(UbBootstrapMode expected,
                              UbBootstrapProtocol requested,
                              const UbBootstrapDesc& desc) {
    CHECK_STATUS(validateUbBootstrap(desc));
    switch (requested) {
        case UbBootstrapProtocol::kV1:
            if (desc.protocol_version != 1) return Status::InvalidArgument(...);
            // v1 只能走已验证 RM legacy；池化不服务 v1 对端
            return Status::OK();      // 由调用方按 profile 决定是否进 RM legacy
        case UbBootstrapProtocol::kV2:
            if (desc.protocol_version != 2) return Status::InvalidArgument(...);
            break;
        case UbBootstrapProtocol::kAuto:
            break;
    }
    // kAuto 沿用现有 mode 推导逻辑
    ...
}
```

### 6.3 明确不移植的三件

1. **`bootstrap_v2.h` 的字段加法契约**（理由见 §1.3）。
2. **`target_profiles` 与 `jetty_ids` 等长校验**——它把 v1 的逐 lane 模型固化进 v2。
3. **`bootstrap_peer_source`**（从对端 `probe_area` 发现邻接并建边）。总设计 §2.2/§6.1 规定邻接来自 `adjacency[peer_id][port_i]` 配置声明、由 bootstrap 校验；"发现式建边"会把配置错误变成运行时静默拓扑，与 §9.1 `probe/adjacency`"非空、无重复"的 fail-closed 要求冲突。

### 6.4 保留的两个好点

- **`bootstrap_v2_ignored` 计数**：对端声明 v1 时，任何 v2 块必须被**净化后忽略**并计数，而不是"能解就解"。主干应补这个计数器。
- **未知 schema 容忍 / 坏类型 fail-closed**：主干 `bootstrap_types.h:223-228` 已有 `protocol_version` 的严格读取，补一条"未知顶层键忽略、已知键类型错误抛错"的单测即可钉住。

---

## 7. 详细设计 D4–D8

### 7.1 D4 配置面归一（对齐总设计 §9.1）

完整映射见**附录 A**。要点：

- **改名 3 项**：`send_pool/desired_per_context` → `jetty_pool/business_desired_serving`；`send_pool/refill_interval_ms` → `jetty_pool/refill_interval_ms`；`business_queue_budget` → `queue_budget`。
- **新增 10 项**：`business_initial_size`、`business_min_size`（兼容映射键）、`business_max_size`、`retire_timeout_ms`、`cooldown_ms`、`probe/local_ports`、`probe/adjacency`、`probe/target_map`、`quarantine_budget`、`tombstone_budget`。其中 `business_min_size` 与 `jetty_per_endpoint` 并存且值不一致时必须拒绝（见总设计 §9.1）。
- **删除 1 项**：`send_pool/task_max_inflight_attempts`（D3 之后无意义，与 `task_max_inflight_attempts` 重复）。
- **迁移期策略**：改名采用"新键优先、旧键告警、两者并存且值冲突则拒绝"三段式，跨两个 minor 版本后再删旧键。**不做**静默兼容。

**风险**：配置改名会打破已有脚本。缓解：旧键保留读取、启动日志打印 `deprecated key X, use Y`；`jetty_per_endpoint` 在 RM pooled 下只读不生效（总设计 §9.1"RM pooled 不再用它计算租借数量"）。

### 7.2 D5 组件拆分（对齐总设计 §10.2）

总设计要求的 5 个新文件，主干一个都没有。拆分点是现成的：两个执行器本来就已经是独立成员函数。

| 总设计 §10.2 | 从哪来 | 拆分要点 |
| --- | --- | --- |
| `business_jetty_pool.{h,cpp}` | `send_jetty_pool.{h,cpp}` 改名 | 纯 rename + 头注释更新；`SendJettyPool` 类型名保留为 alias 一个版本，减少下游改动 |
| `jetty_retire_worker.{h,cpp}` | `deleteLoop()` + `destroyRetired()` | 接口 `RetireWorker::enqueue(std::vector<shared_ptr<PoolMember>>)` 与 `stop()/join()`；删除线程**不 poll JFC**（总设计 §7.2 规则 5） |
| `jetty_refill_worker.{h,cpp}` | `refillLoop()` + `createAndPublish()` 的槽位预留 | 接口 `RefillWorker::requestWake()`；`refill_gap = max(0, desired_serving − Serving − Creating)` 逻辑原样搬移 |
| `target_registry.{h,cpp}` | `bootstrap.cpp` 的导入 + `endpoint.cpp` 的 target 安装 + `ub_transport.cpp` 的预算键归一 | **必须新增**：当前主干没有 `ImportedTargetRegistry`/`TargetRegistry` 概念（两者命中 0 次），target 的 epoch/引用/unimport 分散在 endpoint 与 bootstrap 中。这是 D5 里唯一有实质重构的一项 |
| `probe_scheduler.{h,cpp}` | `probe_link_manager.cpp` 的调度部分 | 若 PLM 内部 scheduler 已足够自洽，可**延后**并以注释声明"合并在 PLM 内"，但需在评审中显式记录该偏差 |

**顺序**：先做 retire/refill（机械、风险低），再做 target_registry（有语义），probe_scheduler 最后按需。

### 7.3 D6 池状态机对齐（总设计 §5.1）

主干九态与总设计状态图的映射：

| 总设计 | 主干现状 | 动作 |
| --- | --- | --- |
| CREATING | `kCreating` | — |
| **BACKOFF** | 无 | **新增**：创建失败且无遗留原生责任时进入，refill 重试时回 CREATING |
| IDLE | `kIdle` | — |
| LEASED | `kInUse` | 保留名字，注释注明"V3 称 IN_USE" |
| FAULTED | `kFaulted` | — |
| QUIESCING | `kDraining` | — |
| **FLUSHING** | 无（混在 `kDraining` 里） | **新增**：PostGate 关闭且 active_post 退出、modify ERROR 已提交、等待 flush-done 的阶段 |
| DELETE_READY | `kDeleteReady` | — |
| DELETING | `kDeleting` | — |
| QUARANTINED | `kQuarantined` | — |
| DELETED | `kDestroyed` | — |

新增 `kBackoff` 与 `kFlushing` 后，`memberStates()` 的调用方（诊断、测试）需同步；建议同时提供 `poolMemberStateName()` 的稳定字符串，避免日志格式变化。

**为什么 `BACKOFF` 重要**：现有实现里创建失败直接把槽位还回 `creating--`，下一轮 refill 立刻重试。缺 BACKOFF 状态时无法区分"正在创建"与"创建失败正在退避"，`refill_gap` 会把失败者当成缺口反复冲击 provider（总设计 §7.3"失败按实际所有权清理并指数退避加抖动"）。

### 7.4 D7 tombstone 与三类预算

**问题**：主干 `quarantine` 命中 32 次（有隔离概念），但 `tombstone`、`quarantine_budget`、`tombstone_budget` 全仓 0 次。总设计 §7.2 要求：

> tombstone 保留 Context/device incarnation、实体类型、native ID、generation、ERROR epoch、事件责任和预算。……tombstone 满则背压和告警，不以 TTL 清理未证明安全的归属。

**设计**：

```cpp
struct JettyTombstone {
    uint64_t context_incarnation = 0;
    uint64_t device_incarnation = 0;
    EntityKind kind = EntityKind::kJetty;     // kJetty / kJfc / kJfr
    uint32_t native_id = 0;
    uint64_t generation = 0;
    uint64_t error_epoch = 0;
    Responsibility responsibility = Responsibility::kUnknown;
    uint64_t budget_hold = 0;                  // 计入 N_jetty(device)，直到删除证据
};

class TombstoneTable {
  public:
    // 满时返回 TooManyRequests，调用方必须背压（不 shed、不 TTL 清理）
    Status insert(JettyTombstone stone);
    std::optional<JettyTombstone> lookup(EntityKind, uint32_t native_id);
    // 只有"provider 证明销毁"或"事件代次已明确过期"才 erase
    void erase(uint64_t generation);
    size_t size() const; size_t capacity() const;
};
```

三处接线：

1. **CQE/AE 归属**：`workers` 收到无有效 token 的完成事件时先查 tombstone；命中则只结算旧责任并计数 `unknown_token`，未命中才升级 Context DRAINING（总设计 §7.1 事件分类表）。
2. **预算**：`NativeBudget` 的释放条件从"delete 返回成功"收紧为"tombstone erase"，即总设计 §5.3"native 预算必须等销毁责任明确"。
3. **背压**：`tombstone_budget` 满 → `refill` 暂停并告警；`quarantine_budget` 满 → 停准入（不是丢责任）。

**验收**：总设计 §11.4"退役链、排他删除、三类句柄结果、tombstone/事件屏障满足第 7 节；无证据不释放"——补上 D7 之前，这条**必然不合格**。

### 7.5 D8 观测补齐（总设计 §12.1）

主干缺失的指标与落点：

| 指标 | 现状 | 落点 |
| --- | --- | --- |
| `probe_link_count` | 无访问器 | `UbTransport::probeLinkCount()`（当前分支的 `probePathCount()` 语义正确：PLM 路径行数，非健康记录数，口径写进注释） |
| `business_idle/leased/faulted/retiring/delete_ready/quarantined` | `SendJettyPoolStats` 有 5 项，缺 `faulted`/`retiring` 细分 | 扩展 `SendJettyPoolStats` |
| `delete_pending` / `tombstone` / `unknown_token` | 无 | D7 提供 |
| `campaign_pending` / `probe_stopped` / `probe_inflight_per_port` | 无 | PLM 已有内部状态，补只读访问器 |
| `pool_generation` / `probe_generation` | 无 | 每次 Context 恢复递增，随日志输出 |
| `staging_deferred_free` | 待核 | 若 TENT staging 已有一侧计数则复用 |

**约束**（总设计 §12.1）：每 WR 热路径不得同步打印；日志必须带 generation 与原因。周期摘要沿用现有 `UbWorkers` 的 log-every-N 汇总行。

---

## 8. 并发与锁序约束（汇总）

| 规则 | 来源 | 本设计中的体现 |
| --- | --- | --- |
| 锁序 `context/pool → endpoint/path → JFC registry` | 总设计 §5.1 | `tryClaimPost` 只取 `mutex_`，不跨调用；PLM epoch 读在池锁**外**（两条独立输入） |
| 不持锁等 provider / CQE / retire worker | 总设计 §5.1 | 两个执行器与 `createAndPublish`/`destroyRetired` 均在锁外执行 provider 调用 |
| provider 调用不在 pool/endpoint/registry 锁内 | 总设计 §5.1 | 保持既有实现不变 |
| PostGate ticket 与 close 线性化 | 总设计 §5.2 规则 3 | `tryClaimPost` 在同一 `mutex_` 内比较 stopping 与成员代次 |
| 删除线程不 poll JFC | 总设计 §7.2 规则 5 | `RetireWorker` 拆分后维持该边界 |
| 已接受 WR 不得提前释放 lease | 总设计 §5.2 | `AttemptGate` 的 `handler_refs == 0` 条件不变 |

**新增的一条**：`error_epoch` 的读（`tryClaimPost` 内）与写（`retireMemberLocked` 内）必须在同一 `mutex_` 下，否则 `kErrorEpochMoved` 判定失去意义。

---

## 9. 错误处理与回滚矩阵

| 事件 | D1 后的行为 | 不得发生 |
| --- | --- | --- |
| claim 拒绝（任一因由） | 逆序回滚全部 owner，成员回 LIFO 头，`deferAttempt` 不计 retry，1ms 定时重新排队 | 退役成员、记路径故障、切换运行模式 |
| claim 拒绝时 `claim/enabled=false` | 退回只查 stopping，计数 `claim_disabled` | 静默跳过判定 |
| `tryLease` 返回 `TooManyRequests` | 视为容量压力 defer，不计 retry | 触发路径隔离 |
| `tryLease` 硬错误 | 按原生错误走 retry 预算 | 当成容量压力无限 defer |
| bootstrap 协议不匹配 | 明确拒绝或进已验证 RM legacy | 静默降级到 v1 |
| 创建失败且无原生责任 | 进 `kBackoff`，指数退避 | 立即重试冲击 provider |
| 删除失败 | `HANDLE_RETAINED` 才退避；`UNKNOWN` 转 quarantine 并保留预算 | TTL 清理未证明归属 |

---

## 10. 测试计划

### 10.1 单元（注入 adapter + 假时钟）

| 域 | 必须证明 | 归属 |
| --- | --- | --- |
| claim | §5.7 七条 | D1 |
| K | 两个 task 各 K 并发独立；无进程级截断 | D3 |
| 状态机 | BACKOFF 在创建失败后生效并在成功前保持；FLUSHING 在 flush-done 前不转 DELETE_READY | D6 |
| tombstone | 满时背压、不 shed；预算只在 erase 后释放 | D7 |
| bootstrap | v1/v2 矩阵、未知键忽略、类型错误 fail-closed、`bootstrap_v2_ignored` 计数 | D2 |
| 配置 | 新旧键并存冲突拒绝、`business_min_size` 与 `jetty_per_endpoint` 冲突拒绝 | D4 |

### 10.2 集成

1. 完整启动/关闭顺序（总设计 §8）：启动后 `desired_serving` 就位、path 为 ELIGIBLE_UNVERIFIED、post 线程从不 createJetty。
2. claim 拒绝路径：装满 → claim 拒绝 → defer → 释放后自动恢复，retry 计数不变。
3. 真实错误退役链：modify ERROR → flush-done → 软件 flush → DELETE_READY → retire worker。
4. 多 peer 共用 target，endpoint 淘汰不提前 unimport。

### 10.3 实机门槛

按总设计 §9.2 的"阶段零门槛"六项逐条出具证据，其中共享 target、真实 DMA/入站屏障、`delete/clean_jfc` 并发、native ID/事件行为**软件单测不能替代**。实机环境需在评审材料中写明 URMA/UDMA/驱动/profile 版本。

### 10.4 测试文件组织

主干现有 `ub_native_data_path_test.cpp` 3110 行、`ub_pooled_core_test.cpp` 1435 行。建议按 §10.1 的域拆成独立目标（与当前分支的 11 个细粒度目标趋同），每个目标单文件不超过约 600 行，便于定向重跑。此项可与任一提交同行，不作为独立提交。

**已合并的历史结论**（原 `jetty_pool_test_report.md`，该过程产物不再入库）：

- 软件级基线为 `tent_ub_pooled_core_test`（fake URMA adapter，不依赖真实
  provider/NIC）：预算账本、JFC credit、每 attempt 独占池、完成门、池化配置键与
  Context 挂载缝均有覆盖；并发租还 hammer 与 stop 交错验证了独占性、预算上限和
  "只允许 `TooManyRequests`、不阻塞不旁路"。
- 该轮自测发现并修复过一个产品缺陷：`destroyRetired`/`drainQuarantined` 在
  `deleteJetty` 失败后把原生句柄丢进局部变量，使下一次 drain 误判"无句柄=已销毁"
  而提前释放预算。现实现必须保持"删除失败时把句柄归还成员、预算继续计费"。
- 回归判定采用 A/B 复测：基线已存在的失败（当时为 1 个 bootstrap 用例与 13 个
  `UbRmLegacyTest.*`）与池化改动无因果关系。新增工作沿用该判定方式，不把基线失败
  算作本次回归。
- 限制不变：软件级验证不能替代真实 URMA/UDMA/驱动/profile 下的 DMA、入站屏障、
  `delete/clean_jfc` 并发与 native ID/事件行为取证（§10.3）。

---

## 11. 验收矩阵（总设计 §11.4 逐条）

| §11.4 条款 | 现状 | 完成后 | 证据 |
| --- | --- | --- | --- |
| RM-only/physical gate、bootstrap 与 provider 实机门槛通过，RM 路径无 RC bind/硬编码 | 部分（地基在，实机门槛未出证） | 需要实机报告 | §10.3 |
| 单 attempt 单 WR、LIFO、K、独立选路及完成门满足总设计 §5；endpoint 不拥有固定 Jetty | **已满足** | 保持 | 主干 `attempt_gate.h`、`slice.h:599`、`send_jetty_pool.h` |
| topology、共享 target 和 probe 数量来自合法邻接；当前代次恢复后停 timer | **已满足**（邻接模型在） | 加 `probeLinkCount()` 观测 | D8 |
| 退役链、排他删除、三类句柄结果、tombstone/事件屏障满足总设计 §7；无证据不释放 | **不满足**（无 tombstone） | 满足 | D7 |
| 空池不增 retry、满载不误扩池、全部角色及旧代次不突破预算 | 部分（Serving 语义在，quarantine 预算缺） | 满足 | D6 + D7 |
| Unknown WRITE 不被 fence 改写，自动重试符合安全契约 | **已满足** | 保持 | `AttemptGate::ExecutionOutcome::kUnknown` |
| shutdown/熔断/回滚幂等且保留未排空 owner，不在线切换 mode | 部分（关停加固在，claim 未线性化） | 满足 | D1 + D7 |

结论：**D1/D6/D7 是 `§11.4` 从"不满足"到"满足"的唯一路径**，其余项属于取证与观测。

---

## 12. 风险与回滚

| 风险 | 影响 | 缓解 |
| --- | --- | --- |
| claim 判定误伤正常 post | 吞吐下降 | `jetty_pool/claim/enabled=false` 一键回退到移植前行为；`claim_rejected_*` 全零监控 |
| 删除进程级 ledger 后并发升高 | provider 压力 | 任务级 K 仍是闸门；native 预算与 JFC credit 独立限流；灰度观察 `task_K_wait` |
| 配置改名打破部署脚本 | 启动失败 | 旧键保留读取 + 弃用告警；冲突值 fail-closed 而非静默取一 |
| 组件拆分引入 include 环 | 编译失败 | 拆分顺序 retire/refill → target_registry；每步独立可编译提交 |
| tombstone 表满 | 停准入 | 有界 + 告警；**不得**用 TTL 清理未证明归属（总设计 §7.2 明令） |
| 主干 squash 历史 | 无法二分定位回归 | 新工作一律按 §3.2 的 9 个提交推进，禁止再 squash |

全局回滚点：D0/D3/D5/D7/D8 各自可 `git revert` 单提交；D1 有配置开关；D4 有双键并存期。

---

## 附录 A：配置键映射（总设计 §9.1 → 主干现状）

图例：`✓` 已一致；`⇄` 需改名；`＋` 需新增；`✗` 需删除。

| §9.1 键（前缀 `transports/ub/`） | 主干现状 | 状态 |
| --- | --- | --- |
| `trans_mode` | `trans_mode` | ✓ |
| `jetty_pool/enable` | `jetty_pool/enable` | ✓ |
| `jetty_pool/force_disable` | `jetty_pool/force_disable` | ✓ |
| `jetty_pool/business_desired_serving` | `send_pool/desired_per_context` | ⇄ |
| `jetty_pool/business_initial_size` | — | ＋（首版 = desired_serving） |
| `jetty_pool/business_min_size` | `jetty_per_endpoint`（旧键） | ＋（兼容映射，冲突拒绝） |
| `jetty_pool/business_max_size` | — | ＋ |
| `jetty_pool/retire_timeout_ms` | — | ＋ |
| `jetty_pool/refill_interval_ms` | `send_pool/refill_interval_ms` | ⇄ |
| `jetty_pool/cooldown_ms` | — | ＋（现有 `endpoint_cooldown_ms` 语义不同，不复用） |
| `probe/enabled` | `probe/enabled` | ✓ |
| `probe/interval_ms` | `probe/interval_ms` | ✓ |
| `probe/backoff_max_ms` | `probe/backoff_max_ms` | ✓ |
| `probe/timeout_ms` | `probe/timeout_ms` | ✓ |
| `probe/peer_budget` | `probe/peer_budget` | ✓ |
| `probe/stop_on_recovery` | `probe/stop_on_recovery` | ✓ |
| `probe/recover_success_threshold` | `probe/recover_success_threshold` | ✓ |
| `probe/failure_threshold` | `probe/failure_threshold` | ✓ |
| `probe/local_ports` | — | ＋ |
| `probe/adjacency` | — | ＋ |
| `probe/target_map` | — | ＋ |
| `task_max_inflight_attempts` | `task_max_inflight_attempts` | ✓ |
| （重复项） | `send_pool/task_max_inflight_attempts` | ✗ |
| `native_budget` | `native_budget/max_jetty_per_device` | ✓ |
| `replacement_reserve` | `native_budget/replacement_reserve` | ✓ |
| `queue_budget` | `business_queue_budget` | ⇄ |
| `quarantine_budget` | — | ＋ |
| `tombstone_budget` | — | ＋ |

统计：**已一致 14、需改名 3、需新增 10、需删除 1**，共 27 个键位对应总设计 §9.1 的清单。

## 附录 B：符号对照与取舍

| 符号 | 主干 | 当前分支 | 取舍 |
| --- | --- | --- | --- |
| `tryClaimPost` / `ClaimClosed` | 无 | 有 | **移植**（D1） |
| `LeaseWithCredit::error_epoch` | 无 | 有 | **移植**（D1） |
| `attempt_gate.h`（完成门） | 有 | 无（用 CompletionRecord） | 主干保留，总设计 §7.4 三维度已覆盖 |
| `CompletionRecord` | 有（8 事实位） | 有 | 主干保留 |
| `UbTask::tryAcquireAttemptSlot` | 有 | 有 | 两边一致，保持 |
| `TaskPermitLedger` | 有（且被错误使用） | 有（未接） | **删除**（D3） |
| `bootstrap_v2.h` 字段加法 | 无 | 有 | **不移植**（§1.3） |
| `bootstrap_peer_source` | 无 | 有 | **不移植**（§6.3） |
| `makePhysicalDeviceIdentity` | 有（`bootstrap.cpp`） | 有（`identity_import.h`，带缓存） | 主干保留；缓存版收益是幂等去重，若 D5 target_registry 需要可后补 |
| `probePathCount()` | 无 | 有 | **移植为 `probeLinkCount()`**（D8） |
| `jetty_pool.{h,cpp}`（RM legacy 复用池） | 有 | 无 | 主干保留 |

## 附录 C：待删清单

| 对象 | 位置 | 原因 |
| --- | --- | --- |
| `TaskPermitTicket` / `TaskPermitLedger` | `admission.{h,cpp}` | 与任务级 K 重复且语义错误（D3） |
| `task_permits` 成员与构造参数 | `workers.h:197/415`、`workers.cpp:1557` | 同上 |
| `send_pool/task_max_inflight_attempts` | `params.h` | 与 `task_max_inflight_attempts` 重复（D4） |
| 根目录 6 个 `supercache_jetty_pool_*` + `jetty_pool_test_report.md` | 仓库根 | AGENTS.md PR 精简（D0） |
| `jetty_pool_test_report.md` | 仓库根 | 过程产物（D0） |

## 附录 D：事实复核命令

```bash
# 分支拓扑与规模
git rev-list --left-right --count xdlequ/main...work/merge-ub-rm-jetty
git diff --stat f73dfdac..tent/ub_rm_jetty_post

# 地基能力（当前分支 0、主干有）
git grep -lE 'DeviceKind|TargetDescriptor|physical_only' tent/ub_rm_jetty_post -- '*/tent/**'
git grep -lE 'DeviceKind|TargetDescriptor|physical_only' work/merge-ub-rm-jetty -- '*/tent/**'

# claim 缺口
git grep -n 'tryClaimPost' work/merge-ub-rm-jetty
git grep -n 'error_epoch' work/merge-ub-rm-jetty -- '*send_jetty_pool.cpp'   # 期望空

# K 矛盾
git grep -n 'TaskPermitLedger' work/merge-ub-rm-jetty -- '*admission.h'
git grep -n 'task_permits_' work/merge-ub-rm-jetty -- '*workers.*'

# 配置面
git grep -oE '"transports/ub/[a-z_/]+"' work/merge-ub-rm-jetty -- '*params.h' | sort -u | wc -l   # 50

# tombstone 缺口
git grep -c 'tombstone' work/merge-ub-rm-jetty                                                    # 0

# 合并冲突面
git merge-tree --write-tree tent/ub_rm_jetty_post work/merge-ub-rm-jetty | grep -c '^CONFLICT'    # 12
```

## 附录 E：实施状态与已知偏差（2026-11-18）

本节记录按本方案落地后的实际状态，供评审对照；不改变前文的接口与验收口径。

### E.1 提交面状态

| # | 提交主题 | 状态 | 主要位置 |
| --- | --- | --- | --- |
| 1 | 根目录 md 卫生 | 完成 | 6 份 `supercache_*` 合并为 `docs/analyze/tent-ub-rm-pool-source-design.md`；测试报告与旧清单删除 |
| 2 | 删除进程级券 | 完成 | `admission.{h,cpp}`、`workers.{h,cpp}`、`ub_transport.cpp`；K 只由 `UbTask::tryAcquireAttemptSlot()` 把关 |
| 3 | 池化 claim 链 | 完成 | `business_jetty_pool.{h,cpp}` 的 `tryClaimPost`/`error_epoch`；`workers.cpp` 的 `ClaimDecision`、分因由计数与 `claim/enabled` 开关 |
| 4 | FLUSHING/BACKOFF | 完成（BACKOFF 为 PoolKey 级，见 E.3） | 状态枚举 + 退役硬件 fence + refill 退避 |
| 5 | bootstrap 协议选择器 | 完成 | `UbBootstrapProtocol`、`gateUbBootstrapForMode(..., requested, ...)`、`bootstrap_v2_ignored` 计数 |
| 6 | 组件拆分 | 完成 | `business_jetty_pool.*`、`jetty_retire_worker.*`、`jetty_refill_worker.*`、`target_registry.*`；`probe_scheduler.*` 已存在 |
| 7 | 配置面归一 | 完成 | 新旧键并存冲突拒绝、`business_min_size` 与 `jetty_per_endpoint` 冲突拒绝 |
| 8 | tombstone 与预算 | 完成 | `tombstone.*`；预算释放以 tombstone erase 为证据；quarantine 满停准入、tombstone 满暂停 refill |
| 9 | 观测补齐 | 完成 | `probeLinkCount/probeCampaignCount/probeStopped/probeGeneration/poolGeneration`；池统计细分与 claim/tombstone 计数 |

### E.2 合并态修复（落地前置）

落地前 `work/merge-ub-rm-jetty` 的 HEAD 不能编译：`endpoint.h` 同时存在
`RemoteJettyInfo` 与 `RemoteTargetPtr` 两套 RM target 接口，`workers.cpp` 把
`Inflight`/`AttemptGate`（`d985f2f6` 线）与 `BusinessRecord`/`CompletionRecord`
（`f73dfdac` 线）交错合并成嵌套函数。本实施先收敛到后者这一条完整链路（即 W3f/W3g
已验证的池化 post 路径），删除重复的 `Inflight`/`AttemptGate` 运行时接线，保留
`AttemptGate` 作为被单测覆盖的独立组件。因此 §5.5 中的"⑤ AttemptGate"落在 W2 完成门
（`makeRecord`/`tryCommitPost`/`gate::onRealCr`）上，claim 仍在 gate token 产生之前，
满足"被拒绝的 claim 不产生 gate token"的约束。

同时修正了合并态带入的另外三处不一致：`probe.enabled` 曾要求 `rm_pool`（与 §6.3
"探测独立"冲突）、`rm_legacy` 安装被硬编码 `NotImplemented`（与已合入的
`bindRm`/`importRemoteTarget` 冲突）、`classifiesPostAcceptance` 的测试期望与实现
不一致。

### E.3 已知偏差（需评审确认）

1. **BACKOFF 是 PoolKey 级，不是成员级**：创建失败不会发布成员，故无法把一个
   "成员"置为 BACKOFF。实现把退避记录在 `PoolBucket::backoff_until_ns`，refill 跳过
   退避中的 PoolKey；需求路径仍受限 `max_creating`，可立即恢复。`kBackoff` 仅用于
   统一状态词表。
2. **事件归属只计数，不自动把 Context 置 DRAINING**：主干无法区分"已释放 token 的
   迟到 CQE"与"从未发出的 token"，后者才是需要升级的拓扑故障。tombstone 命中时计入
   `unknown_token_tombstoned`；未命中仍按未知 token 计数。自动 DRAINING 需要先有
   "已释放 token" 索引，否则会把正常重复 CQE 放大成设备故障。
3. **`probe/local_ports|adjacency|target_map` 为声明式配置**：已按 §9.1 解析并做
   非空/重复/未声明端口 fail-closed 校验；当前 standalone provider 仍从可信发现
   生成图，配置与发现不一致不会静默拼出一条拓扑。配置的文件级消费留给 probe
   directory 的后续接线。
4. **`quarantine_budget` 满时停准入**，`tombstone_budget` 满时暂停 refill；两者都
   不 shed、不 TTL 清理。tombstone 的预算释放以 erase 为证据；若插入失败导致无条目，
   预算保持计费并计入 `tombstone_insert_failures`。
5. **`staging_deferred_free`** 未新增计数：TENT staging 侧没有可直接复用的单侧计数，
   按 §7.5 "待核"保留。

### E.4 验证

- 构建：`cmake --build build -j8` 通过。
- 测试：`ctest -R 'ub_|config_override|transport_selector|config_lifecycle'`
  20/20 通过；其中新增 `tent_ub_post_claim_test`（11 例）、
  `tent_ub_tombstone_test`（7 例）与共享 target/池化 retry/refill 集成用例。
- 未执行：真实 URMA/UDMA/驱动/profile 的 DMA、入站屏障、`delete/clean_jfc` 并发与
  native ID/事件取证（§10.3），以及 30 分钟 soak。
