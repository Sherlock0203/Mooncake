# UB rm_pool 全链路接线开发验证报告

- 日期：2026-11-16
- 基线：`7966f9d8`（[TENT] add jetty pool，第一版池化核心组件已完成测试）+ 本次全链路接线变更
- 范围：将 `SendJettyPool` 接入 transport install 与 endpoint 完整发送路径，
  完成 rm_pool 数据面全流程，并执行全链路连通性验证
- 构建环境：Docker 镜像 `mooncake-devenv:local`，CMake Release，
  `-DBUILD_UNIT_TESTS=ON -DUSE_TENT=ON -DUSE_UB=ON`，从仓库根配置
  （`-S /workspace/mooncake`）；全部测试基于 fake URMA adapter，
  不依赖真实 provider/NIC/内核资源
- 结论：**4 个测试套件 134/134 全部通过（0 失败、0 跳过）。rm_pool 闸门按
  受信配置正常开启且保持 fail-closed；数据面经池化发送路径完成
  WRITE/READ 全链路数据传输，无阻塞、无泄漏、无回归。**

## 1. 链路接入节点说明

### 1.1 运行模式闸门（节点 0）

`params.h` `resolveUbRuntimeMode` 的 `kRmPool` 分支由第一版的
`NotImplemented` fail-closed 占位替换为受信开启路径：

| 前置条件 | 不满足时行为 |
|---|---|
| `trans_mode=rm` | `InvalidArgument`（既有矩阵） |
| `physical_only=true` | `InvalidArgument`：`runtime_profile=rm_pool requires physical_only=true` |
| `jetty_pool.force_disable=false` | `InvalidArgument`：`rm_pool is rejected while jetty_pool.force_disable=true` |
| 全部满足 | `mode = UbRuntimeMode::kPooledRm`，VLOG 记录后返回 OK |

闸门语义保持 fail-closed：任一守卫缺失即拒绝，绝不静默回退
`rm_legacy`/`rc_legacy`（INV16 开启前等价）。

### 1.2 transport install 装配链（节点 1–5）

`ub_transport.cpp` install 路径在 `kPooledRm` 下按序装配，任一环节失败
即 `failInstall` 整体逆序清理，不允许半挂载：

| 节点 | 位置 | 装配动作 |
|---|---|---|
| 1. 设备级预算（C05） | [ub_transport.cpp#L414-L444](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp#L414-L444) | 按 `device_key=makePhysicalDeviceIdentity(native_device_path, eid)` 去重，对每个物理设备 `NativeBudget::configure(device_key, verified=caps.max_jetty, configured, reserve)`；`configured` 未显式配置时取 `verified - replacement_reserve`（预留余量，上限不超过设备验证值），为 0 则拒绝 install |
| 2. 任务准入（C06） | [ub_transport.cpp#L445-L446](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp#L445-L446) | `TaskPermitLedger(task_max_inflight_attempts)`：进程级每任务在途 attempt 票据 |
| 3. 完成门（C07/D04） | [ub_transport.cpp#L447](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp#L447) | 进程级 `AttemptGate`：每 attempt 单次结算 |
| 4. JFC credit（C06） | [ub_transport.cpp#L448-L468](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp#L448-L468) | 每 context 建 `JfcCreditLedger`，对每个 JFC `install(capacity = jfc_depth - jfc_credit_reserve)`；容量为 0 即拒绝 install |
| 5. **池挂载（本任务核心）** | [ub_transport.cpp#L469-L482](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/ub_transport.cpp#L469-L482) | 构造 `SendJettyPoolOptions{desired_serving, max_creating, jetty_options}`，调用 **`context->attachPooledSendPool(make_shared<SendJettyPool>(adapter, context->handle(), context->jfcs(), native_budget, device_key, credits, pool_options))`** 完成池与 context 的挂载绑定 |

挂载契约：池是唯一的生产者，workers 数据面是唯一的消费者；未挂载池的
context 永不进入池化分支（对 RC / rm_legacy 路径完全休眠，由
`UbContextPooledAttachTest.AttachIsDormantForLegacyPaths` 回归保证）。

### 1.3 workers 与 endpoint 接线（节点 6–7）

| 节点 | 位置 | 接线动作 |
|---|---|---|
| 6. 数据面准入句柄 | `ub_transport.cpp` `UbWorkers` 构造 | `task_permits`、`attempt_gate` 作为构造参数注入 `UbWorkers`（[workers.h](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/workers.h) 新增默认参数，legacy 路径传 nullptr 不受影响） |
| 7. endpoint 发送路径 | `ub_transport.cpp` `resolveEndpoint` / `onBootstrap` | `rm_endpoint_runtime = (kRmLegacy \|\| kPooledRm)`：rm_pool 复用 RM v2 bootstrap + `bindRm` + 显式 imported target 的完整 endpoint 建立路径；`onBootstrap` 的 RM 服务端 gate 同样包含 `kPooledRm`。endpoint 另新增 `jettyOptions()` 访问器供数据面构造 `PoolKey` |

## 2. 数据面调用逻辑说明（rm_pool 全流程）

数据面位于 [workers.cpp](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/workers.cpp)，
每个池化 inflight 携带 `pooled_pool / pooled_lease / pooled_permit /
gate_token / jfc_credit_token / pooled_settled` 等状态，全生命周期如下：

### 2.1 提交路径 `processPending`

1. `slice->beginAttempt` 后进入池化准入分支：
   `task_permits_->tryAcquire` 获取任务票据（RAII `TaskPermitTicket`）；
2. 以 `endpoint->jettyOptions()` + 设备/上下文信息构造 `PoolKey`，
   以 `task_id / slice_id / attempt_generation`（均非 0）构造 `AttemptKey`；
3. `pooled_pool->tryLease(pool_key, attempt_key, pooled_lease)`：
   - 成功 → `attempt_gate_->registerAttempt` 登记完成门，持有
     `gate_token` 与 JFC credit token；
   - `TooManyRequests`（预算/credit/池容量不足）或票据无效 →
     `slice->deferAttempt`（kPosting→kRetryPending，不计 retry、不推进
     代际）+ 资源回退 + `deferPending` 延迟重排，绝不阻塞、绝不旁路；
   - 硬错误 → `resolveAttempt(FAILED)` + `enqueueRetry` 走既有重试链；
4. post 阶段按 `pooled_pool` 是否存在选择发送 Jetty：池化时用
   `lease.member()->jetty`（池成员），legacy 用 endpoint 自有 Jetty；
5. post 返回后调用 `attempt_gate_->onPostReturn(gate_token, status,
   acceptance)`，将 post 接受分类交给完成门仲裁；
6. `tryCommitPost` 失败（池化）：discard gate、释放 credit、
   `returnOnce` 归还成员；
7. 零接受回退路径（池化）：`settlePooledAttempt(kZeroAccept)` 结算，
   **不**退役 endpoint（与 legacy 行为隔离）。

### 2.2 完成路径 `handleCompletion`

各完成 category 分别调用 `settlePooledAttempt`：

| 完成类别 | 结算 kind | 成员去向 |
|---|---|---|
| 成功（长度一致） | `kRealSuccess` | `returnOnce(true)` 回流池（LIFO 复用） |
| 真实错误（RMA/长度矛盾/失败 CQE 等） | `kRealError` | `retireFaulted` 永久退役（INV07 首次错误即退役） |
| TIMEOUT（池化） | 不退役 endpoint，转 §2.3 栅栏路径 | — |

### 2.3 超时栅栏 `scanTimeouts` 与停机 `stop()`

- 池化 inflight 超时时先 `adapter_->quiesceJetty(lease.member()->jetty)`
  同步栅栏，证明旧 DMA 已结束后 `settlePooledAttempt(kFence)`；
  栅栏失败则保留责任稍后重试（不重放、不泄漏）；
- `stop()`：先收集全部池化在途 `pooled_to_fence` 逐个 `quiesceJetty`，
  收尾对仍未结算者兜底 `settlePooledAttempt(kFence)`，保证停机后
  无悬挂租约、无预算泄漏。

### 2.4 单次结算 `settlePooledAttempt`

[workers.cpp#L1374](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/src/transport/ub/workers.cpp#L1374)：
`pooled_settled` CAS 保证每 attempt 恰好结算一次；按 kind 调用完成门
`onRealCompletion / reportFault / tryFinalize`；`return_to_pool` 或
回滚场景 → `returnOnce(true)`，否则 `retireFaulted`；最后 discard gate、
释放 JFC credit。配套新增 `slice->deferAttempt`（[slice.h](file:///d:/code/coding_is_art/Mooncake/mooncake-transfer-engine/tent/include/tent/transport/ub/slice.h)）
与 `SendJettyPool::creditLedger()` 访问器（credit 由调用方释放，池不释放）。

## 3. 变更文件清单

| 层 | 文件 | 变更 |
|---|---|---|
| 契约 | `tent/include/tent/transport/ub/params.h` | rm_pool 闸门开启（physical_only / force_disable 守卫，fail-closed 语义不变） |
| 契约 | `tent/include/tent/transport/ub/slice.h` | `deferAttempt`：准入延迟不消耗重试、尊重取消 |
| 契约 | `tent/include/tent/transport/ub/send_jetty_pool.h` | `creditLedger()` 访问器 |
| 契约 | `tent/include/tent/transport/ub/endpoint.h` | `jettyOptions()` 访问器 |
| 契约 | `tent/include/tent/transport/ub/workers.h` | 构造注入 `task_permits/attempt_gate`；`PooledSettleKind` 与 `settlePooledAttempt` 声明 |
| 实现 | `tent/src/transport/ub/ub_transport.cpp` | install 装配链（预算/票据/完成门/credit/`attachPooledSendPool`）、workers 注入、endpoint/bootstrap 的 `kPooledRm` 接线、shutdown 逆序清理 |
| 实现 | `tent/src/transport/ub/workers.cpp` | rm_pool 数据面全流程（§2） |
| 测试 | `tent/tests/ub_native_data_path_test.cpp` | 新增 2 个 `UbRmPoolTest` 全链路用例；修复 fake adapter `importRemoteTarget` tp_type 契约（RTP→CTP）；既有断言随闸门语义更新 |
| 测试 | `tent/tests/ub_core_test.cpp` | RuntimeMatrix/ForceDisable 期望随闸门更新；修复陈旧 tp_type 断言（与生产 CTP 契约对齐） |
| 测试 | `tent/tests/ub_pooled_core_test.cpp` | `PooledProfileOpensForWellFormedConfig`：闸门开启断言 |

## 4. 功能测试用例执行结果

### 4.1 套件汇总（ctest `-R tent_ub_`，100% 通过）

| 套件 | 用例数 | 结果 | 覆盖 |
|---|---|---|---|
| `tent_ub_core_test` | 43 | **43 通过** | 参数/闸门矩阵、bootstrap 校验、rail 选择（含闸门开启后的新期望） |
| `tent_ub_pooled_core_test` | 47 | **47 通过** | 第一版池化核心组件全量回归（预算/准入/池/完成门/并发 hammer/参数/挂载休眠） |
| `tent_ub_teardown_test` | 15 | **15 通过** | 拆卸/清理回归 |
| `tent_ub_native_data_path_test` | 29 | **29 通过** | 数据面全链路（含新增 2 个 rm_pool E2E） |
| **合计** | **134** | **134 通过 / 0 失败 / 0 跳过** | ctest 退出码 0 |

### 4.2 新增全链路连通性用例（本次验收核心）

**`UbRmPoolTest.RuntimeGateOpensAndStaysFailClosedOnMissingGuards`**
（闸门连通性）：

- 受信配置（`runtime_profile=rm_pool` + `trans_mode=rm` +
  `physical_only=true`）→ resolve 返回 OK 且 `mode==kPooledRm`；
- 缺 `physical_only` → `InvalidArgument`；
- `jetty_pool.force_disable=true` → `InvalidArgument`。

**`UbRmPoolTest.TransportRunsSelfReadWriteThroughPooledSendPool`**
（数据面全链路连通性，逐断言验证"闸门开启 + 传输通畅无阻塞"）：

| 断言 | 验证点 | 结果 |
|---|---|---|
| install 返回 OK | rm_pool 闸门 + 装配链全通 | ✅ |
| install 后 `createJettyCalls()==0` | 端点集与池成员均惰性创建，无预分配 | ✅ |
| 首次 WRITE 状态 `COMPLETED`、字节数一致、数据一致 | 数据面经池化路径实际传输通畅 | ✅ |
| `rmPostCalls()>=1` 且 `bindJettyCalls()==0` | RM 显式 target 发送契约保持 | ✅ |
| `lastPostedTargetId==importedIds.front()` | 上层 bootstrap/import 链路与数据面打通 | ✅ |
| 首次 WRITE 后 `createJettyCalls()>=2` | 池成员按需创建 | ✅ |
| 第二次 WRITE 后创建计数不增 | 池复用生效（无重复创建、无阻塞） | ✅ |
| READ 全通且复用池成员 | 双向传输通畅 | ✅ |
| uninstall 后 `deleteJettyCalls==createJettyCalls`、`unimportCalls>=importCalls` | 停机无泄漏、无悬挂 | ✅ |

### 4.3 基线对照（A/B 回归判定）

| 项 | 基线 `7966f9d8` | 本次 |
|---|---|---|
| `tent_ub_` 失败数 | 14（13× `UbRmLegacyTest` + 1× `UbBootstrapTest`） | **0** |
| rm_pool 闸门 | `NotImplemented`（关闭） | 受信开启 + fail-closed 守卫 |
| 池化数据面 | 未接线 | 全流程接通 |

基线 14 个失败的根因均为**测试桩/断言停留在旧 RTP 契约，而生产链路
（`endpoint.cpp` `bindRm`、`urma_adapter.cpp` import、`bootstrap.cpp`
校验）统一为 CTP 契约**：

1. `FakeUrmaAdapter::importRemoteTarget` 要求 `tp_type != kUrmaTpTypeRtp`
   （应为 `!= kUrmaTpTypeCtp`）→ 修复后 13 个 `UbRmLegacyTest` 全部转绿；
2. `ub_core_test` 断言"CTP 应被 bootstrap 拒绝"（生产要求恰为
   "必须是 CTP"）→ 将断言改为拒绝 RTP，与生产契约对齐。

两处修复均为测试侧契约对齐，未改动任何生产行为；生产侧唯一行为变化
即 rm_pool 闸门按守卫开启（第一版设计预留的既定动作）。

### 4.4 回归保证

- RC / rm_legacy 路径：`UbContextPooledAttachTest.AttachIsDormantForLegacyPaths`、
  `TransportRunsSelfReadWriteWithExplicitRmTargets`、
  `TransportRunsSelfReadWriteWithPooledJettys`（legacy jetty_reuse 池）、
  全部 teardown 用例通过——未挂载池的 context 不进入池化分支；
- 第一版池化核心组件 47 用例（含 8 线程×500 轮并发 hammer、stop 交错
  排空）全量复测通过，接线未破坏组件级不变式（独占性、预算守恒、
  单次结算、首次错误永久退役）。

## 5. 结论

1. **链路接入**：`SendJettyPool` 已通过 `attachPooledSendPool` 挂载至
   每个 context，并与预算账本、任务票据、JFC credit、完成门在 install
   阶段完成原子装配；endpoint 建立复用 RM v2 bootstrap/bindRm 全路径。
2. **数据面**：rm_pool 全流程（准入→租约→post→完成门→单次结算→
   回流/退役→超时栅栏→停机兜底）实现完毕，E2E 用例验证
   WRITE/READ 数据传输通畅、池复用生效、无阻塞无泄漏。
3. **闸门**：rm_pool 在受信配置下正常开启，守卫缺失时保持
   fail-closed，绝不静默降级。
4. **质量**：134/134 用例通过，基线 14 个失败全部消除且根因已归档，
   对 RC/rm_legacy 无回归。

遗留（非本阶段职责，与第一版一致）：真实 provider 硬件用例
NOT_RUN（需 `/sys/class/ubcore` 节点，`TENT_UB_REAL_DEVICE_TESTS` 开启后执行）。
