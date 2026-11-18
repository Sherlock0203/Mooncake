# Jetty 池化模块接口设计分析结论（Mooncake × yuanrong 0.9.2 交叉核对）

> 本文为一次独立的只读源码分析结论存档，不修改任何源码、不改动所引 5 篇设计文档。
> 用途：为「对外暴露接口清单 / 上层交互 API / 生命周期与依赖关系」提供可复核的基线，并登记文档与源码的口径差异。

| 项 | 内容 |
|---|---|
| 分析日期 | 2026-11-10 |
| Mooncake 基线 | 分支 `tent/tent_rm` @ `e1d39c4d` |
| yuanrong 基线 | 分支 `0.9.2` @ `760e727fe`（工作树干净），路径 `D:\code\yuanrong-datasystem` |
| 参考文档 | `supercache_jetty_pool_current_and_target_rm_concise.md`（下称 V4 主文档）、`supercache_jetty_pool_blocking_decision_register.md`、`supercache_jetty_pool_deviation_correction_register.md`、`supercache_jetty_pool_p0_p5_execution_plan.md`、`tent_rm_p0_p5_execution_checklist.md` |
| 路径缩写 | `MI` = `mooncake-transfer-engine/tent/include/tent/`；`MS` = `mooncake-transfer-engine/tent/src/`；`Y` = `src/datasystem/`（yuanrong） |
| 未量化项 | 两侧均未做性能基准；池容量、租借命中率、flush/delete 时延、探测收敛时间等**无实测数据**，本文不做性能结论 |
| 后续补充 | 2026-11-10 追加差异项 X-11、「附录 B：术语与口径说明」（专用 RM 发送 Jetty / 业务发送池、受信配置准入）与「附录 C：待配合硬件验证项」（H-01~H-08） |

---

## 0. 结论摘要（先给结论）

1. **Mooncake 当前不存在任何 Jetty 池化实现。** 全仓词边界检索对 `SendJettyPool` / `LocalPortTarget` / `ImportedTargetRegistry` / `PathRegistry` / `ProbeResource` / `DeleteQueue` / `RefillController` / `PathHealth` / `RecoveryRegistry` 均 **0 命中**；`jetty_pool/*`、`probe/*` 仅是配置 schema 字段，`resolveUbRuntimeMode` 对 `rm_legacy` / `rm_pool` 一律返回 `NotImplemented`（`MI/transport/ub/params.h:494-568`）。因此 5 篇文档中的所有池化接口**全部属于「建议新增」，不是现有 API**。
2. **「公共接口」必须分三层**，否则极易把纯内部方法误列为公共 API：① 安装级 SDK（仅 4 个头被安装）；② TENT 树内公共头（模块间/测试可见，但**不安装**）；③ `src/` 内部实现。
3. **yuanrong 0.9.2 有一套命名完全不同、且全部位于 `src/` 的池实现**，其 `include/` 全量检索 `jetty` / `UrmaResource` / `SendJettyPool` 均 0 命中。它可作为机制参考，**不能当作 V4 目标设计已落地的证据**。
4. **存在 1 项会影响 P0 门禁的过时文档口径**：V4 主文档 4 处仍描述「批量 admission 少记 inflight」缺陷，而该缺陷在当前代码中已不存在（详见 §7 X-01）。`tent_rm_p0_p5_execution_checklist.md` P0-3 已自行修正，但主文档与执行计划未同步。
5. **落地建议**：以 `UrmaAdapter`（唯一可注入的 native 边界）为轴向下扩展；新增组件放 `src/transport/ub/` 内部头，仅在 `MI/transport/ub/` 暴露 `UbTransport` 与配置/校验类型；SDK 侧 `drain/health/close` 属**新增**而非「保留」。

---

## 1. 分析范围与方法

- 对 Mooncake `tent/include/tent/**`、`tent/src/**`、`tent/tests/**` 做逐类声明/实现定位，区分 `include/`（对外）与 `src/`（内部）。
- 对 yuanrong `0.9.2` 工作树做关键词全量检索（`JettyPool` / `jetty` / `target` / `probe` / `flush` / `cohort` / `priority` / `quarantine` / `epoch` 等），仅分析当前检出分支；发现多版本重复的 UB 实现时不混入本分支结论。
- 对 5 篇文档逐节提取组件名、接口名、调用方向与生命周期阶段，再与两侧源码逐条比对；差异以「文档表述 vs 源码事实」成对登记。
- 所有结论均带 `文件:行号`。检索无命中的项明确写「不存在」，不以推测填充。

---

## 2. 分类清单 A：现有公共接口

### A1 安装级 SDK —— 唯一真正对外（`MI/CMakeLists.txt:1-11` 仅安装 4 个头）

| 接口 | 所属类 / 位置 | 签名要点 | 职责与使用场景 |
|---|---|---|---|
| `submitTransfer` ×2 | `TransferEngine` `MI/transfer_engine.h:331,334` | `Status submitTransfer(BatchID, const std::vector<Request>&, ...)` | 应用提交传输；UB 只是被选中的 transport 之一 |
| `getTransferStatus` ×3 | 同上 `:349,352,355` | `Status getTransferStatus(BatchID, size_t task_id, TransferStatus&)` | 查询传输状态 |
| `cancelTransfer` / `freeBatch` / `allocateBatch` | 同上 `:341,329,327` | `Status cancelTransfer(BatchID, size_t)` 等 | 取消任务、批次生命周期 |
| `registerLocalMemory` / `unregisterLocalMemory` | 同上 `:303,306,320` | `Status registerLocalMemory(void*, size_t, ...)` | 本地内存注册/注销 |
| `getNicLoadStats` / `progressBatch` | 同上 `:365,363` | — | 负载观测与推进 |
| `Transport` 虚接口 | `MI/runtime/transport.h:75-87` | `install` / `uninstall` / `allocateSubBatch` / `submitTransferTasks` / `getTransferStatus` / `cancelTransferTask` / `addMemoryBuffer` / `quiesce` | 所有 transport 的统一契约；`quiesce()` 默认 `return Status::OK()`（`:87`） |

> **UB 在 SDK 中只暴露两个符号**：`TransportType::UB`（`MI/common/types.h:58`）与 `#define TRANSPORT_UB (11)`（`MI/transfer_engine.h:113`）。**没有任何 Jetty / pool / URMA 头被安装**。

### A2 TENT 树内公共（模块间与测试可见，**不安装、非 SDK API**）

| 接口 | 类 / 位置 | 签名 | 职责 / 使用场景 |
|---|---|---|---|
| `install` | `UbTransport` `MI/transport/ub/ub_transport.h:38` | `Status install(std::string&, shared_ptr<ControlService>, shared_ptr<Topology>, shared_ptr<Config> = nullptr) override` | 唯一装配入口；由 `TransferEngineImpl::construct` 调用（`MS/runtime/transfer_engine_impl.cpp:482`） |
| `UbTransport(adapter)` | 同上 `:32` | `explicit UbTransport(std::shared_ptr<ub::UrmaAdapter> adapter = nullptr)` | **依赖注入缝**，fake / interposer 从这里进 |
| `uninstall` / `submitTransferTasks` / `getTransferStatus` / `cancelTransferTask` / `addMemoryBuffer` / `removeMemoryBuffer` / `warmupMemory` | 同上 `:42-59` | 均 override `Transport` | 卸载、提交、查询、取消、内存管理 |
| `UrmaAdapter`（24 个纯虚） | `MI/transport/ub/urma_adapter.h:308` | 生命周期 `initialize()/shutdown()/available()/nativeApiVersion()/nativeSegmentDescriptorSize()`；资源 `discoverDevices/openContext/closeContext/createJfc/deleteJfc/registerLocalSegment/unregisterLocalSegment/importRemoteSegment/unimportRemoteSegment/createJetty/deleteJetty/bindJetty/unbindJetty/resetJetty`；数据面 `post(jetty, const vector<WorkRequest>&, size_t& posted_count)`、`poll(jfc, size_t, vector<Completion>&)`、`quiesceJetty(jetty, uint32_t timeout_ms, vector<Completion>&)` | **唯一 native 边界**，注释明确不感知 Task / Slice / 调度；可注入接口 |
| `createDefaultUrmaAdapter` | 同上 `:380` | `shared_ptr<UrmaAdapter> createDefaultUrmaAdapter()` | 工厂；按 `TENT_HAS_REAL_URMA` 选 Real / Unavailable（`MS/transport/ub/urma_adapter.cpp:1745`） |
| `UbParams::FromConfig` | `MI/transport/ub/params.h:165` | `static Status FromConfig(const Config&, UbParams&)` | 解析 22 个 `transports/ub/*` 配置键 |
| `resolveUbRuntimeMode` | 同上 `:494` | `inline Status resolveUbRuntimeMode(const UbParams&, UbRuntimeMode&)` | 安装前模式裁决；当前仅 `kRcLegacy` 可进入 |
| bootstrap 4 函数 | `MI/transport/ub/bootstrap.h:39,43,46,52` | `validateUbBootstrap` / `gateUbBootstrapForMode` / `validateUbBootstrapV2PeerIdentity` / `compareUbBootstrapV2` | v2 严格校验、mode gate、代次比较 |
| `buildLocalSendProfile` / `resolveDeviceKindIdentities` / `physicalDevices` | `MI/transport/ub/urma_adapter.h:133`、`device_selection.h:85,127` | `Status buildLocalSendProfile(const DeviceInfo&, int64_t priority, LocalSendProfile&)` | 本地 RM profile 派生、受信设备身份准入 |
| `ControlClient::bootstrapUb` / `ControlService::setBootstrapUbCallback` | `MI/runtime/control_plane.h:105,164` | `static Status bootstrapUb(const string&, const UbBootstrapDesc&, UbBootstrapDesc&)` / `void setBootstrapUbCallback(const OnReceiveUbBootstrap&)` | 双端 bootstrap RPC（枚举 `BootstrapUb` 在 `MI/rpc/rpc.h:51`） |
| `UbEndpoint` | `MI/transport/ub/endpoint.h:48` | `prepare()`、`bind(const UbBootstrapDesc&)`、`makeBootstrapDesc(...)`、`quiesce(uint32_t, vector<Completion>&)`、`retire()`、`tryAcquireOutstanding(uint64_t=0)`、`releaseOutstanding(uint64_t=0)` | **当前 Jetty 生命周期 owner**（每 endpoint 固定 Jetty 集） |
| `EndpointStore` | `MI/transport/ub/endpoint_store.h:26` | `getOrCreate(key, ctx, UbEndpointPtr&)`、`get`、`retire`、`retireLocalDevice`、`clear` | endpoint 缓存与隔离 |
| `UbContext` / `UbJfc` | `MI/transport/ub/context.h:21`、`jfc.h:20` | `initialize(uint32_t, const JfcOptions&)`、`shutdown()`、`addInflight(uint64_t)`、`removeInflight(uint64_t)`；`poll(size_t, vector<Completion>&)`、`close()` | Context / JFC 与 inflight 计数 |
| `UbWorkers` | `MI/transport/ub/workers.h:133` | `start()/stop()/submit(UbTask::Ptr, uint64_t mask)/cancel(...)`；构造注入 `EndpointResolver` / `EndpointRetirer` / `LocalDeviceFailureHandler` | post / poll 两池调度 |
| `QuotaManager` / `RailMonitor` / `UbBufferManager` | `MI/transport/ub/quota.h:96`、`rail_monitor.h:96`、`buffers.h:70` | `tryAcquire(...)` → `QuotaReservation`、`recordSuccess/recordError/recordTimeout`、`findLocal/importRemote` | 额度、路径健康、MR 导入 |

---

## 3. 分类清单 B：现有内部实现（**不得列为公共 API**）

| 内部项 | 位置 | 说明 |
|---|---|---|
| `UbTransport::Impl`（pimpl） | `MS/transport/ub/ub_transport.cpp:97-652` | `adapter` / `contexts` / `endpoints` / `buffers` / `rails` / `quota` / `workers` / `resolveEndpoint` / `onBootstrap` / `publishLocalDevices` / `shutdownUnlocked` 全部隐藏 |
| `UnavailableUrmaAdapter` / `RealUrmaAdapter` / `RealJetty` / `RealJfc` / `RealContext` | `MS/transport/ub/urma_adapter.cpp:61,1029,686,...` | 匿名命名空间 / 具体类；`RealJetty::initialize:694` 硬编码 `URMA_TM_RC` |
| `UbWorkers` private | `MI/transport/ub/workers.h:178-206` | `postingLoop` / `pollingLoop` / `processPending` / `buildRoute` / `chooseAndResolveEndpoint` / `handleCompletion` / `releaseInflight` / `resolveInflight` / `scanTimeouts` / `nextCompletionToken` 等 |
| `UbEndpoint` private | `MI/transport/ub/endpoint.h:132-138` | `failLocked` / `resetAndUnbindLocked` / `deleteJettysLocked` / `finishRetireLocked` / `allocateGeneration` |
| `UbContext::shutdownLocked`、`EndpointStore::retireLocked/retryQuarantinedLocked`、`ControlService::onBootstrapUb` | `context.h:89`、`endpoint_store.h:64-65`、`control_plane.h:180` | 锁内逻辑 / 回调落地，属实现细节 |
| `UbParams::read*` 私有 helpers | `MI/transport/ub/params.h:266-451` | 纯配置解析内部函数 |
| **yuanrong 全部池实现** | `Y/common/rdma/urma_send_lane.h`、`urma_resource.h`、`urma_manager.h` 等 | 头文件均在 `src/`；`include/` 全量检索 `jetty` / `UrmaResource` / `SendJettyPool` **0 命中** |

> yuanrong 唯一对外可见面是配置 Builder：`EmbeddedConfig::EnableUrma/UrmaPollSize/UrmaRegisterWholeArena/UrmaEventMode`（`include/datasystem/utils/embedded_config.h:97-118`）、`KVClientConfig::Builder::UrmaSendLaneCountPerPeer/UrmaLogThresholdUs/UbFaultIsolationEnable`（`kv_client_config.h:190,198,206`）、`KVClient::SetWorkerHealthCallback`（`include/datasystem/kv_client.h:471`，通用健康回调，与 Jetty 池无关）。`fast_transport_manager_wrapper.h` 中的 `AcquireUrmaSendLane` 等自由函数虽被 worker 调用，但头也在 `src/`，**不是 SDK 导出头**。

---

## 4. 分类清单 C：建议新增 / 调整的接口（V4 草案 + yuanrong 等价映射）

| 层 | 建议接口（草案签名） | 归属 / 目标文件 | 职责与使用场景 | yuanrong 0.9.2 等价物 |
|---|---|---|---|---|
| Adapter | `probeRmCapabilities(ctx, RmCapabilities&)`、`createRmJfcJfr(ctx, RmJfcJfrOptions&, JfcSet&)`、`importRemoteTarget(ctx, TargetDescriptor&, RemoteTargetPtr&)`、`unimportRemoteTarget(RemoteTargetPtr&)`、`modifyJettyError(jetty, lane_generation, error_epoch)`、`postOne(jetty, WorkRequest&, PostAcceptance&)`、`postLegacy(jetty, vector<WorkRequest>&, size_t&)` | `MI/transport/ub/urma_adapter.h/.cpp`（V4 §6.3） | RM native 能力探测、JFR/JFC、共享 target、异步 ERROR、单 WR 提交；`post` 拆为 legacy / pooled 两路 | `UrmaResource::GetJettyPriorityInfoForCTP / ImportTargetJetty / AsyncModifyJettyToError`（`Y/common/rdma/urma_resource.h:1148,1187,1228`）；**无 `postOne` / 三态 acceptance** |
| Adapter 调整 | `WorkRequest` 增加 `token/lane_generation/target_epoch/RemoteTargetPtr`；新增 `enum PostAcceptance{NOT_ACCEPTED, ACCEPTED, UNKNOWN}`；`bindJetty/unbindJetty` 保留编译兼容但 RM 禁调用 | 同上 | 接受态与完成门；fake 断言 RC-only 调用为 0 | `UrmaSendLaneLease::AddWr/CancelWr` + `SettleAction`（`urma_send_lane.h:79,95`）仅近似 |
| 控制面 / Registry | `PathRegistry.snapshotEligible(peer, task_policy)`、`LocalPortTarget.publish/revoke`、`ImportedTargetRegistry.acquire/release`、`PortBinding`、`PeerTargetDescriptor` | 新增（V4 §19.3） | 拓扑路径、每端口共享 target、按 Context 去重 import | `UrmaConnection::GetTargetJetty()`（`urma_resource.h:960`）、`UrmaTargetJetty::Import`（`:843`）、`UrmaManager::urmaConnectionMap_`；**无 LocalPortTarget / Registry 类** |
| 业务池 | `SendJettyPool.tryLease(pool_key, attempt)` / `release(...)`、`SliceJettyLease`、`RmSendJetty`、`PostGate`、`AttemptGate.onPostReturn(...)/onRealCr(...)` | 新增 `send_jetty_pool.h/.cpp` | LIFO 租借、单 attempt 单 WR、一次性完成归还门 | `SendJettyPool::PopIdle/Release/Remove`（`urma_send_lane.h:311,325,341`）+ `UrmaResource::AcquireJetty/ReleaseJetty`（`urma_resource.h:1200,1208`）+ `UrmaJetty::PostGate/PostPermit`（`:665,691`） |
| 恢复 | `RecoveryRegistry.beginErrorOnce(lane, error_epoch)`、`onFlushDone(entity_key, error_epoch)`、`DeleteQueue.enqueueReadyOnce(resource_owner)`、三态 `HANDLE_RETAINED/CONSUMED/UNKNOWN`、`RefillController.reserveAndCreate(role, role_key)`、tombstone | 新增 | 异步 ERROR / flush / delete / refill / ABA 防护 | `HandleFlushErrDone`（`urma_resource.h:1251`）、`pendingDeleteJettys_`（`:1366`）、`QuarantineJetty`（`:1318`）、私有 `RefillLoop/RefillSendJettyPool`（`:1335,1340`）；**删除结果为二态（成功→DESTROYED / 失败→QUARANTINED），无三态** |
| 探测 / 健康 | `ProbeResource.postProbe(probe_key)`、`PeerProbeCampaign.triggerOrMerge(peer, failed_path)`、`PathHealth.tryRecover(probe_key, evidence)`、`REVALIDATE` campaign | 新增 | 一字节 WRITE、故障触发核验、逐路径恢复 | `ProbeUbDataPlane`（`fast_transport_manager_wrapper.cpp:187`）、`PeerUbAdmission::TryBeginProbe/CompleteProbe`（`object_cache/peer_ub_admission.h:219-221`）、`UbPortHealthMonitor`（`ub_port_health.h:167`）；**无 campaign / fault_epoch / force_disable** |
| Runtime / Store | `TransferLifetimeOwner` pin/unpin、`close()` → `Draining/Unproven`、`health()`、`drain()`、安全完成 sidecar | `MS/runtime/proxy_manager.cpp`、`transfer_engine_impl.cpp` | 实际 buffer / staging owner、取消与超时、Shutdown | `UrmaResource::BeginShutdown/WaitForPostPermitsDrained/IsProviderCleanupDeferred`（`urma_resource.h:1236-1244`）语义接近 |
| SDK | V4 §18.3 称「保留 `register/import/submitRead/Write/Transfer/status/wait/cancel/drain/health/close`」 | `MI/transfer_engine.h` | 独立 UbEngine SDK | yuanrong `include/` 同样不导出这些 |

**关键设计差异（不能照搬 yuanrong）**：

- V4 是「按 `PoolKey = ContextKey + LocalSendProfile` 分池 + LIFO」；yuanrong 是**进程级单池**，`AcquireJetty(jetty&)` 无 key，`Release` 用 `push_back` 而 `PopIdle` 用 `front()` → 实为 **FIFO**（`urma_send_lane.h:311,325`）。
- V4 要求**单 attempt 单 WR**；yuanrong `UrmaSendLaneLease` 用 `pendingWrs_` 支持多 WR + `Seal()`。
- V4 的 `DELETE_READY` 要求 `urma_flush_jetty` 取空 `WR_UNHANDLED`；yuanrong 直接凭 `URMA_CR_WR_FLUSH_ERR_DONE` 调 `DeleteAfterFlush()`（`urma_send_lane.h/.cpp:566`），无此步骤。

---

## 5. 与上层模块交互的 API（调用方 → 被调用方 / 方向 / 触发时机 / 数据流）

| # | 调用方 → 被调用方 | 方向 | 触发时机 | 核心数据流 |
|---|---|---|---|---|
| 1 | `TransferEngineImpl::loadTransports` → `UbTransport()` | 下行 | 引擎构造，受 `USE_UB` + `transports/ub/enable` + NIC_UB 门控（`MS/runtime/transport_loader.cpp:100-105`） | 配置 → transport 实例 |
| 2 | `construct()` → `UbTransport::install` → `Impl::install` | 下行 | 安装（`MS/runtime/transfer_engine_impl.cpp:482`） | `UbParams::FromConfig` → `resolveUbRuntimeMode` → `adapter.initialize/discoverDevices` → `resolveDeviceKindIdentities` → `UbContext::initialize`（`openContext/createJfc`）→ 构造 `UbBufferManager/EndpointStore/RailMonitor/QuotaManager` → `publishLocalDevices` → `setBootstrapUbCallback` → `UbWorkers` + `start()`（`MS/transport/ub/ub_transport.cpp:127-376`） |
| 3 | `TransferEngine::submitTransfer` → `UbTransport::submitTransferTasks` | 下行 | 每次业务请求 | `UbTask::create` + `addSlice` + `seal()` → `UbWorkers::submit(task, mask)`（`ub_transport.cpp:758-785`） |
| 4 | `UbWorkers::processPending` → `EndpointResolver` → `EndpointStore::getOrCreate` | 下行 → 回调 | 首包 / 冷路径 | `UbEndpoint::prepare()` 循环 `adapter.createJetty`（`MS/transport/ub/endpoint.cpp:204-213`）→ `makeBootstrapDesc` → `ControlClient::bootstrapUb` → 对端 `ControlService::onBootstrapUb` → `endpoint->bind()` 逐项 `bindJetty`（`endpoint.cpp:296`） |
| 5 | `UbWorkers::processPending` → `QuotaManager::tryAcquire` / `UbBufferManager::findLocal/importRemote` → `adapter.post` | 下行 | 每 slice | `endpoint->jetty(worker_index)` → `adapter_->post(jetty, {work}, posted_count)`（`MS/transport/ub/workers.cpp:906`）；**当前固定 Jetty，不经过池** |
| 6 | `UbWorkers::pollingLoop` → `UbJfc::poll` → `adapter.poll` → `handleCompletion` → `resolveInflight` | 上行 | CQE 到达 | `Completion{token, category, completed_bytes, local_jetty_id}` → `inflight_[token]` → `UbSlice::resolveAttempt` → `UbTask` 终态 → `getTransferStatus` 读取 |
| 7 | `deconstruct` → `UbTransport::uninstall` → `Impl::shutdownUnlocked` | 下行 | 关闭 | `setBootstrapUbCallback({})` → `workers->stop()`（逐 endpoint `quiesce` → `adapter.quiesceJetty` 同步等 marker）→ `endpoints->clear()` → `buffers->clear()` → `UbContext::shutdown()` → `adapter->shutdown()`（`ub_transport.cpp:403-451`） |
| 8 | **yuanrong**：`BatchGetObjectRemoteImpl` → `AcquireUrmaSendLane` → `UrmaManager::AcquireSendLane` | 下行 | worker→worker Batch Get（`Y/worker/.../worker_worker_oc_service_impl.cpp:1253`） | `UrmaConnection::AcquireInflightSlot(remainingUs)`（**先 credit 后取池**）→ `UrmaResource::AcquireJetty` → `SendJettyPool::PopIdle` → 建 `UrmaSendLaneLease` → `RegisterActiveSendLane`（`urma_manager.cpp:1944-1976,2734-2758`） |
| 9 | **yuanrong**：`UrmaWriteImpl` → `PollJfcWait` → `CheckCompletionRecordStatus` → `CompleteActiveSendLane` → `ApplyActiveSendLaneAction` | 上行 | WR / CQE | `SettleAction{RELEASE→ReleaseJetty, RETIRE→RetireJetty}`（`urma_resource.cpp:1442-1465`）；`FLUSH_ERR_DONE` → `HandleFlushErrDone` → `ScheduleDeleteJetty` |
| 10 | **yuanrong**：`SealUrmaSendLaneLease`（`worker_worker_oc_service_impl.cpp:1276`） | 下行 | RPC 批次封口 | `SealSendLaneLease` → `laneLease.Seal()` → 最终 RELEASE / RETIRE |

---

## 6. 接口依赖关系与生命周期

### 6.1 依赖链

- **现有**：`UbTransport` → `Impl` → `{UbParams, UrmaAdapter, UbContext, EndpointStore, UbBufferManager, QuotaManager, RailMonitor, UbWorkers}`；`UbWorkers` 通过 `EndpointResolver` 回调反查 `Impl::resolveEndpoint`（**回调式反向依赖**，避免 workers 直接持有 transport）；`EndpointStore` → `UbEndpoint` → `UbContext` → `UrmaAdapter`。
- **目标（V4 §16.3）**：`adapter.initialize` → `discoverDevices/DeviceKind` → RM gate → `openContext` → RM JFC/JFR → capability + budget → **Port / Path / Target / Recovery Registry** → pollers + control/delete/callback → `LocalPortTarget` + `ImportedTargetRegistry` → `ProbeResource`（不启周期 WR）→ `BusinessJettyPool` 预热 → bootstrap v2 callback + `RefillController` → 发布 inventory / `ELIGIBLE_UNVERIFIED` → **最后**启动 post workers + health scheduler。
- **与现有 `Impl::install` 的差别**：现有在 `install` 末尾即 `workers->start()`（`ub_transport.cpp:376`），且完全无 Registry / 池 / probe / delete / refill 层。

### 6.2 生命周期四阶段对照

| 阶段 | 现有实际流程 | 目标流程（建议接口序列） |
|---|---|---|
| **初始化** | `install` → 配置/模式 → adapter → Context/JFC → 组装 → `workers.start` | 见 6.1 目标链；任一步失败逆序释放，已可能接受 WR 的资源先排空 |
| **获取** | `EndpointStore::getOrCreate` → `prepare()` 建固定 Jetty → `bind()` → `jetty(index)` 直接 post | `PathRegistry.snapshotEligible` → K permit + quota → `ImportedTargetRegistry.acquire` → `SendJettyPool.tryLease(pool_key, attempt)` → JFC credit → `AttemptGate` 复核代次 → `adapter.postOne` |
| **释放** | `handleCompletion` → `resolveInflight` → 计数归零（**无 lease 归还**） | 正常路径：`onPostReturn` + `onRealCr` + `completed_bytes` + handler 退出 + CAS 一次归还 → LIFO `push_front`；错误路径：`beginErrorOnce` → DRAINING → `onFlushDone` → DELETE_READY → `DeleteQueue.enqueueReadyOnce` |
| **销毁** | `quiesce`（同步 `quiesceJetty` 等 marker）→ reset/unbind/`deleteJetty` → `EndpointStore::clear` → `Context::shutdown` → `adapter.shutdown` | 停新业务/probe → 停 refill 发布 → join post → 保持 poll 排空 + target 入站屏障 → 删除线程三态登记 → 未证明安全则 `close` 返回 `Draining/Unproven` 并移交进程级 owner；重复 shutdown 幂等；最后 `urma_uninit` |

---

## 7. 文档 ↔ 源码不一致清单

| ID | 文档表述 | 源码事实 | 影响 |
|---|---|---|---|
| X-01 | V4 §2.1 行 74-75、§6.4、§19.1、§21.1：「现有 `tryAcquireOutstandingBatch` 对 endpoint 增加 admitted 个 WR，Context 只 `addInflight` 一次 → 批量少记缺陷必须修复」 | **已过时**。全仓无 `tryAcquireOutstandingBatch`；现为 `UbEndpoint::tryAcquireOutstanding(uint64_t bytes)`（`endpoint.h:87`）、`UbContext::addInflight(uint64_t bytes)`（`context.h:73`），`workers.cpp:906` 固定 `post(jetty, {work}, posted_count)` **单 WR**。`tent_rm_p0_p5_execution_checklist.md` P0-3 已改为「回退后原批量少记缺陷不存在」，但 V4 主文档 4 处未同步 | **高**：P0 门禁会继续追逐一个不存在的缺陷；建议优先回写 |
| X-02 | §18.3「独立 SDK 公共 API **保留** register/import、submitRead/Write/Transfer、status/wait/cancel/drain/health/close」 | 安装头 `transfer_engine.h:271-368` 是 **BatchID 模型**：`submitTransfer(BatchID, ...)`、`getTransferStatus`、`cancelTransfer`、`registerLocalMemory`、`freeBatch`；**无** `submitRead/submitWrite/drain/health/close`；`UbTransport` 也**未 override** `Transport::quiesce()` | 中：应标为「目标新增/改造」，非「保留」 |
| X-03 | §3.3「`ub_core` 定义窄接口、`ub_transfer` 读取健康资格、`ub_health` 发探测，由 `UbEngine` 装配」 | 仓库无 `ub_core/ub_transfer/ub_health` 目录、无 `UbEngine` 类；只有测试目标名 `tent_ub_core_test`（`tests/CMakeLists.txt:554`）。仅有 `include/tent/transport/ub/` 与 `src/transport/ub/` 两个目录 | 中：组件命名不可直接用于代码落地 |
| X-04 | §6.3 七个 adapter 接口、§18.3 九个内部接口、§3.1 十四个组件 | 全仓 grep **0 命中**。§6.3 已注明「不是当前已支持的公共 API」，但 §18.3 标题「核心内部接口与公共 API」易被误读 | 中：建议全部显式标注 `建议新增 / 当前不存在` |
| X-05 | §18.1 配置表列出 `topology.ports/peers/adjacency/template`、`send_pool.desired_per_context`、`native_budget/replacement_reserve`、`K`、`probe.local_ports/target_map`、`drain.timeout/delete.retry/refill.scan`、`queue/quarantine/tombstone budgets` | `params.h:165-263` 只解析 **22 个** `transports/ub/*` 键；上述键全部不存在；`probe/*` 8 个键虽已解析但**无任何消费者**（`resolveUbRuntimeMode` 直接 `NotImplemented`） | 中：配置面需整体新设计 |
| X-06 | §5.2 v2 schema 含 `schema_version / descriptor_abi / access_capabilities / local_port_identity / local_eid / descriptor` | `UbRmBootstrapDesc`（`bootstrap_types.h:110-122`）字段为 `transport_mode/device_kind/physical_device_identity/node_incarnation/context_incarnation/topology_epoch/transport_modes/provider_capabilities/targets/probe_target_map`；`UbRmTargetDescriptor`（`:83`）为 `target_identity/eid/uasid/id/target_epoch/profile`。**无** schema_version、descriptor、access_capabilities；`probe_target_map` 是 `map<string, vector<string>>` 而非结构化映射 | 中：bootstrap v2 契约需重新对齐 |
| X-07 | §1.2「每 Context / 兼容 LocalSendProfile 建池」+ LIFO；§3.1 `SendJettyPool` | 当前 `EndpointStore` 按 `UbEndpointKey` 缓存固定 Jetty 集，`jetty_per_endpoint = 6`（`params.h:145`），`prepare` 用 `index % jfcs.size()` 分配，**无池、无 profile 分池、无 LIFO** | 高：目标态与现状差距的根因 |
| X-08 | §16.3 要求「业务预热 / 探测资源完成前不得启动 post workers」 | 现有 `Impl::install` 在构造完即 `workers->start()`（`ub_transport.cpp:376`），无预热 / 探测阶段 | 中：初始化顺序需改造 |
| X-09 | 偏差登记 D-05/D-06、执行计划 P2、清单 §13.2 称池组件未实现 | **与源码一致**（正面证据）：`quiesceJetty` 确为同步忙等（`urma_adapter.cpp:1498-1585`，`while(!flush_done)` + `urma_poll_jfc` + 20µs sleep），`RealJetty::initialize` 确为 `URMA_TM_RC`（`:694`） | — |
| X-10 | 5 篇文档之间 | 清单 P0-3 已修正 X-01，V4 主文档与执行计划 P0 条目仍沿用旧表述；偏差登记 D-01~D-10 未把 X-01 列入偏差清单 | 中：文档间口径需统一 |
| X-11 | V4 §5.1 行 131：「DeviceKind 必须来自受信任的显式身份记录**或可验证 provider 元数据**」；D-04 行 92 及偏差登记行 20 使用「管理员配置 / allowlist」表述 | `DeviceKindSource::VALIDATED_METADATA` 仅出现在枚举定义（`MI/transport/ub/urma_adapter.h:46`）与 `deviceKindSourceName` 的 switch（`:69`），**全仓无赋值点**；唯一生效来源是 `EXPLICIT_CONFIG`（`MI/transport/ub/device_selection.h:114`）。代码中亦无 `admin` / `allowlist` 符号，实际只是 `Config::set` 写入的 JSON 数组 | 中：该分支目前**无实现**，不能作为「provider 元数据可证明设备类型」的依据；「管理员 / allowlist」属运维语义概括，不是代码实体。详见附录 B.2 |

---

## 8. yuanrong 0.9.2 命名对照（避免把 V4 目标名当成参考实现）

| V4 目标名 | yuanrong 0.9.2 实际 |
|---|---|
| `SendJettyPool` | **存在但语义不同**：`urma_send_lane.h:283`，无锁容器、FIFO、进程级单池；真 owner 是 `UrmaResource`（`urma_resource.h:1085,1383`） |
| `LocalPortTarget` / `ImportedTargetRegistry` / `PathRegistry` / `TargetRegistry` | 逐字均不存在；等价物 `UrmaTargetJetty`（`:834`）、`UrmaConnection::GetTargetJetty`（`:960`）、`urmaConnectionMap_` / `jettyRegistry_` |
| `ProbeResource` / `PathHealth` / `campaign` / `fault_epoch` / `force_disable` | 逐字不存在；等价物 `ProbeUbDataPlane`、`UbPortHealthSnapshot/healthEpoch`、`UbProbeToken{peer, epoch}`、`PeerUbAdmission::TryBeginProbe` |
| `DeleteQueue` / `RefillController` / `RecoveryRegistry` / `WR_UNHANDLED` / `urma_flush_jetty` | 逐字不存在；等价物 `pendingDeleteJettys_` + `quarantinedJettys_` + `deleteJettyThread_`、私有 `RefillLoop`、`URMA_CR_WR_FLUSH_ERR_DONE` 状态、`DeleteAfterFlush()` |
| 删除三态 `RETAINED/CONSUMED/UNKNOWN` | 无；`UrmaSendLaneLease::SettleAction{NONE, RELEASE, RETIRE}`（`urma_send_lane.h:55`）是**租约结算**三态，不是 delete 结果 |
| `PathRegistry.snapshotEligible` 等 §18.3 九个接口 | 全部无对应；最接近的是 `UbPortHealthMonitor::GetSnapshot/ReadSummaryForQuery`（`ub_port_health.h:188,192`） |

---

## 9. 落地建议

1. **接口轴心**：以 `UrmaAdapter`（唯一可注入 native 边界）为轴向下扩展；所有新增接口先在该抽象上定义，再由 Real / fake 双实现验证。
2. **可见性边界**：新增组件（Pool / Registry / Probe / Delete / Refill）一律放 `src/transport/ub/` 内部头；只把 `UbTransport` 与配置/校验类型留在 `MI/transport/ub/`，避免把内部方法误升格为公共 API。
3. **池接口必须带 key**：`SendJettyPool.tryLease` 必须带 `PoolKey`（`ContextKey + LocalSendProfile`），不要复制 yuanrong 的无 key 进程级单池。
4. **完成语义分离**：`postOne` 与 `postLegacy` 严格分离完成/重试语义；单 attempt 单 WR 与一次性归还门（CAS）作为硬约束写进 fake 断言。
5. **SDK 定位**：`drain/health/close` 属**新增**而非「保留」；现有 BatchID 模型与目标 UbEngine SDK 需明确是「并存」还是「替换」。
6. **优先修文档**：先把 X-01 回写进 V4 主文档与执行计划 P0 条目，并把 X-01 补入偏差登记，否则 P0 门禁会继续追逐不存在的缺陷。
7. **待补量化数据**：池容量 / 每 Context Jetty 数、租借命中率与排队时延、flush-done 到 delete-ready 的时延、探测收敛时间与阈值、优先级隔离效果——两侧均无实测值，选型结论需在压测后复核。

---

## 附录 A：可复核的检索证据

- Mooncake 池化组件存在性：`grep -rniE "SendJettyPool|LocalPortTarget|ImportedTargetRegistry|PathRegistry|ProbeResource|DeleteQueue|RefillController|PathHealth|RecoveryRegistry" MI/... MS/...` → 0 命中。
- 文档草案接口存在性：`grep -rniE "probeRmCapabilities|createRmJfcJfr|importRemoteTarget|unimportRemoteTarget|modifyJettyError|postLegacy|postOne|snapshotEligible|tryLease|beginErrorOnce|onFlushDone|enqueueReadyOnce|reserveAndCreate|triggerOrMerge|tryRecover" include src` → 0 命中。
- 批量 admission 旧口径：`grep -rn "tryAcquireOutstandingBatch|addInflightBatch"` → 0 命中；现存 `tryAcquireOutstanding` / `addInflight` / `removeInflight`。
- 组件目录：`find . -type d -iname "*ub*"` → 仅 `include/tent/transport/ub/` 与 `src/transport/ub/`；`grep -rniE "ub_core|ub_transfer|ub_health|UbEngine"` 仅命中测试目标名。
- yuanrong 导出面：`grep -rniE "jetty|SendJettyPool|UrmaResource" include/` → 0 命中。
- 安装头清单：`MI/CMakeLists.txt:1-11`。
- 模式门控：`MI/transport/ub/params.h:494-568`（`rm_legacy` / `rm_pool` → `NotImplemented`）。
- `VALIDATED_METADATA` 赋值点：`grep -rn "VALIDATED_METADATA\|kind_source" include src tests` → 仅枚举定义、switch 与读取；赋值只有 `NONE`（`device_selection.h:101`）和 `EXPLICIT_CONFIG`（`:114`）。
- `device_identities` 配置样例：`MT/tests/ub_core_test.cpp:886-910`、`MT/tests/ub_native_data_path_test.cpp:537-549`（`MT` = `mooncake-transfer-engine/tent/`）。
- yuanrong 探测复用业务池：`ProbeUbDataPlane`（`Y/common/rdma/fast_transport_manager_wrapper.cpp:187`）→ `UrmaWritePayload`（`Y/common/rdma/urma_manager.cpp:2770`）→ `UrmaWritePayloadImpl`（`:2804`）→ `AcquireSendLaneFromConnection`（`:2871`）。
- 探测专用内存（非专用 Jetty）：`GetRecoveryProbeSourceInfo`（`Y/common/rdma/urma_manager.cpp:968`）、`response.recovery_probe_addr()`（`fast_transport_manager_wrapper.cpp:189`）。

---

## 附录 B：术语与口径说明（2026-11-10 补充）

> 来源：本文交付后的两轮术语答疑。目的：把 5 篇文档中容易误读的短语逐条映射到代码符号，并显式标注**文档自造词**（无对应代码实体）与**占位未实现**项。
> 缩写补充：`MT` = `mooncake-transfer-engine/tent/`；`MI` / `MS` / `Y` 见文首表格。

### B.1 「每个本地端口一个专用 RM 发送 Jetty，不进入业务发送池」

出处：V4 §1.2 行 38；相关定义 §3.1 行 181、§14 行 850-900、清单 P4-1（行 151）。

**一句话**：隔离的是**本地发送侧的 Jetty 资源**，共享的是**远端 target 句柄**。

| 术语 | 定义 |
|---|---|
| 业务发送池 | 按 `PoolKey = ContextKey + LocalSendProfile` 分池，成员是可租借的业务发送 Jetty；每 attempt 租一个、最多一次 native post（单 WR）、完成后 CAS 一次归还，同池 LIFO 复用；首次真实错误 CQE 对应成员强制退役、永不回池（V4 §1.2 行 32-35、42） |
| 专用 RM 发送 Jetty | 每个本地 `PortBinding` 常备一个 `ProbeResource`，含 1 个发送 Jetty + 探测源内存 + 远端探测槽位引用 + 独立额度；只发一字节 `URMA_OPC_WRITE` 探测，不承载业务数据；健康时可保留资源但不投周期 WR（§14.1 行 854、§14.2 行 862） |

**逐维对比**

| 维度 | 业务发送池 | 专用 RM 发送 Jetty（ProbeResource） |
|---|---|---|
| 粒度 / 数量 | 每 `PoolKey` 一池，N 个成员 | 每本地端口 1 个，全节点 = 端口数，角色容量恒为 1（§14.3 行 870） |
| 生命周期 | 租借 → 归还 → 退役 → `RefillController` 补池 | 常驻；资源创建与周期任务启动分离，健康时不发 WR |
| 并发 | 一个 Jetty 同时只服务一个 attempt，池内并行 | 每口最多 1 个在途 probe，同口多 peer/path 轮转、不同口并行（§14.1 行 856） |
| 配额 | 占 Task K + WR credit + 业务 JFC 预算 | **不占 Task K**，占独立控制配额 + 独立 JFC/credit（§14.2 行 864） |
| 写入目标 | 业务内存地址 | 对端授权的专用探测区，源 buffer 不变，禁止写业务地址（行 862、§10 行 388） |
| 失败语义 | 真实错误 → 该 Jetty 退役 | probe 失败只更新明确归属路径，其余同口路径保持 Pending（§14.3 行 872） |
| 账本 | 计入 `Serving = IDLE + IN_USE`，按 `Deficit` 补池（§12.1 行 751-756） | **不计入业务 Serving**，按独立角色补建；`role_key = ProbeResourceKey`（§19.3 行 1101） |

**为何必须区分**

1. **打破恢复死锁**：路径隔离 / `force_disable` 时新业务 pooled post 为零，但仍必须放行 REVALIDATE probe（§19.3 行 1103、§14.2 行 864）。若探测从业务池取 Jetty，池成员恰是被隔离 / 退役的那批，探测发不出去 → 路径永远无法恢复。
2. **故障归因不串味**：探测 WR 的 ERROR / flush / delete 若落在业务池成员上，会触发业务 Jetty 退役与补池，把「探测发送资源坏了」误记成「路径物理故障」；反之业务池耗尽会让探测拿不到 Jetty，只能记 Pending / Unknown（§14.3 行 872-874）。
3. **避免连带 flush**：同一发送队列多个在途 WR 时，一个 target 故障会连带 flush 其他 WR，故探测限制为每口最多 1 在途、同口串行（§14.1 行 856）；该约束只给探测，不能把业务 slice 聚成 Group（§1.3 行 53）。
4. **门禁与配额独立**：业务满载时 Task K / credit 已耗尽，健康调度器仍需能发 probe。
5. **安全与证据语义不同**：探测写独立注册区与远端专用槽位；且「WRITE probe 成功只证明当前探测配置可用，不证明业务 READ/WRITE 权限」（§14.2 行 866）。业务成功才是合格路径证据，两种证据不能在同一资源上结算。
6. **容量账本分开**：§11 行 779 的预算是「业务 200 + 4 探测发送成员 + 4 共享 target = 208」分开核算；§12.1 行 756 明确 `probe/target 不计入业务 Serving`，否则 `Deficit = DesiredServing - Serving - Creating` 会被探测成员污染。

**易混淆点**：节点每个配置端口发布一个共享 `LocalPortTarget`，`ImportedTargetRegistry` 按本地 Context 导入，业务与探测共同引用，不归某个 probe Jetty 或 endpoint 独占（§1.2 行 36-37）。记法：**共享地址，隔离队列与责任**。

**源码核对**

- 这是目标设计，非 Mooncake 现状：当前既无 `SendJettyPool` 也无 `ProbeResource`，仅 `probe_target_map` 字段（`MI/transport/ub/bootstrap_types.h:120`）与 v2 校验（`MS/transport/ub/bootstrap.cpp:123`）；`rm_*` 模式返回 `NotImplemented`。
- yuanrong 0.9.2 恰是反例：`ProbeUbDataPlane` 经 `UrmaWritePayload` → `UrmaWritePayloadImpl` → `AcquireSendLaneFromConnection`（附录 A 证据），**探测复用业务 SendJettyPool 成员**，只隔离了探测源 / 目的内存，没有专用 probe Jetty。不能拿它当「专用探测 Jetty」的落地证据。
- 待配合硬件验证：探测槽位、授权范围、并发互斥、超时后未知写处理（V4 行 117，见附录 C H-07）；探测超时、`recover_success_threshold`（默认 1，行 831）、同口轮转时延均无实测值。

### B.2 「adapter 将发现设备初始化为 `UNKNOWN`；`physical_only` 依赖管理员配置的显式身份记录匹配 name/path/EID/index/transport_modes，openContext 前还做 capability 校验。当前机制是受信配置准入，不是独立硬件证明」

出处：V4 D-04 行 92；偏差登记行 20；V4 §5.1 行 131、§2.1 行 67。

> 笔误提示：原文是「**显式**身份记录」（explicit，必须由人明确写死），不是「显示」（display）。

**逐词映射（真词 / 译名 / 自造 / 占位）**

| 文档措辞 | 代码里对应 | 判定 |
|---|---|---|
| `UNKNOWN` 初始化 | `DeviceKind::UNKNOWN`（`MI/transport/ub/urma_adapter.h:35-41`）、`DeviceInfo.kind` 默认值 `:119`、`kind_source = NONE` `:120` | **真实** |
| `physical_only` | `UbParams::physical_only`（`MI/transport/ub/params.h:139`） | **真实** |
| 「显式身份记录」「trusted identity」 | 结构体 **`DeviceIdentity`**（`MI/transport/ub/device_selection.h:23-30`）+ 配置键 **`transports/ub/device_identities`**（`params.h:140,380`） | 代码叫 `DeviceIdentity`，中文译名 |
| 「管理员配置」「allowlist」 | `Config::set(...)` 写入的一段 JSON 数组 | **文档自造**：全仓无 `admin` / `allowlist` 符号，也无角色 / 权限校验，只是部署时写入的配置 |
| 「capability 校验」 | 下述四步中的 4 处具体检查 | 真实，但被概括成一个词 |
| 「可验证 provider 元数据」 | `DeviceKindSource::VALIDATED_METADATA`（`urma_adapter.h:46`） | **枚举存在但全仓无赋值点**，未实现的占位路径 → X-11 |
| 「受信配置准入」「独立硬件证明」 | 无对应符号 | **文档自造的定性结论**，说明「信任来自哪、以及缺什么」 |

**「显式身份记录」的真实内容**：由 `readDeviceIdentities`（`params.h:378-449`）解析成 `std::vector<DeviceIdentity>`。测试中的真实样例（`MT/tests/ub_core_test.cpp:891-900`）：

```json
{
  "transports/ub/physical_only": true,
  "transports/ub/device_identities": [{
    "native_device_name": "udmac0d0e1",
    "native_device_path": "/sys/.../udmac0d0e1",
    "eid_index": 0,
    "eid": "<16 字节 EID 的字符串形式>",
    "kind": "physical",
    "transport_modes": 1
  }]
}
```

| 字段 | 含义 | 出处 |
|---|---|---|
| `native_device_name` | URMA `urma_device_t::name` | `MS/transport/ub/urma_adapter.cpp:1076` |
| `native_device_path` | `urma_device_t::path`（设备路径） | `:1077` |
| `eid` / `eid_index` | URMA EID 及其在该设备上的序号；**一个 device/EID 对 = 一条 `DeviceInfo`** | `urma_adapter.h:109-111,116-117` |
| `kind` | `physical` / `bonding` / `virtual` / `aggregate` / `unknown` 之一 | `device_selection.h:32-47` |
| `transport_modes` | uint16 位图，`RM=0x1 / RC=0x2 / UM=0x4` | `urma_adapter.h:79-83` |

`physical_only=true` 但数组为空直接报 `InvalidArgument`（`params.h:174-176`）；记录本身要求 name/path/EID 非空、`kind != UNKNOWN`、mode 位合法（`device_selection.h:49-66`）。

**四步准入链**

1. **发现**：`discoverDevices` 从 `urma_get_device_list` / `urma_get_eid_list` 生成 `DeviceInfo`，`kind` 保持 `UNKNOWN`、`kind_source = NONE`（`MS/transport/ub/urma_adapter.cpp:1057-1100`）。原因见注释 `urma_adapter.h:32-34`：「URMA 不提供 provider 无关的设备类型属性」。
2. **匹配**：`resolveDeviceKindIdentities`（`device_selection.h:85-125`）先把所有设备重置为 `UNKNOWN`，再用 `sameDeviceLocator` 比对 **name + path + eid_index + eid 四元组全等**（`:68-73`），要求实时 `capabilities.transport_modes` 与配置值**精确相等**（`:108-112`）、双向唯一；一条配置没匹配上即 `DeviceNotFound` 并提示 "absent or stale"（`:118-122`）。成功后才把配置的 `kind` 搬进 `device.kind`，标 `EXPLICIT_CONFIG`（`:113-114`）。
3. **筛选**：`physical_only ? physicalDevices(discovered) : preferBondingDevicesIfPresent(...)`（`MS/transport/ub/ub_transport.cpp:169-174`）；`physicalDevices` 只留 `kind == PHYSICAL && kind_source != NONE`（`device_selection.h:127-137`）。
4. **复核**：`UbContext::initialize` 在 `openContext` 前检查 `jfc_count*2 <= max_jfc`（`MS/transport/ub/context.cpp:42-48`）；`RealUrmaAdapter::openContext` 再查一次设备，确认 name/path/EID 仍在、设备仍 active、`transport_modes` 未变（`urma_adapter.cpp:1106-1181`）。

**为什么是「受信配置准入」而非「独立硬件证明」**：`kind = "physical"` 是人在配置文件里写的一行字符串，代码只是信任并搬运到 `device.kind`。四步中所有校验只回答「**这条配置是否仍对应当前这台实时设备**」（名字、路径、EID、mode 位是否一致），**没有任何一步读取硬件 / 固件属性来证明「这是物理端口而非 bonding 或虚拟聚合」**。系统防的是配置陈旧 / 串台（fail-closed 拒绝），防不住管理员把虚拟设备标成 physical；正确性依赖人维护的 inventory。代码注释亦直说：`device_selection.h:20-22`「这是 administrator-approved 的分类记录，不是名称模式推断」。D-04 纠正的正是「不能把 `physical_only` 宣传成 provider 自动证明物理性」。

**两个坑**

- V4 行 131 的「或可验证 provider 元数据」**目前无实现**：`VALIDATED_METADATA` 只在枚举定义与 `deviceKindSourceName` 的 switch 中出现，全仓无赋值点；唯一生效来源是 `EXPLICIT_CONFIG`。已登记为 X-11。
- 默认 `physical_only = false`（`params.h:139`）、`device_identities` 为空，当前生产链路走 legacy 名称启发式 `isBondingDeviceName`（`device_selection.h:141-151`），根本不看身份记录；V4 行 67「默认仍是非 physical_only」即指此。

**未量化 / 未决**：无版本化 inventory、无配置签名校验、无硬件 attestation；D-04 把「由设备 / 实机 Owner 维护 allowlist 并提供版本化 inventory」列为待确认项，目前无实测或验收数据。

---

## 附录 C：待配合硬件验证项（2026-11-10 补充）

> 定位：以下是**需要真实 provider / 硬件环境才能得出结论的验证项**，不是设计缺陷，也不需要外部签字流程。源码阅读、fake adapter、单元测试、interposer 只能验证 Mooncake 侧状态机；拿到实机结果前，相关设计一律按「假设」处理，不写成已验证。统一标记为 `待配合硬件验证`。
> 表中 `*.cpp` / `*.h` 路径除特别说明外均位于 `MS/transport/ub/`；「V4 行号」指 `supercache_jetty_pool_current_and_target_rm_concise.md`。

| # | 验证项 | 为什么必须上机 | 环境 / 观测 | 源码依赖点 |
|---|---|---|---|---|
| H-01 | 共享 target 被多发送 Jetty / 多 peer / 业务+probe 并发访问 | 进程内 `pollMutex` 只约束本进程 poller，证明不了内核 / 固件互斥 | 4/6 口双端逐步加压；看 CQE 归属、数据正确性、hang / 泄漏、p99 尾延迟 | `urma_adapter.cpp:1498-1585`、`endpoint_store.cpp:16` |
| H-02 | `poll` 与 `delete` / `clean_jfc`、`import` / `unimport` 的并发语义（含 lock_free 配置） | 源码只能看到调用顺序，看不到 provider 对象表与 CQ 锁 | poll / delete / import 并发竞争；看 CQE 是否被压缩、对象表残留 | 同上；V4 行 722 |
| H-03 | 入站 DMA fence：modify ERROR → flush-done → `urma_flush_jetty` 取空 → AE ACK | 当前同步等待 provider marker；marker 名称 / ABI、flush 返回 0 的含义随版本变化 | canary 目的内存 + 注销后复用该页 + IOMMU / dma-api-debug；注入 marker 丢失 / 重复、`WR_UNHANDLED` 未取空、ACK 迟到 | `urma_adapter.cpp:999-1010,1498-1585`；V4 行 388、663-665 |
| H-04 | `WR_UNHANDLED` 是否携带原 token、`completed_bytes` 与请求一致 | `convertCompletion` 按某一版本的 status / `user_ctx` 语义硬编码 | 双端 1B / 4KiB / 64KiB±1 / 1MiB、partial post、早到 CQE；逐笔核对 token 与长度 | `urma_adapter.cpp:999-1010`；V4 行 76、497 |
| H-05 | delete 失败后 handle 是 retained / consumed / unknown，及对象表副作用 | 现有代码假定失败后仍可重试，历史 provider 行为可能相反 | 按失败码注入；看返回码、对象表项、能否再次 delete、预算释放、ID 复用 | `urma_adapter.cpp:1015-1027`、`endpoint.cpp:109-148` |
| H-06 | teardown 顺序失败后的回滚与重复 shutdown 幂等 | 代码侧顺序正确，但 MR 注销时入站 DMA 是否结束由 provider 决定 | 100 次带载 install / uninstall + 崩溃退出；看 hang / 泄漏 / 迟到写入 | `ub_transport.cpp:403-451`；V4 行 999 |
| H-07 | 探测一字节 WRITE 的槽位权限、并发互斥、超时后未知写 | 探测区授权与 DMA 边界属于 provider / MR 语义 | 专用探测区 canary、并发 probe、stale / timeout 注入 | V4 行 862-866、117 |
| H-08 | 池容量与尾延迟 / SLO 数值 | 无实机输入时任何数值都是占位 | 并发压测 + soak；给出吞吐、p99、CPU、错误率 | V4 行 779、1163 |

**上机前准备**：① 记录实际解析到的 UMDK commit / ABI / 内核·驱动·固件与设备清单，保证结论可复现；② 先用 interposer 把异常分支（marker 丢失、delete busy、迟到 CQE）跑通，再上实机，避免把软件 bug 误判成 provider 行为。

**当前证据边界**：截至 2026-11-10，仓库内只有 fake adapter / UT / 轻量协议 smoke（V4 行 81），上表 8 项**均无实机报告**；并发规模、fence 超时、delete 退避、quarantine 预算、p99 与 SLO 等数值全部无实测值，文档中的示例数字只作占位。

**不能当作实机证据的替代**：mock / fake adapter、进程内 `pollMutex`、ACK timeout、inflight 计数归零、`shared_ptr::use_count()`、按 slice 全长记 `completed_bytes`、历史 UMDK 源码观察、未记录 SHA 的浮动构建。
