# TENT UB 探测对端获取（Peer Bootstrap）详细设计

> 日期：2026-11-17。
> 关系：本文是《tent-ub-probe-link-detail-design》§12.2 / §16 #1 的落地子设计，也是《tent-ub-probe-standalone-supply》P6 的前置实现；**不改变** PLM 状态机、探测 WR 格式、poller 接缝与完成/超时语义。
> 范围：只做**路线 B（最小生产版）**——每节点 `ControlService` 发布/查询 endpoint 描述符，`peer_segments` 同名配对，`UbTransport` 自动接线，area 稳定化；多节点显式 link graph、随机 token/TLS、ProbeRegistry 留给后续（路线 C）。
> 状态：路线 B 已实现（B0–B4，2026-11-17）。每节点 `GetProbeBootstrap`、`peer_segments` 自动 source、area 稳定化、TTL/target/area/gate 刷新和旧 import 延迟释放均已落地；多节点显式 link graph、随机 token/TLS、中心 registry 仍属路线 C。实机/tebench 联调待验证。

## 0. 决策与范围

| 编号 | 决策 | 说明 |
| --- | --- | --- |
| B-1 | **每节点 `ControlService` 新增 `GetProbeBootstrap` RPC** | 不引入中心 coordinator；TENT 已有每节点 RPC 与 `SegmentManager` |
| B-2 | **首版用 `peer_segments` + 同名配对** | 2 节点/tebench 够用；显式 `links` 留 C |
| B-3 | **source 在 `UbTransport::install` 内自动构造** | tebench 不改代码；`provider=standalone` 且未注入 source 时生效 |
| B-4 | **area 稳定化** | send-lane 重建只换 probe Jetty/source，不换 area 页 |
| B-5 | **包含最小 TTL 刷新** | 不刷新则对端 Jetty 重建后 target_epoch 永远不更新；TTL 30s，B 内实现 |
| B-6 | **不做** | 随机 token、TLS、显式 link graph、跨控制面 push、ProbeRegistry |
| B-7 | **保留 selftest 静态 source** | 通过注入 source 优先；用于无控制面联调 |

非目标：

- 不做中心服务；
- 不把探测流量放进控制面；
- 不改 `ProbeLinkManager` 的健康/ campaign/门禁逻辑；
- 不改 `HandshakeProbePeerDirectory` 的 import 语义（只加 refresh）。

### 0.1 备选方案与取舍

| 方案 | 优点 | 缺点 | 结论 |
| --- | --- | --- | --- |
| 复用业务 `UbBootstrapDesc` 携带 probe 字段 | 不新增 RPC，改动小 | 只能拿到业务实际用过的 endpoint；无业务流量时无法主动探测；不能覆盖全部 link | 可作为临时 fallback，不作为 B |
| selftest 静态 JSON | 最快 | 非生产；tebench 需手工文件 | 保留为测试 fallback |
| 中心 ProbeRegistry/Coordinator | 成员发现集中 | 新增服务、单点、TENT 已有每节点控制面 | 不做 |
| **每节点 `GetProbeBootstrap` + `peer_segments`** | 复用现有控制面；无业务流量也能探；改动可控 | 需显式 peer 列表；B 不做多节点 link graph | **采用** |

## 1. 现状与差距

当前探测装配链：

```text
UbTransport::install
  └─ provider=standalone
       ├─ StandaloneProbeResourceProvider::discover()   // 只建本地资源
       ├─ probe_provider->snapshot(graph)               // 需要远端 descriptor
       │    └─ HandshakeProbePeerDirectory::snapshotEdges()
       │         └─ ProbePeerDescriptorSource::snapshot()  // ← 生产实现缺失
       ├─ ProbeLinkManager::initialize(graph)
       └─ workers.start() + scheduler.start()
```

现有可复用资产：

- `ProbePeerDescriptorSource` 接口（[probe_peer_handshake.h](../../mooncake-transfer-engine/tent/include/tent/transport/ub/probe_peer_handshake.h#L72-L78)）；
- `HandshakeProbePeerDirectory` 的 import/缓存（`snapshotEdges`）；
- provider 的 `portViews()` / `localAreaViews()`；
- `ControlService` 的 callback/注册模式（`BootstrapUb` 可照抄）；
- PLM 的远端事件处理（`kEdgeAdded/kEdgeRemoved/kAreaReplaced/kTargetEpochChanged/kGateChanged`）。

缺口：

1. 没有生产 `ProbePeerDescriptorSource`；
2. `UbTransport()` 默认 `probe_peer_source=nullptr`，`transport_loader` 不注入；
3. provider 每次 lane 重建都轮换 area，且没有刷新；
4. descriptor 没有节点身份/epoch，无法处理对端重启；
5. tebench 打开 `probe.enabled=true` 必然 install 失败。

## 2. 总体架构

```text
Node A                                              Node B
┌─────────────────────────────┐                     ┌─────────────────────────────┐
│ StandaloneProbeProvider     │                     │ StandaloneProbeProvider     │
│  ports_ → endpoint descs    │                     │  ports_ → endpoint descs    │
│  area(稳定)/jetty/source    │                     │  area(稳定)/jetty/source    │
└──────────────┬──────────────┘                     └──────────────┬──────────────┘
               │ exportBootstrap()                                  │
               ▼                                                    ▼
┌─────────────────────────────┐   GetProbeBootstrap   ┌─────────────────────────────┐
│ ControlService (A)          │ ◄────────────────────► │ ControlService (B)          │
│  callback → provider        │   JSON over TCP RPC   │  callback → provider        │
└──────────────┬──────────────┘                       └──────────────┬──────────────┘
               │                                                     │
               ▼                                                     ▼
┌─────────────────────────────┐                       ┌─────────────────────────────┐
│ ControlPlaneProbePeerSource │                       │ ControlPlaneProbePeerSource │
│  peer_segments + cache      │                       │  peer_segments + cache      │
└──────────────┬──────────────┘                       └──────────────┬──────────────┘
               ▼                                                     ▼
┌─────────────────────────────┐                       ┌─────────────────────────────┐
│ HandshakeProbePeerDirectory │                       │ HandshakeProbePeerDirectory │
│  import peer area/jetty     │                       │  import peer area/jetty     │
└──────────────┬──────────────┘                       └──────────────┬──────────────┘
               ▼                                                     ▼
        PLM + scheduler  ──────── URMA 1B WRITE (per edge) ────────► peer area
```

关键点：

- 控制面只交换描述符，不承载探测流量；
- 每个节点独立判断自己的出边；
- area 由对端注册、对端保持稳定；本端只 import；
- 对端 Jetty 重建只改 `target_epoch`，本端通过 TTL 刷新更新 target。

### 2.1 启动时序

```text
A: UbTransport::install
  ├─ provider.discover() / prepareLocalResources()
  ├─ provider.setNodeIdentity(segment_name, node_epoch)
  ├─ ControlService.setProbeBootstrapCallback(provider.exportBootstrap)
  ├─ ControlPlaneProbePeerSource.snapshot()
  │    ├─ provider.endpointDescriptors()                    # 本地端点
  │    └─ for peer in peer_segments:
  │         SegmentManager.openRemote + getRemote            # peer rpc_server_addr
  │         ControlClient.getProbeBootstrap(peer_addr)       # 对端端点
  │         pair(local, peer) → ProbePeerDescriptor[]        # 同名/eid 配对
  ├─ HandshakeProbePeerDirectory.snapshotEdges()             # import peer area
  ├─ ProbeLinkManager.initialize(graph)
  ├─ workers.start()                                         # poller
  └─ scheduler.start()
```

## 3. 数据模型

### 3.1 wire 结构

新增 `include/tent/runtime/probe_bootstrap.h`，只依赖 `segment.h`/基础类型，**不依赖** `urma_adapter.h`：

```cpp
struct ProbeEndpointKey {
    std::string node_id;             // 该 endpoint 所属节点（segment name）
    std::string native_device_name;  // udmac0d1e2
    uint32_t eid_index = 0;
};

struct ProbeRemoteJetty {
    std::string eid;
    uint32_t id = 0;
    uint32_t uasid = 0;
    uint32_t token = 0;
};

struct ProbeAreaDescriptor {         // 与 SegmentDescriptor 同构，避免 UB 头依赖
    uint32_t schema_version = 0;
    uint32_t urma_api_version = 0;
    uint32_t urma_abi_size = 0;
    std::string hex;
};

struct ProbeEndpointDesc {
    ProbeEndpointKey key;
    std::string topology_name;       // ub:udmac0d1e2:eid0
    std::string eid;
    int local_nic = -1;              // 业务 Topology NIC id（关键：与 UbPostPath 对齐）
    ProbeAreaDescriptor area;
    uint64_t area_length = 0;
    uint32_t area_token = 0;
    uint64_t area_epoch = 0;
    ProbeRemoteJetty probe_jetty;
    uint64_t target_epoch = 0;       // probe Jetty 代次
    uint64_t endpoint_generation = 0;
    bool node_gate_ok = true;
    bool port_gate_ok = true;
    bool profile_gate_ok = true;
};

struct ProbeBootstrapRequest {
    uint32_t schema_version = 1;
    std::string requester_node;
    uint64_t known_node_epoch = 0;
};

struct ProbeBootstrapResponse {
    uint32_t schema_version = 1;
    std::string node_id;
    uint64_t node_epoch = 0;
    std::string rpc_server_addr;
    uint64_t topology_epoch = 0;
    bool unchanged = false;          // 兼容旧服务端；当前服务端总返回完整 endpoints
    std::vector<ProbeEndpointDesc> endpoints;
    std::string reply_msg;           // 非空表示服务端拒绝/暂不可用
};
```

JSON 用 nlohmann 显式 `to_json/from_json`；`area.hex` 与 `SchemaVersion` 校验，禁止直接反序列化原生 `urma_seg_t`。

**校验规则（拒绝整条响应）**：

- `schema_version == 1`、`reply_msg` 为空；
- `node_id` 非空、`node_epoch != 0`；
- `endpoints.size() <= 256`；每个 endpoint 的 `key.node_id == response.node_id`；
- `local_nic >= 0`、`native_device_name/eid/topology_name` 非空；
- `area.schema_version` 匹配、`hex` 非空、`area_length` 在 `[1, 64KiB]`；
- `area_epoch != 0`、`target_epoch != 0`、`probe_jetty.id != 0`；
- 同一 `(native_device_name, eid_index)` 不得重复。

### 3.2 与现有 descriptor 的映射

`ControlPlaneProbePeerSource` 把配对结果转成现有 `ProbePeerDescriptor`：

| `ProbePeerDescriptor` | 来源 |
| --- | --- |
| `local_nic` | 本地 endpoint 的 `local_nic` |
| `peer_segment_id` | `SegmentManager::openRemote(peer_segment_name)` 的 handle |
| `remote_nic` | 对端 endpoint 的 `local_nic`（对端业务拓扑 id） |
| `peer_nic_path` | `peer_segment_name` 或 `rpc_server_addr`（诊断） |
| `topology_epoch` | `response.topology_epoch` |
| `target_epoch` | 对端 endpoint 的 `target_epoch` |
| `area_descriptor/area_length/area_epoch/area_token` | 对端 endpoint 的 area |
| `remote_jetty` | 对端 endpoint 的 `probe_jetty` 转 `RemoteJettyInfo` |
| `gate_*` | 对端 endpoint 的 gate 位 |
| `endpoint_generation` | 对端 endpoint 的 `endpoint_generation` |

**`remote_nic` 必须是对端业务 Topology NIC id**，否则 `ProbeLinkManager::resolveVersionLocked()` 无法把业务 `UbPostPath.remote_device_id` 映射到 probe path。当前 provider 已通过 `TopologyIdResolver` 解析本地业务 id，B 沿用。

### 3.3 epoch 语义

| epoch | 递增方 | 本端使用 |
| --- | --- | --- |
| `node_epoch` | provider/transport 每次启动 | 只做**相等/不等**：变化即丢弃该 peer 全部缓存；不做跨节点大小比较 |
| `topology_epoch` | 链路/拓扑配置版本（B 固定 1） | 进入 `PathVersion.topology_epoch` |
| `target_epoch` | probe Jetty 每次重建 | 对端刷新 target；older 忽略 |
| `area_epoch` | 仅 area 轮换时 | 对端刷新 area；send-lane 重建不变 |

`node_epoch` 建议取 `steadyNowNs() ^ random`，保证进程重启后不同即可；不依赖跨机时钟顺序。

## 4. 控制面 RPC

### 4.1 opcode 与序列化

- [`include/tent/rpc/rpc.h`](../../mooncake-transfer-engine/tent/include/tent/rpc/rpc.h#L36-L53)：在 `RpcFuncID` **末尾**追加 `GetProbeBootstrap`，保持已有数值不变；
- 新增 `include/tent/runtime/probe_bootstrap.h`，由 `control_plane.h` 与 UB 代码共同包含；
- JSON body 与现有 `BootstrapUb` 一致：request/response 各一个 JSON 字符串。

### 4.2 `ControlClient`

```cpp
static Status getProbeBootstrap(const std::string& server_addr,
                                const ProbeBootstrapRequest& request,
                                ProbeBootstrapResponse& response);
```

实现照抄 `bootstrapUb`：

```cpp
json j = request;
std::string raw = j.dump(), resp_raw;
CHECK_STATUS(tl_rpc_agent.call(server_addr, GetProbeBootstrap, raw, resp_raw));
response = json::parse(resp_raw).get<ProbeBootstrapResponse>();
if (response.schema_version != 1) return Status::MalformedJson(...);
if (!response.reply_msg.empty())
    return Status::RpcServiceError(response.reply_msg);
return Status::OK();
```

超时：现有 `CoroRpcAgent::call` 没有显式 timeout 参数。B 用 `callAsync` + 条件变量等 `probe/peer_rpc_timeout_ms`：超时后放弃本次等待（late response 只计数/丢弃，不再使用），保证 install/refresh 不被单个 peer 无限阻塞；若不引入异步，首版至少记录 RPC 耗时并在 `peer_wait_ms` 总预算内重试。

### 4.3 `ControlService`

新增：

```cpp
using OnReceiveProbeBootstrap =
    std::function<int(const ProbeBootstrapRequest&, ProbeBootstrapResponse&)>;

void setProbeBootstrapCallback(const OnReceiveProbeBootstrap& callback);
```

私有：

```cpp
void onGetProbeBootstrap(const std::string_view& request, std::string& response);
```

实现模式与 `onBootstrapUb` 完全相同：

1. parse JSON；
2. 在 `probe_bootstrap_callback_mutex_` 下取 callback；
3. 无 callback → `reply_msg = "probe bootstrap is not available"`；
4. 有 callback → 调它，序列化 response；
5. 任何异常 → 返回带 `reply_msg` 的错误，不抛到 RPC 线程。

注册：

```cpp
rpc_server_->registerFunction(
    GetProbeBootstrap,
    [this](const std::string_view& req, std::string& resp) {
        onGetProbeBootstrap(req, resp);
    });
```

### 4.4 服务端数据来源

`UbTransport::install` 在 `standalone->discover()` 之后、`snapshot()` 之前：

```cpp
metadata->setProbeBootstrapCallback(
    [provider = standalone.get()](const ProbeBootstrapRequest& req,
                                  ProbeBootstrapResponse& resp) {
        return provider->exportBootstrap(req, resp);
    });
```

`StandaloneProbeResourceProvider::exportBootstrap()`：

- 锁 `mutex_`；
- 填 `node_id`（transport 的 `local_segment_name`）、`node_epoch`、`topology_epoch`；
- 不按 `node_epoch` 返回 `unchanged`：同一 node_epoch 内 target_epoch/area_epoch/端口集合都可能变化；服务端总返回完整端点列表，`unchanged` 仅保留兼容旧客户端。
- 遍历 `ports_` 中 `available` 的端口，产出 `ProbeEndpointDesc`；
- 任一端点缺 area/jetty/EID → 跳过并计数，不因一个坏口让整个响应失败。

`rpc_server_addr` 由 `UbTransport` 的 callback 包装层填充（`metadata->segmentManager().getLocal()->rpc_server_addr`），provider 只负责端点；避免 provider 依赖控制面身份。

`UbTransport::shutdownUnlocked()` 在清 `BootstrapUb` callback 的同时清 `probe bootstrap` callback，顺序在 workers stop 之前。

### 4.5 启动时序与重试

`TransferEngineImpl::construct` 中 `metadata_->start()` 在 `loadTransports()` 之前，因此两节点可以互查。但对端 transport 可能尚未 install，`GetProbeBootstrap` 返回“not available”。

`ControlPlaneProbePeerSource::snapshot()`：

- 对每个 `peer_segment` 在 `peer_wait_ms`（默认 5000）内重试；
- 至少拿到一条 edge 才返回成功；
- 一条都没有 → 返回 `DeviceNotFound`，`UbTransport::install` fail-fast，错误信息包含 peer 名与最后一次 RPC 错误；
- 后续由 TTL 刷新补齐。

## 5. Standalone provider 改动

### 5.1 端点导出

`PortState` 补：

```cpp
uint32_t eid_index{0};
std::string eid;
std::string native_device_path;
uint32_t area_token{kProbeAreaToken};      // B 固定；C 随机化
uint32_t jetty_token{kProbeAreaToken};     // B 固定；C 随机化
uint64_t endpoint_generation{0};           // 可选，业务 anchor
```

`discoverLocked()` 填充；`exportBootstrap()` 从 `port.area_segment`/`port.probe_jetty` 生成 `ProbeEndpointDesc`：

```cpp
desc.key = {node_id, native_device_name, eid_index};
desc.topology_name = port.topology_name;
desc.eid = port.eid;
desc.local_nic = port.config.local_nic;
desc.area = to_wire(port.area_segment->descriptor());
desc.area_length = port.area_size;
desc.area_token = port.area_token;
desc.area_epoch = port.area_epoch;
desc.probe_jetty = {port.eid, port.probe_jetty->id(),
                    port.probe_jetty->uasid(), port.jetty_token};
desc.target_epoch = port.config.generation;
desc.endpoint_generation = port.endpoint_generation; // 可为 0
```

B 端点列表包含 `discoverLocked()` 选出的全部 active EID（与当前 no-filter/native-filter 语义一致）；多节点显式 link graph 留 C。若 `TopologyIdResolver` 返回 -1（设备不在业务 topology），**跳过该端点并计数，不回退到 discovery index**，避免重新引入业务证据错配；全部端点被跳过时 `exportBootstrap` 返回 `reply_msg`。

B 阶段 `port.jetty_token`/`area_token` 可继续用 `0xACFE`；C 阶段随机化。

### 5.2 area 稳定化（必须）

修改 `rebuildPortLocked`：

- **不再**分配 `new_area`；
- `port.area_segment` / `area_memory` / `area_size` / `area_epoch` 保持不变；
- 只新建 `new_jetty` + `new_source`；
- `port.config.generation++`、`port.source_generation = generation`；
- `target_epoch = generation`；
- `emitHandleReplacedLocked` 携带同一 area descriptor，`area_epoch` 不变。

只有以下情况轮换 area：

1. probe Context 必须重建；
2. 进程重启；
3. 显式配置变更。

轮换时 `area_epoch++`、发布新 descriptor、旧 area 保留到安全窗口结束（C 阶段完善回收）。

### 5.3 节点身份

provider 新增：

```cpp
void setNodeIdentity(std::string node_id, uint64_t node_epoch);
std::string nodeId() const;
uint64_t nodeEpoch() const;
```

`UbTransport::install` 在 `discover()` 后调用；`node_epoch` 在 transport install 时生成一次。

## 6. ControlPlaneProbePeerSource

新增 `include/tent/transport/ub/probe_control_plane.h` + `src/transport/ub/probe_control_plane.cpp`。

### 6.1 依赖注入（便于单测）

```cpp
class ProbeBootstrapClient {          // 生产实现包 ControlClient；测试可 fake
  public:
    virtual Status get(const std::string& server_addr,
                       const ProbeBootstrapRequest& request,
                       ProbeBootstrapResponse& response) = 0;
};

class PeerAddressResolver {           // 生产实现包 SegmentManager
  public:
    virtual Status resolve(const std::string& peer_segment,
                           std::string& rpc_server_addr,
                           SegmentID& segment_id) = 0;
};

class LocalEndpointSource {           // 生产实现包 StandaloneProbeResourceProvider
  public:
    virtual Status endpoints(std::vector<ProbeEndpointDesc>& out) = 0;
};
```

`ControlPlaneProbePeerSource` 构造参数：三个接口 + `ProbeLinkGraph`（B 只有 `peer_segments`）+ `ProbeParams`。

```cpp
struct ProbeLinkGraph {
    // B：只有 peer segment 列表；C 扩展为显式 local/remote endpoint 对。
    std::vector<std::string> peer_segments;
};
```

### 6.2 配对算法（B）

每个 `peer_segment`：

1. `PeerAddressResolver::resolve()` 拿 `rpc_server_addr` + `SegmentID`（`openRemote` 结果按 peer segment 缓存，不重复 open）；
2. `ProbeBootstrapClient::get()` 拉 `ProbeBootstrapResponse`；
3. 建本地/对端 endpoint 索引：
   - 本地：`(native_device_name, eid_index)` → endpoint；
   - 对端：`(native_device_name, eid_index)` → endpoint；
4. 对每个本地 endpoint：
   - 先精确匹配 `(native, eid_index)`；
   - 若没有，且该 native 在两侧都只有一个 endpoint，则按 native 配对（兼容首 EID 不同）；
   - 都没有 → 跳过并计数 `probe_peer_no_endpoint`；
5. `peer_segments` 中等于本地 `node_id` 的项直接跳过并告警；
6. 生成 `ProbePeerDescriptor`（字段见 §3.2）；
7. `peer_segment_id` 用 resolver 返回的 handle。

B 不读 `links` 配置；`peer_segments` 为空时直接返回 `DeviceNotFound`（fail-fast）。

### 6.3 缓存与刷新

```cpp
struct PeerCache {
    std::string node_id;
    uint64_t node_epoch{0};
    uint64_t topology_epoch{0};
    std::vector<ProbeEndpointDesc> endpoints;
    uint64_t fetched_at_ns{0};
    uint64_t backoff_ns{0};
    bool in_flight{false};
};
```

- 每个 peer 一个 cache 条目；`std::mutex` 保护；
- TTL = `probe/descriptor_ttl_ms`（默认 30000）；
- 同一 peer 单飞：`in_flight` 时其他调用等待/复用结果；
- 失败：`backoff_ns = min(backoff*2, 10s)`，不每秒重试；
- `node_epoch` 变化：立即丢弃该 peer 全部 endpoint，重新配对；
- 旧服务端返回 `unchanged=true` 时复用旧 endpoint，仅更新 `fetched_at`；新服务端不会走该分支。

### 6.4 刷新触发

B 内最小刷新由 provider maintenance 线程驱动：

- 每 `probe/descriptor_refresh_ms`（默认 30000）调 `directory->refresh(events)`；
- `refresh` 内部调 source `snapshot()`（TTL 到期才真正发 RPC）；
- 生成 `ProbeEvent` 通过 `availability_sink_->onPoolAvailabilityChange()` 应用；
- directory 先构建 pending cache，provider 投递后按 `delivered_events` 提交：
  - 全部成功 → 提交完整 cache；
  - 部分成功 → 只提交已投递事件对应的 prefix，未投递的新 import 回滚；
  - 失败只记日志/计数，下个周期按已提交 prefix 重新 diff。

触发条件（任一）：

1. TTL 到期；
2. 对端 node_epoch 变化；
3. `kAreaReplaced` import 失败 / target 错误（可选立即 refresh，B 允许下个周期）。

### 6.5 失败处理

| 情况 | 行为 |
| --- | --- |
| peer RPC 不可达/超时 | 保留旧 cache（若有），退避；首次 snapshot 计入 `peer_wait_ms` |
| 对端不支持（unknown opcode） | 跳过该 peer，计数 `probe_peer_unsupported`；零 edge 时 install 失败 |
| 响应 schema/字段非法 | 整条响应拒绝，计数 `probe_peer_malformed` |
| 本地 endpoint 缺失 | 跳过该 edge，计数 `probe_peer_local_missing` |
| import area 失败 | 保留旧 edge；计数；下个 TTL 重试 |
| 对端重启 | `node_epoch` 变 → 清 cache，重新拉取/配对 |
| 对端 target_epoch 变 | 刷新 descriptor，发 `kTargetEpochChanged` |

## 7. HandshakeProbePeerDirectory 改动

B 只加两处：

1. **refresh**：

```cpp
virtual Status refresh(std::vector<ProbeEvent>& events) {
    return Status::OK();   // 默认无操作
}
```

`HandshakeProbePeerDirectory::refresh`：

- 调 source `snapshot()` 取新 descriptor；
- 与 `cache_` 做 diff：
  - 新 edge → `kEdgeAdded`（import area，生成 `ProbeEdgeSupply`）
  - 删 edge → `kEdgeRemoved`
  - same edge + area/token/epoch 变 → `kAreaReplaced`
  - same edge + target_epoch 变 → `kTargetEpochChanged`
  - gate 变 → `kGateChanged`
- 事件由 provider 填 `graph_epoch/event_seq` 后**按序逐条**投递；某条失败即停止，保留未投递队列/下个周期重试，**不得跳号**；
- 新旧 edge 的 `PathVersion` 相同时，优先用 `kAreaReplaced`/`kTargetEpochChanged` 原地更新，避免先删后加导致 campaign 抖动。

2. **旧 import 延迟释放**：

- 不再在 `refresh` 内立即 `unimportRemoteSegment`；
- 把旧 `RemoteSegmentPtr` 放入 `retired_`，仅当 `use_count()==1` 或超过安全窗口（2×timeout）才 unimport；
- 避免 PLM/record 仍持有旧 area 时被释放。

## 8. 装配与 tebench

### 8.1 `UbTransport::install`

在现有 probe 装配块（`ub_transport.cpp:344` 起）内：

```cpp
if (params.probe.enabled && params.probe.provider == kStandalone &&
    probe_peer_source == nullptr) {
    auto source = std::make_unique<ControlPlaneProbePeerSource>(
        std::make_unique<SegmentManagerAddressResolver>(&metadata->segmentManager()),
        std::make_unique<ControlClientProbeBootstrapClient>(),
        std::make_unique<ProviderLocalEndpointSource>(standalone.get()),
        makePeerSegmentGraph(params.probe.peer_segments),
        params.probe);
    probe_peer_source_owned = std::move(source);
    probe_peer_source = probe_peer_source_owned.get();
}
```

- 若调用方已注入 source，优先使用；
- `probe_peer_source_owned` 由 `UbTransport::Impl` 持有，生命周期覆盖 directory/provider；
- 在 `shutdownUnlocked()` 中清 probe callback 后，再销毁 source/directory/provider。

### 8.2 `transport_loader`

不需要改：`UbTransport()` 默认构造即可；source 在 `install` 内按配置自动构造。

### 8.3 tebench 配置

```jsonc
{
  "transports": {
    "ub": {
      "enable": true,
      "probe": {
        "enabled": true,
        "provider": "standalone",
        "peer_segments": ["<target_seg_name>"],
        "peer_wait_ms": 5000,
        "descriptor_ttl_ms": 30000,
        "peer_rpc_timeout_ms": 1000,
        "device_filter": ["udma0", "udma1"]
      }
    }
  }
}
```

```bash
# target
MC_TENT_CONF=/path/probe.json ./tebench --backend=tent --xport_type=ub ...
# initiator（target_seg_name 由 target 打印）
MC_TENT_CONF=/path/probe.json ./tebench --backend=tent --xport_type=ub \
  --target_seg_name=<SEG> ...
```

B 阶段 `peer_segments` 需手工填 target segment name；后续可给 tebench 加 `--probe_peer_segments` 自动注入。

## 9. 配置键

| 键 | 默认 | 校验 |
| --- | --- | --- |
| `probe/peer_segments` | 空 | 字符串数组；空且 provider=standalone → install 失败 |
| `probe/peer_wait_ms` | 5000 | > 0；首次 snapshot 等待 |
| `probe/descriptor_ttl_ms` | 30000 | > 0；≥ `peer_rpc_timeout_ms` |
| `probe/descriptor_refresh_ms` | 30000 | >= 0；0 表示关闭周期刷新；建议 `<= descriptor_ttl_ms` |
| `probe/peer_rpc_timeout_ms` | 1000 | > 0 |

`probe/links`、`probe/peer_source` 留 C；每 EID 端点已由当前 `discoverLocked()` 提供。

## 10. 失败矩阵与语义

| 场景 | 期望 |
| --- | --- |
| 两节点同时启动 | `peer_wait_ms` 内互相拿到 descriptor；至少一条 edge 才 install |
| 对端未开探测 | `GetProbeBootstrap` 返回 not available；`probe_peer_unsupported`；零 edge 时 install 失败并提示 |
| 对端旧版本 | unknown RPC；skip/计数；不崩溃 |
| 对端重启 | 刷新看到 `node_epoch` 变；旧 edge removed，新 edge added；PLM 旧记录 stale |
| 对端 probe Jetty 重建 | 本端 TTL 刷新看到 `target_epoch` 变；`kTargetEpochChanged`；旧 in-flight stale |
| 本端 rebuild | area 不变；`kProbeHandleReplaced` 只换 jetty/source；对端无感 |
| area import 失败 | 保留旧 edge；计数；下个 TTL 重试 |
| 控制面 RPC 失败 | 保留旧 cache；退避；不阻塞 PLM/poller |
| 安装期零 edge | fail-fast，错误包含 peer/最后一次 RPC 状态 |

## 11. 并发与生命周期

- `exportBootstrap` 在 provider `mutex_` 内拷贝，RPC 线程不持有 PLM 锁；
- source 的 RPC 在 install 线程或 maintenance 线程执行，**不在 poller 线程**；
- `ProbeBootstrapClient` 使用 `tl_rpc_agent`，每线程独立，不共享连接；
- `ProbePeerDescriptorSource` 由 `UbTransport::Impl` 持有，directory 只存裸指针；
- teardown 顺序：
  1. 清 `ControlService` probe callback；
  2. 停 scheduler/workers（现有）；
  3. 停 provider maintenance（现有 `shutdown()` join）；
  4. 销毁 directory（`shutdown` unimport）；
  5. 销毁 source/provider。
- source 的 cache 不持有 native handle，只持有 plain descriptor，销毁无副作用。

## 12. 测试

| 测试 | 覆盖 |
| --- | --- |
| `ub_probe_bootstrap_codec_test` | JSON round-trip、schema/epoch/hex 校验、非法字段拒绝 |
| `ub_probe_peer_source_test` | fake client/resolver/local source：精确配对、native fallback、缺 endpoint、RPC 失败、node_epoch 变 |
| `ub_probe_control_plane_test` | `ControlService` handler callback、完整端点响应、无 callback 错误 |
| `ub_probe_area_stable_test` | rebuild 后 area_descriptor/area_epoch 不变，target_epoch 递增 |
| `ub_probe_directory_refresh_test` | add/remove/area/target/gate 事件 diff；旧 import 延迟释放 |
| `tent_probe_selftest` | 保留静态 source；新增 no-JSON 模式可选 |
| tebench 2 节点 | `peer_segments` + `probe.enabled`，`probe_test_*` 计数增长，无 poller 阻塞 |

## 13. 落地分片

| 阶段 | 内容 | 出口 | 量级 |
| --- | --- | --- | --- |
| B0 | wire 结构 + provider endpoint 导出 + area 稳定化 | codec/area 单测 | ~200 行 + 测试 |
| B1 | `GetProbeBootstrap` RPC（client/service/callback/校验） | fake RPC 互通 | ~250 行 + 测试 |
| B2 | `ControlPlaneProbePeerSource` + `peer_segments` 配对 + `UbTransport` 自动接线 | 2 节点无 JSON，tebench 可开探测 | ~350 行 + 测试 |
| B3 | tebench/selftest 联调、错误路径、日志/计数 | 2 节点实机通过 | ~100 行 |
| B4 | 最小 TTL 刷新 + node/target epoch 失效 + 旧 import 延迟释放 | 对端重启/Jetty 重建可恢复 | ~200 行 + 测试 |

约 1.5–2 周（单人）；B0–B2 可先合并出可用版本，B4 紧接着做。

### 13.1 灰度与回滚

- `probe.enabled` 默认 false；B 只在 `enabled=true && provider=standalone` 时生效，不影响业务 UB 路径；
- 首次部署用 `probe/test/apply_health=false` 只看链路，再切 `true`；
- 注入式 source（selftest/测试）优先于控制面 source，二者不混用；
- 回滚：`probe.enabled=false` 即可；已建 probe 资源随 transport uninstall 释放，业务无感；
- 若 `peer_segments` 配错/对端不支持，install fail-fast，不静默降级为“不探测”。

## 14. 风险与未决

| 风险 | 缓解 |
| --- | --- |
| `peer_segments` 需要手工填 target segment name | B 接受；后续 tebench 参数/自动发现 |
| 对端未安装 transport 时查询失败 | `peer_wait_ms` 重试；零 edge fail-fast |
| 旧 area import 释放时序 | 延迟释放 + `use_count`/安全窗口；C 完善 |
| `node_epoch` 不持久化 | 只做不等判断；重启即变；不依赖顺序 |
| 控制面 RPC 无 TLS | 与业务 bootstrap 同通道，内网；C 加 TLS/认证 |
| 多节点显式 link graph | B 不做；C 加 `links`；每 EID 端点当前已支持 |

## 15. 验收标准

1. 2 节点 tebench：不写 JSON，配置 `peer_segments` 后探测自动启动，`probe_test_*` 计数增长；
2. `apply_health=false` 纯链路检查；`true` 时 ISOLATED/RECOVERING/HEALTHY 正常；
3. 本端 probe Jetty 重建：area 不变，对端无 re-import；target_epoch 刷新后恢复；
4. 对端重启：`node_epoch` 变化，旧 edge 移除、新 edge 建立；
5. 对端旧版本：install 失败信息明确，或至少不崩溃、可诊断；
6. 无 poller 阻塞、无 retired page 持续增长；
7. selftest 静态 source 仍可运行，作为 fallback。
