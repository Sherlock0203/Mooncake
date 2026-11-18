# Jetty 池化方案测试验证报告

- 范围：TENT UB 传输层 rm_pool 池化核心组件（软件级验证）
- 测试目标：`tent_ub_pooled_core_test`（GoogleTest）
- 构建环境：Docker 镜像 `mooncake-devenv:local`，CMake Release，
  `-DBUILD_UNIT_TESTS=ON -DUSE_TENT=ON -DUSE_UB=ON`，URMA 以桩库
  （`liburma.so` 空符号库）链接；全部测试基于 fake URMA adapter，
  不依赖真实 provider/NIC/内核资源。
- 结果：**47/47 通过，0 失败，0 跳过**（ctest 退出码 0）。

## 1. 被测组件与用例矩阵

| 测试套件 | 用例数 | 覆盖组件 | 对应验证点 |
| --- | --- | --- | --- |
| `UbNativeBudgetTest` | 8 | `NativeBudget` 设备级全角色预算账本（C05） | 配置校验、预留/迁移/释放、全角色上限、替换储备、释放需销毁证明、非法迁移拒绝、溢出拒绝、泄漏预留计数 |
| `UbAdmissionTest` | 3 | `JfcCreditLedger` / `TaskPermitLedger`（C05/C06） | JFC WR credit 往返、超额预留拒绝、Task K 票据上限 |
| `UbSendJettyPoolTest` | 14 | `SendJettyPool` 每 attempt 独占 LIFO 池（C06） | 池复用、LIFO 顺序、JFC credit 匹配、预算耗尽延迟、创建失败降级、stop 交错、故障永久退役、双重归还拒绝、泄漏隔离、删除失败隔离、drain 语义、非法参数、PoolKey 隔离 |
| `UbAttemptGateTest` | 13 | `AttemptGate` 完成门（C07/D04） | 单次结算、post/完成乱序、重复 CQE 矛盾、长度矛盾、失败退役、零接受回滚、未知接受保持责任、handler 引用阻塞结算、池关闭退役、故障强制退役 |
| `UbPooledDataPathTest` | 2 | 端到端 lease→post→gate→return | 正常路径复用同一成员；降级路径退役并按储备重建 |
| `UbSendJettyPoolConcurrencyTest` | 2 | 高并发压测 | 8 线程×500 轮租还 hammer 不变式；stop 与并发租还交错的安全排空 |
| `UbParamsPooledTest` | 4 | `UbParams` 池化配置键（C01） | 默认值 legacy 安全、显式池化键解析、非法值拒绝、rm_pool 保持 fail-closed |
| `UbContextPooledAttachTest` | 1 | `UbContext` 集成缝 | 挂载点对 RC/RM-legacy 路径完全休眠 |

## 2. 需求覆盖对照

### 2.1 高并发场景下的池化资源复用
- `HammerLeaseReturnKeepsInvariants`：8 线程 × 500 轮并发
  `tryLease/returnOnce`，期望池深 4、预算 4、双 JFC（容量 1024）。
  逐轮校验**独占性不变式**（同一 lane 不允许被两个 attempt 同时租出）、
  预算上限不突破、租还计数守恒；延迟获取只允许返回
  `TooManyRequests`（绝不阻塞、绝不旁路）。
- `HammerWithStopDrainsSafely`：6 线程 × 300 轮租还进行中由独立线程发起
  `beginStop()`，验证 stop 后新租约被拒、在途租约全部以退役方式结算、
  无成员丢失、无预算悬挂。
- `ReturnThenLeaseReusesLifoOrder` / `NormalAttemptReusesMember`：
  复用顺序（LIFO）与复用后不再触发原生 `createJetty`（创建计数守恒）。

### 2.2 资源泄漏防护
- `DroppedLeaseQuarantinesMemberWithBudget`：lease 被丢弃（未走
  `returnOnce`）时成员被隔离、预算保持计费、`leaked_leases` 计数，
  绝不回流。
- `DoubleReturnRejected`：同一 lease 二次归还被拒绝并计数（INV04）。
- `LeakedReservationCounted`：预算预留泄漏被显式计数。
- `DrainIdleDeletesOnlyIdleMembers`：空闲回收只删除 IDLE 成员，
  IN_USE 成员不受影响。
- 预算释放只接受“已证明销毁”路径（`ReleaseRequiresProofAndTerminalState`），
  未证明销毁前预算持续计费（INV09/INV10）。

### 2.3 异常场景降级处理
- `CreateFailureDefersAndReleasesBudget`：`createJetty` 注入失败 →
  本轮延迟（`TooManyRequests`）、预算即时回退、下一轮无需重启即恢复。
- `BudgetExhaustionDefersWithoutCreation`：预算耗尽时池绝不扩张，
  只延迟并计数。
- `DeleteFailureQuarantinesRetiredMember`：`deleteJetty` 注入失败 →
  成员连同原生句柄进入隔离区、预算保持计费；后续 drain 重试删除成功
  后才释放预算。
- `FaultRetiresPermanently` / `GateFailedReturnRetiresMember`：
  首个真实错误即永久退役（INV07），恢复后也不回流。
- `StopRejectsNewLeasesAndRetiresOutstanding`：停机交错下在途租约
  安全结算。
- `AttemptGate` 13 个用例覆盖 post/完成乱序、重复 CQE、长度不符、
  接受状态未知等全部矛盾场景：矛盾一律退役隔离，绝不带病回流。
- `PooledProfileRemainsFailClosed`：结构完整的 rm_pool 配置在提升门
  返回 `NotImplemented`（INV16）——池化组件就绪但未验证前，生产流量
  仍走既有 RC/RM-legacy 路径，原业务逻辑零影响。

## 3. 自测过程中发现并修复的问题

| # | 现象 | 根因 | 处置 |
| --- | --- | --- | --- |
| 1 | `JfcCreditMatchingSkipsExhaustedMember` 失败 | 测试未按 `LeaseWithCredit` 契约在模拟完成点释放 WR credit token，容量为 1 时后续预留必然失败 | 修正测试：在归还/完成点释放 token，并补充 `matched.jfc_index` 断言 |
| 2 | `DeleteFailureQuarantinesRetiredMember` 失败 | **产品代码缺陷**：`destroyRetired`/`drainQuarantined` 在 `deleteJetty` 失败后将原生句柄移入局部变量并丢弃，导致池丢失原生所有权，下一次 drain 误判“无句柄=已销毁”而提前释放预算 | 修复 `send_jetty_pool.cpp`：删除失败时把句柄归还成员，保证下一次 drain 可重试（与头文件契约一致） |
| 3 | `PooledProfileRemainsFailClosed` 失败 | 测试配置 `physical_only=true` 但未提供 `device_identities`，被 `UbParams::FromConfig` 正确拒绝 | 修正测试：补充完整合法的 physical 设备身份，使配置结构完整后再验证 fail-closed |
| 4 | 回归构建：`tent_ub_teardown_test` 链接失败（`SendJettyPool::beginStop` 等未定义） | 该既有目标显式枚举源文件，包含被修改的 `context.cpp` 但未列入新增池化翻译单元 | 修复 `tent/tests/CMakeLists.txt`：为 teardown 目标补充 `native_budget.cpp`/`admission.cpp`/`send_jetty_pool.cpp`；生产库 `tent_xport_ub` 以 glob 收集源文件，自动包含新组件 |

## 4. 性能与稳定性验证说明

- 并发压测（第 2.1 节）在 Release 构建下运行，全部不变式成立，
  无死锁、无饥饿告警；池的锁策略为“锁内只做簿记、原生调用一律在
  锁外”，hammer 期间未出现互斥放大。
- 池化运行时指标可通过 `SendJettyPool::stats()` 观测：
  `total_leases/total_returns/pool_misses/stop_rejections/leaked_leases/
  created_members/create_failures/double_returns/quarantined_members`，
  可作为上线后高 QPS 场景的延迟与利用率监控锚点。
- 限制说明：本轮为**软件级验证**（fake adapter，无真实 NIC）。
  真实 URMA 硬件上的 QPS/延迟基准属于 P4+ 阶段，需在具备可信
  physical 设备与 provider 的环境中执行；在此之前 rm_pool 提升门
  保持 fail-closed，不影响现有业务路径。

## 5. 回归验证

- 编译回归：生产库 `tent_xport_ub`（glob 自动纳入 4 个新翻译单元）与
  全部 4 个 UB 测试目标（`tent_ub_core_test`、`tent_ub_pooled_core_test`、
  `tent_ub_teardown_test`、`tent_ub_native_data_path_test`）在
  Release 构建下编译链接通过，无新增告警。
- 测试回归（A/B 判定）：
  - `tent_ub_pooled_core_test` 47/47 通过；`tent_ub_teardown_test` 通过。
  - `tent_ub_core_test` 存在 1 个失败
    （`UbBootstrapTest.ValidatesRmIdentityProfilesAndProbeReferences`），
    `tent_ub_native_data_path_test` 存在 13 个失败（均为
    `UbRmLegacyTest.*`）。**经 A/B 复测确认**：将本次改动的 4 个文件
    回退到基线提交（`00546197`）后重新构建运行，上述 14 个失败原样
    复现，故均为基线既有问题，与本次池化改动无因果关系。
- 池化挂载点（`UbContext::attachPooledSendPool`）默认休眠：未安装池时
  `pooledSendPool()` 返回空，legacy 发送路径完全不感知。
- 环境说明：构建采用仓库设计的 stub 模式（不设置 `URMA_LIBRARY`，
  `TENT_HAS_REAL_URMA` 关闭）。若强制注入伪 `URMA_LIBRARY` 会触发
  `FindUrma.cmake` FetchContent 回退路径的既有缺陷（未包含
  `bond/include` 目录，`urma_ubagg.h` 不可见），与本次改动无关。

## 6. 结论

Jetty 池化核心（预算账本 / 准入票据 / 每 attempt 独占池 / 完成门 /
配置键 / 上下文集成缝）功能自测与并发压测全部通过；资源复用、
泄漏防护、异常降级三类验证点全覆盖；rm_pool 提升门保持 fail-closed，
满足“原有业务逻辑不受影响”的集成约束。配套配置说明见
`jetty_pool_config_guide.md`。
