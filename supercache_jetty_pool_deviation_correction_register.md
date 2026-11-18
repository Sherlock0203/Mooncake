# Jetty 池化与 Physical RM 偏差修正登记

日期：2026-11-10  
代码基线：`tent/tent_rm`，HEAD `e1d39c4d`，父提交 `0d3463c0`。  
范围：TENT UB 的 physical RM 前置代码、Jetty/endpoint 生命周期、bootstrap、设备筛选、URMA adapter、相关 UT/CMake。与本特性无关的 Mooncake 组件不纳入本登记。

## 证据边界

- 用户报告第一步前置代码的自测 UT 已全部通过；当前工作区没有可重放的测试命令、构建参数和报告，因此登记为“作者自测通过”，不替代 Linux CTest/CI、真实 provider 或双端硬件证据。
- 当前生产路径仍是 RC/bootstrap v1。RM v2 的 DTO、严格校验、trusted identity 校验和 generation compare helper 已存在，但生产 producer/consumer、事务提交、共享 target owner 和 RM native 数据面尚未接通。
- `tent/tests/CMakeLists.txt` 已注册 `tent_ub_core_test`、`tent_ub_teardown_test`、`tent_ub_native_data_path_test` 和 `tent_transport_selector_test`；是否在本次分支实际执行，必须以 CI 日志为准。

## 修正清单

| ID | 原文风险 | 代码证据 | 已完成修正 | 后续动作 |
|---|---|---|---|---|
| D-01 | 文档基线为旧 `tent/perf / 7f2d08f0`，把新增代码写成未提交 | `git branch --show-current`=`tent/tent_rm`；HEAD=`e1d39c4d`；父提交 `0d3463c0` | 设计文档页头和执行清单均改为当前分支/提交 | 在 CI 证据包记录完整 SHA 和工作树差异 |
| D-02 | 未体现“自测 UT 已通过”，又容易把 smoke 当成完整验收 | 用户声明自测通过；仓库无可重放报告 | 状态拆成“作者自测通过”“正式 CTest/CI 待证” | 测试 Owner 归档命令、环境、目标、总数和日志 |
| D-03 | “v2 已实现”可能被误读为生产握手已切 v2 | `bootstrap_types.h`/`bootstrap.cpp` 有 v2 DTO/validator；`ub_transport.cpp:onBootstrap` 仍以 RC gate；`endpoint.cpp:makeBootstrapDesc` 仍输出 v1 | 明确 v2 为前置校验脚手架，RM v2 在 native 分配前 fail-closed | P1 接入 producer/consumer、trusted metadata 原子提交和 target owner |
| D-04 | physical-only 可能被理解为 provider 自动证明“物理设备” | `device_selection.h` 从 UNKNOWN 开始；匹配显式 name/path/EID/index/mode identity；`openContext` 做 capability 复核 | 文档改为“受信配置准入”，不宣称硬件物理性证明 | 设备/实机 Owner 维护 allowlist 并提供版本化 inventory |
| D-05 | 同步 quiesce 接口和目标态异步排空混写 | `UrmaAdapter::quiesceJetty` 是同步 fence；当前 worker/poll 无目标态 ERROR/marker/delete 队列 | 文档区分 legacy 同步契约和 pooled 异步状态机 | P1 legacy 先验证；P3 实现异步 drain/delete |
| D-06 | 当前 endpoint 固定 Jetty/逐项 bind 被误写成共享池能力 | `endpoint.cpp:prepare/bind` 创建固定数量 Jetty，要求 peer Jetty 数量相等并逐项 `bindJetty` | 明确 Shared target、ImportedTargetRegistry、SendJettyPool 均未实现 | P1 RM legacy 迁移后才可进入 P2 池化 |
| D-07 | UMDK master 源码观察被当作 provider 保证 | 当前没有可核验 UMDK SHA、ABI、驱动、固件快照 | 删除旧固定 commit 的“当前依赖”措辞，改为验收时记录实际版本 | Provider Owner 提供契约、注入和硬件报告 |
| D-08 | 4/6 口、desired serving、默认数量易被读成当前运行配置 | `UbParams` 默认 RC、pool/probe false；`jetty_per_endpoint` 仅 legacy | 示例标为目标/占位；池容量必须由设备账本计算 | SRE/实机 Owner 冻结拓扑和预算 |
| D-09 | BONDP 编译依赖被写成已解除 | `urma_adapter.cpp` 在 real URMA 分支无条件包含 `urma_ubagg.h`，并实现 bonding context control；物理运行时不走该分支但仍编译依赖该头文件 | 设计文档将其改为最终物理构建准出要求并明确当前未满足 | P1 检查 CMake include 可见性并隔离 BONDP 专用编译依赖 |
| D-10 | 构建 fallback 跟踪浮动 UMDK `master` | 当前工作树 `FindUrma.cmake` diff 使用 `GIT_TAG master`，并要求生产安装系统 UMDK | 文档/验收清单改为冻结可复现 provider SHA；fallback 不作验收基线 | P0-2 固定 fallback 或限制用途，证据包记录 CMake 实际选中的 include/lib 来源 |

## 代码对照索引

| 模块 | 当前实现重点 | 结论 |
|---|---|---|
| 配置与模式 | `tent/include/tent/transport/ub/params.h`、`tent/src/transport/ub/ub_transport.cpp` | `rc_legacy` 默认；`rm_legacy`/`rm_pool` 与 `trans_mode` 严格匹配；未实现模式在安装前拒绝 |
| 设备身份 | `device_selection.h`、`urma_adapter.h` | UNKNOWN 初始值；显式 identity 匹配并校验 transport mask；legacy 才允许 bonding 名称启发式 |
| Bootstrap | `bootstrap_types.h`、`bootstrap.cpp`、`control_plane.cpp` | v1 保持 RC 兼容；v2 严格字段/数字/EID/代次/target/profile 校验；尚无生产 v2 事务 |
| Native 边界 | `urma_adapter.h/.cpp` | adapter 已有可注入接口和静态 ABI 常量断言；RM JFS/JFR/import 实现及真实 provider 验收未完成 |
| Endpoint/Context | `endpoint.cpp`、`context.cpp`、`endpoint_store.cpp` | 当前固定 endpoint Jetty、同步 bind/quiesce、inflight 配对与可重试清理；目标池组件不存在 |
| UT/CMake | `tests/ub_core_test.cpp`、`ub_native_data_path_test.cpp`、`ub_teardown_test.cpp`、`tests/CMakeLists.txt` | 测试代码和注册存在；执行证据需从 CI 归档 |

## 结论

D-01 至 D-10 的文档事实偏差已修正。B-01 至 B-10 属于外部 provider、拓扑、Runtime/Store、SLO 或可复现证据决策，不能由本地代码阅读关闭，统一登记在 `supercache_jetty_pool_blocking_decision_register.md`。
