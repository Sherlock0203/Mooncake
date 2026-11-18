# Jetty 池化与 Physical RM 待沟通决策登记

日期：2026-11-10  
状态：`待 Owner 回复`。仓库只定义角色，没有具名负责人或沟通渠道；本文件记录沟通输入和关闭证据，不能替代真实签字。

## 使用规则

每项关闭时必须补：负责人姓名/角色、决定、适用 Mooncake/UMDK/驱动/固件版本或部署范围、日期、证据链接。只有这些字段齐全，状态才可改为 `已确认`。建议项只是编码默认，不是外部承诺。

## 决策清单

| ID | 当前上下文与技术模块 | 可选方案（推荐） | 影响范围 | 关闭证据 |
|---|---|---|---|---|
| B-01 | UMDK RM/JFS priority→RTP、JFR/JFC 配置、RM WR；模块：`urma_adapter`/native provider | **仅启用实测通过的设备/priority 组合**；或建立 provider 版本级矩阵 | P1 adapter、profile gate、可用设备 | Mooncake/UMDK SHA、ABI、驱动/固件、双端 READ/WRITE 结果 |
| B-02 | 多发送 Jetty/多 peer/业务+probe 共享 target；模块：target registry/import/fence | 每 peer 独立 target；**共享 target，但以 provider 并发/撤销契约和压力结果为准** | P1 target owner、P2 registry、P4 故障域 | 并发矩阵、入站 DMA fence/撤销契约、硬件压力报告 |
| B-03 | `modify ERROR`、flush-done、`urma_flush_jetty`、WR_UNHANDLED、AE ACK、completion length；模块：poll/drain/delete | legacy 同步 fence；**pooled 使用异步 ERROR/marker/poll/flush/delete 屏障** | P1 teardown、P3 异步状态机 | 逐版本契约表、故障注入、双端硬件结果 |
| B-04 | delete 失败的 handle 状态、JFC poll/delete/clean 并发、lock-free；模块：delete/refill/shutdown | provider 明确 retained/consumed；**无法判定统一 UNKNOWN/quarantine** | native 预算、重试、shutdown、ABA | 对象表/AE/ioctl 注入报告，版本化处置矩阵 |
| B-05 | port inventory、port_id、邻接、4/6 口及非对称拓扑；模块：topology/bootstrap/path registry | 动态 inventory；**先显式静态 inventory+checksum+epoch** | 路径数、target 发布、灰度拓扑 | schema、4/6/非对称样例、epoch 变更测试 |
| B-06 | 业务池、probe、target、JFC/JFR、MR/import/quarantine 替换预算；模块：capacity ledger/refill | 固定统一值；按 capability 配置；**按设备 capability+端口邻接计算并留控制/替换余量** | 启动成功率、池容量、故障恢复 | capability snapshot、容量公式、上限压测 |
| B-07 | TENT/Store/staging 的 buffer pin/unpin、H2D/CROSS/POST、shutdown owner；模块：Runtime/Store | host pinned only；扩展统一 owner；**统一 owner token 和释放门** | DMA 安全、取消/超时、析构 | READ/WRITE、timeout/cancel、析构/shutdown 联测 |
| B-08 | 一字节 WRITE 探测槽位、权限、并发、timeout Unknown、业务错误 campaign；模块：ProbeResource/health | peer 专属槽；共享槽串行；**每 LocalTarget 独立受限槽+ProbeResource，实测并发** | P4 probe、授权、路径恢复 | 槽位生命周期、fence、并发、stale/timeout 测试 |
| B-09 | 性能负载、对照、SLO、灰度回滚阈值；模块：benchmark/SRE | 只比较；吞吐门槛；**冻结吞吐+p99+CPU+错误率组合阈值** | P5 验收、正式启用、回滚 | benchmark protocol、数值阈值、rollback trigger 签字 |
| B-10 | 用户报告自测 UT 全通过，但缺命令、构建参数、覆盖清单、日志；模块：测试/CI | 归档自测；CI 重跑；两者；**归档自测并正式 CI 重跑** | P0/P1 证据可信度和复现 | CI job、SHA、配置、测试总数和日志 |

## 沟通闭环模板

```text
ID: B-__
Owner:
Decision: APPROVE / REJECT / CONDITIONAL
Scope/version:
Constraints:
Evidence:
Effective date:
Follow-up task and due milestone:
```

## 当前闭环结论

截至 2026-11-10，B-01 至 B-10 均未取得具名 Owner 回复或可核验外部证据，故不能宣称“无阻塞正式版”。前置代码、文档事实修正和不依赖这些决策的 fake/单元测试可以继续；P1 真实 RM、P2 池化、P3 异步回收、P4 探测、P5 灰度必须按对应项阻塞。
