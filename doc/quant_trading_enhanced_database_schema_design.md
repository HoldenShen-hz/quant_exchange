# 量化交易平台增强版数据库 Schema 设计文档

> 文档层级：平台增强设计层
>  
> 推荐读者：架构师、后端工程师、数据平台工程师
>  
> 建议前置阅读：[基础数据库设计](./quant_trading_database_schema_design.md) / [系统架构设计](./quant_trading_system_architecture_design.md)
>  
> 相关文档：[增强版 API](./quant_trading_enhanced_api_interface_definition.md) / [竞品分析与增强需求](./quant_trading_competitive_analysis_and_enhanced_requirements.md)

## 1. 文档目标

本文档定义 `quant_exchange` 在 MVP 数据库设计之上的增强版 Schema，用于支撑世界级量化平台所需的新增能力。

本设计基于以下文档继续扩展：

- [quant_trading_database_schema_design.md](./quant_trading_database_schema_design.md)
- [quant_trading_system_architecture_design.md](./quant_trading_system_architecture_design.md)
- [quant_trading_competitive_analysis_and_enhanced_requirements.md](./quant_trading_competitive_analysis_and_enhanced_requirements.md)

本文档的目标不是替换 MVP Schema，而是定义“增强版增量表结构”。

## 2. 增强版能力范围

本次增强版 Schema 主要覆盖以下新增模块：

- Universe / Screener / Pairlist
- Feature Store / Factor Expression Engine
- Research Notebook / Dataset Registry
- Experiment Tracking / Model Registry
- Rolling Retrain / Drift Monitor / Online Serving
- Bias Audit Center
- Advanced OMS / EMS
- Smart Order Router
- Strategy Controller / Executor Orchestrator
- Virtual Portfolio / Internal Ledger
- Alternative Data Platform
- Replay / Snapshot / Shadow Deployment
- Spread / Market Making / Options / DEX 扩展能力

## 3. Schema 域扩展规划

在现有 schema 之上，建议新增以下业务域：

- `universe`
  - 标的池、筛选规则、筛选结果
- `feature`
  - 特征定义、特征版本、特征值、血缘
- `research`
  - 研究项目、数据集、Notebook、Artifacts
- `ml`
  - 实验、模型、模型版本、部署、漂移监控
- `audit`
  - 回测偏差审计、数据泄漏、前视偏差、选择偏差检测
- `ems`
  - 高级执行算法、订单篮子、路由决策
- `ledger`
  - 虚拟子账户、内部账本、策略资金核算
- `alt`
  - Alternative Data 源、数据集、许可与质量元数据
- `replay`
  - 事件回放、状态快照、Shadow Deployment
- `mm`
  - 做市、套利、价差和专用引擎参数
- `opt`
  - 期权链、Greeks、IV 曲面
- `dex`
  - DEX connector、池子、LP 仓位、链上路由

## 4. 设计原则

- 所有增强模块尽量与 MVP 表解耦，通过外键关联
- 训练、实验、研究结果必须可复现
- 高成本时序数据按冷热分层
- 所有模型与信号产出必须记录版本和来源
- 高级执行和路由决策必须留下决策证据链
- 回放和 Shadow 运行必须具备完整状态快照

## 5. Universe / Screener Schema

## 5.1 `universe.universes`

用途：

- 标的池主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| universe_code | varchar(64) | UK, not null | 标的池编码 |
| universe_name | varchar(128) | not null | 标的池名称 |
| asset_class | varchar(32) | not null | `SPOT` / `PERP` / `EQUITY` / `OPTION` |
| scope_type | varchar(32) | not null | `STATIC` / `DYNAMIC` |
| owner_user_id | bigint | FK | 负责人 |
| status | varchar(32) | not null | `ACTIVE` / `INACTIVE` |
| description | text |  | 描述 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 5.2 `universe.universe_rules`

用途：

- 标的池筛选规则

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| universe_id | bigint | FK, not null | 标的池 ID |
| rule_type | varchar(64) | not null | `LIQUIDITY` / `VOLATILITY` / `FUNDAMENTAL` / `SENTIMENT` |
| expression | text | not null | 筛选表达式 |
| priority | int | not null default 100 | 优先级 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 5.3 `universe.universe_snapshots`

用途：

- 某时间点的实际标的池结果

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| snapshot_time | timestamptz | not null | 快照时间 |
| universe_id | bigint | FK, not null | 标的池 ID |
| instrument_id | bigint | FK, not null | 标的 ID |
| score | numeric(16, 8) |  | 综合得分 |
| inclusion_reason | jsonb |  | 入选原因 |
| created_at | timestamptz | not null | 创建时间 |

主键建议：

- primary key on `(snapshot_time, universe_id, instrument_id)`

## 6. Feature Store Schema

## 6.1 `feature.feature_definitions`

用途：

- 特征定义主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| feature_code | varchar(128) | UK, not null | 特征编码 |
| feature_name | varchar(255) | not null | 特征名称 |
| feature_type | varchar(32) | not null | `NUMERIC` / `CATEGORY` / `TEXT` |
| source_type | varchar(32) | not null | `MARKET` / `INTEL` / `ALT` / `DERIVED` |
| expression_lang | varchar(32) |  | `SQL` / `DSL` / `PYTHON` |
| expression_body | text |  | 计算表达式 |
| owner_user_id | bigint | FK | 所有人 |
| status | varchar(32) | not null | `ACTIVE` / `DEPRECATED` |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.2 `feature.feature_versions`

用途：

- 特征版本表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| feature_id | bigint | FK, not null | 特征 ID |
| version_no | varchar(32) | not null | 版本号 |
| logic_hash | varchar(64) | not null | 逻辑哈希 |
| schema_json | jsonb |  | 特征结构 |
| is_online_serving | boolean | not null default false | 是否用于在线服务 |
| created_at | timestamptz | not null | 创建时间 |

唯一约束：

- unique on `(feature_id, version_no)`

## 6.3 `feature.feature_lineage`

用途：

- 特征血缘关系

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| feature_version_id | bigint | FK, not null | 特征版本 |
| parent_type | varchar(32) | not null | `FEATURE` / `TABLE` / `DATASET` |
| parent_ref | varchar(255) | not null | 上游引用 |
| created_at | timestamptz | not null | 创建时间 |

## 6.4 `feature.feature_values`

用途：

- 特征值事实表

建议使用分区或列式存储。

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| feature_time | timestamptz | not null | 特征时间 |
| feature_version_id | bigint | FK, not null | 特征版本 |
| instrument_id | bigint | FK | 标的 |
| account_id | bigint | FK | 账户 |
| strategy_id | bigint | FK | 策略 |
| value_numeric | numeric(32, 16) |  | 数值特征值 |
| value_text | text |  | 文本特征值 |
| value_json | jsonb |  | 复杂特征值 |
| quality_score | numeric(16, 8) |  | 质量分数 |
| created_at | timestamptz | not null | 创建时间 |

## 7. Research Schema

## 7.1 `research.projects`

用途：

- 研究项目主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| project_code | varchar(64) | UK, not null | 项目编码 |
| project_name | varchar(255) | not null | 项目名称 |
| owner_user_id | bigint | FK | 负责人 |
| status | varchar(32) | not null | `ACTIVE` / `ARCHIVED` |
| description | text |  | 描述 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 7.2 `research.notebooks`

用途：

- Notebook 元数据表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| project_id | bigint | FK, not null | 项目 ID |
| notebook_name | varchar(255) | not null | Notebook 名称 |
| storage_uri | varchar(1024) | not null | 文件位置 |
| kernel_type | varchar(64) |  | Python / R |
| git_ref | varchar(255) |  | Git 引用 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 7.3 `research.datasets`

用途：

- 研究数据集注册表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| dataset_code | varchar(64) | UK, not null | 数据集编码 |
| dataset_name | varchar(255) | not null | 数据集名称 |
| dataset_type | varchar(32) | not null | `TRAIN` / `VALIDATION` / `TEST` / `RESEARCH` |
| source_ref | varchar(255) | not null | 来源引用 |
| schema_json | jsonb |  | 结构描述 |
| time_range | tstzrange |  | 时间范围 |
| created_at | timestamptz | not null | 创建时间 |

## 8. ML / Experiment Schema

## 8.1 `ml.experiments`

用途：

- 实验主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| experiment_code | varchar(64) | UK, not null | 实验编码 |
| project_id | bigint | FK | 研究项目 |
| experiment_name | varchar(255) | not null | 实验名称 |
| experiment_type | varchar(32) | not null | `BACKTEST` / `ML_TRAIN` / `OPTIMIZE` |
| owner_user_id | bigint | FK | 负责人 |
| status | varchar(32) | not null | `RUNNING` / `SUCCESS` / `FAILED` |
| tags | jsonb |  | 标签 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 8.2 `ml.experiment_runs`

用途：

- 实验运行实例表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| experiment_id | bigint | FK, not null | 实验 ID |
| run_code | varchar(64) | UK, not null | 运行编码 |
| dataset_id | bigint | FK | 数据集 |
| strategy_run_id | bigint | FK | 策略运行 |
| parameters | jsonb | not null | 参数 |
| metrics | jsonb |  | 结果指标 |
| artifact_uri | varchar(1024) |  | 输出位置 |
| started_at | timestamptz |  | 开始时间 |
| finished_at | timestamptz |  | 结束时间 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |

## 8.3 `ml.models`

用途：

- 模型注册表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| model_code | varchar(64) | UK, not null | 模型编码 |
| model_name | varchar(255) | not null | 模型名称 |
| model_type | varchar(64) | not null | `XGBOOST` / `LGBM` / `DL` / `LLM` |
| task_type | varchar(64) | not null | `ALPHA` / `DIRECTION` / `RISK` / `EXECUTION` |
| owner_user_id | bigint | FK | 负责人 |
| status | varchar(32) | not null | `ACTIVE` / `ARCHIVED` |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 8.4 `ml.model_versions`

用途：

- 模型版本表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| model_id | bigint | FK, not null | 模型 ID |
| version_no | varchar(32) | not null | 版本号 |
| training_run_id | bigint | FK | 训练运行 |
| feature_set_ref | varchar(255) |  | 特征集引用 |
| metrics | jsonb |  | 验证指标 |
| model_uri | varchar(1024) | not null | 模型文件位置 |
| status | varchar(32) | not null | `CANDIDATE` / `CHAMPION` / `RETIRED` |
| created_at | timestamptz | not null | 创建时间 |

唯一约束：

- unique on `(model_id, version_no)`

## 8.5 `ml.model_deployments`

用途：

- 模型部署表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| model_version_id | bigint | FK, not null | 模型版本 |
| deployment_target | varchar(64) | not null | `RESEARCH` / `PAPER` / `LIVE` |
| service_name | varchar(128) |  | 服务名 |
| endpoint_ref | varchar(255) |  | 在线接口引用 |
| traffic_weight | numeric(8, 4) |  | 流量比例 |
| status | varchar(32) | not null | `DEPLOYED` / `STOPPED` |
| deployed_at | timestamptz |  | 部署时间 |
| created_at | timestamptz | not null | 创建时间 |

## 8.6 `ml.model_drift_metrics`

用途：

- 模型漂移和稳定性监控

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| model_version_id | bigint | FK, not null | 模型版本 |
| metric_time | timestamptz | not null | 监控时间 |
| drift_type | varchar(64) | not null | `DATA` / `PREDICTION` / `TARGET` |
| metric_name | varchar(64) | not null | 指标名 |
| metric_value | numeric(16, 8) | not null | 指标值 |
| threshold | numeric(16, 8) |  | 阈值 |
| status | varchar(32) | not null | `NORMAL` / `WARN` / `ALERT` |
| created_at | timestamptz | not null | 创建时间 |

## 9. Bias Audit Schema

## 9.1 `audit.audit_jobs`

用途：

- 策略 / 数据质量审计任务

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| audit_code | varchar(64) | UK, not null | 审计编号 |
| audit_type | varchar(64) | not null | `LOOKAHEAD` / `LEAKAGE` / `SURVIVORSHIP` / `RECURSIVE` |
| target_type | varchar(32) | not null | `STRATEGY` / `DATASET` / `MODEL` |
| target_ref | varchar(255) | not null | 目标引用 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 9.2 `audit.audit_results`

用途：

- 审计结果表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| audit_job_id | bigint | FK, not null | 审计任务 |
| severity | varchar(32) | not null | `INFO` / `WARN` / `CRITICAL` |
| finding_code | varchar(64) | not null | 发现编码 |
| finding_title | varchar(255) | not null | 标题 |
| details | jsonb | not null | 详情 |
| evidence_uri | varchar(1024) |  | 证据位置 |
| created_at | timestamptz | not null | 创建时间 |

## 10. EMS / Smart Execution Schema

## 10.1 `ems.execution_algorithms`

用途：

- 执行算法主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| algo_code | varchar(64) | UK, not null | 算法编码 |
| algo_name | varchar(255) | not null | 算法名称 |
| algo_type | varchar(64) | not null | `TWAP` / `VWAP` / `ICEBERG` / `POV` / `SNIPER` |
| parameters_schema | jsonb |  | 参数结构 |
| status | varchar(32) | not null | `ACTIVE` / `INACTIVE` |
| created_at | timestamptz | not null | 创建时间 |

## 10.2 `ems.order_baskets`

用途：

- 订单篮子 / 母单表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| basket_code | varchar(64) | UK, not null | 母单编号 |
| account_id | bigint | FK, not null | 账户 |
| strategy_id | bigint | FK | 策略 |
| instrument_id | bigint | FK, not null | 标的 |
| side | varchar(16) | not null | 买卖方向 |
| target_quantity | numeric(32, 16) | not null | 目标数量 |
| executed_quantity | numeric(32, 16) | not null default 0 | 已执行数量 |
| algo_id | bigint | FK | 执行算法 |
| router_policy_id | bigint | FK | 路由策略 |
| basket_status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 10.3 `ems.router_decisions`

用途：

- 路由决策证据表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| basket_id | bigint | FK | 母单 |
| order_id | bigint | FK | 子单 |
| decision_time | timestamptz | not null | 决策时间 |
| venue_candidates | jsonb | not null | 候选 venue |
| selected_venue | varchar(64) | not null | 选中 venue |
| decision_factors | jsonb | not null | 价格、深度、费率等因子 |
| expected_slippage | numeric(16, 8) |  | 预估滑点 |
| expected_fee | numeric(16, 8) |  | 预估费率 |
| created_at | timestamptz | not null | 创建时间 |

## 10.4 `ems.order_basket_items`

用途：

- 母单与子订单映射表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| basket_id | bigint | FK, not null | 母单 ID |
| order_id | bigint | FK, not null | 子订单 ID |
| slice_no | int |  | 分片序号 |
| role_type | varchar(32) |  | `ENTRY` / `HEDGE` / `CHILD` |
| planned_quantity | numeric(32, 16) |  | 计划执行量 |
| created_at | timestamptz | not null | 创建时间 |

唯一约束：

- unique on `(basket_id, order_id)`

## 10.5 `ems.router_policies`

用途：

- 智能路由策略配置表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| policy_code | varchar(64) | UK, not null | 路由策略编码 |
| policy_name | varchar(255) | not null | 路由策略名称 |
| venue_scope | jsonb | not null | 可用 venue 范围 |
| preference_rules | jsonb | not null | 价格、深度、费率偏好规则 |
| fallback_rules | jsonb |  | 降级与兜底规则 |
| status | varchar(32) | not null | `ACTIVE` / `INACTIVE` |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 11. Strategy Orchestrator Schema

## 11.1 `strat.controllers`

用途：

- 策略控制器表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| controller_code | varchar(64) | UK, not null | 控制器编码 |
| controller_name | varchar(255) | not null | 控制器名称 |
| controller_type | varchar(64) | not null | `PORTFOLIO` / `MM` / `ARBITRAGE` |
| config_json | jsonb | not null | 配置 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 11.2 `strat.executors`

用途：

- 执行器表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| controller_id | bigint | FK | 控制器 |
| executor_code | varchar(64) | UK, not null | 执行器编码 |
| executor_type | varchar(64) | not null | `SIGNAL` / `HEDGE` / `ROUTE` / `LP` |
| config_json | jsonb | not null | 配置 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 12. Ledger Schema

## 12.1 `ledger.virtual_accounts`

用途：

- 虚拟子账户

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| parent_account_id | bigint | FK, not null | 主账户 |
| virtual_account_code | varchar(64) | UK, not null | 虚拟账户编码 |
| virtual_account_name | varchar(255) | not null | 名称 |
| currency | varchar(32) |  | 计价币种 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 12.2 `ledger.entries`

用途：

- 内部账本分录

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| virtual_account_id | bigint | FK, not null | 虚拟账户 |
| entry_time | timestamptz | not null | 分录时间 |
| entry_type | varchar(64) | not null | `ALLOCATE` / `TRADE` / `FEE` / `PNL` / `TRANSFER` |
| amount | numeric(32, 16) | not null | 金额 |
| currency | varchar(32) | not null | 币种 |
| ref_type | varchar(64) |  | 引用对象类型 |
| ref_id | varchar(64) |  | 引用对象 |
| created_at | timestamptz | not null | 创建时间 |

## 12.3 `ledger.transfers`

用途：

- 虚拟账户间内部资金划拨表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| transfer_code | varchar(64) | UK, not null | 划拨编号 |
| from_virtual_account_id | bigint | FK, not null | 转出账户 |
| to_virtual_account_id | bigint | FK, not null | 转入账户 |
| amount | numeric(32, 16) | not null | 划拨金额 |
| currency | varchar(32) | not null | 币种 |
| reason | text |  | 划拨原因 |
| status | varchar(32) | not null | `PENDING` / `APPROVED` / `COMPLETED` / `REJECTED` |
| created_by | bigint | FK | 发起人 |
| approved_by | bigint | FK | 审批人 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 13. Alternative Data Schema

## 13.1 `alt.data_sources`

用途：

- 另类数据源配置表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| source_code | varchar(64) | UK, not null | 数据源编码 |
| source_name | varchar(255) | not null | 数据源名称 |
| source_type | varchar(64) | not null | `ONCHAIN` / `MACRO` / `FUNDAMENTAL` / `WEB` |
| vendor_name | varchar(255) |  | 提供商 |
| license_level | varchar(64) |  | 许可级别 |
| metadata | jsonb |  | 元数据 |
| status | varchar(32) | not null | 状态 |
| created_at | timestamptz | not null | 创建时间 |

## 13.2 `alt.datasets`

用途：

- 另类数据集注册表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| source_id | bigint | FK, not null | 数据源 |
| dataset_code | varchar(64) | UK, not null | 数据集编码 |
| dataset_name | varchar(255) | not null | 名称 |
| dataset_schema | jsonb |  | 结构 |
| update_frequency | varchar(32) |  | 更新频率 |
| quality_score | numeric(16, 8) |  | 质量分数 |
| storage_uri | varchar(1024) |  | 存储位置 |
| created_at | timestamptz | not null | 创建时间 |

## 13.3 `alt.dataset_records`

用途：

- 另类数据记录表或记录索引表

说明：

- 若原始记录量极大，可将明细落对象存储 / 列式库，本表仅保存索引和查询必要字段

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| dataset_id | bigint | FK, not null | 数据集 |
| record_time | timestamptz | not null | 记录时间 |
| instrument_id | bigint | FK | 关联标的 |
| record_key | varchar(255) |  | 外部记录键 |
| payload | jsonb |  | 结构化内容 |
| storage_ref | varchar(1024) |  | 外部存储引用 |
| quality_score | numeric(16, 8) |  | 质量分数 |
| created_at | timestamptz | not null | 创建时间 |

## 14. Replay / Shadow Schema

## 14.1 `replay.event_logs`

用途：

- 可回放事件总表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| event_id | varchar(64) | UK, not null | 事件 ID |
| event_type | varchar(64) | not null | 事件类型 |
| source | varchar(64) | not null | 来源 |
| event_time | timestamptz | not null | 事件时间 |
| key_ref | varchar(255) |  | 业务键 |
| payload | jsonb | not null | 事件内容 |
| created_at | timestamptz | not null | 创建时间 |

## 14.2 `replay.state_snapshots`

用途：

- 状态快照表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| snapshot_code | varchar(64) | UK, not null | 快照编号 |
| snapshot_type | varchar(64) | not null | `ACCOUNT` / `POSITION` / `STRATEGY` / `SYSTEM` |
| target_ref | varchar(255) | not null | 目标引用 |
| snapshot_time | timestamptz | not null | 快照时间 |
| state_payload | jsonb | not null | 状态内容 |
| created_at | timestamptz | not null | 创建时间 |

## 14.3 `replay.replay_jobs`

用途：

- 回放任务表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| job_code | varchar(64) | UK, not null | 回放任务编号 |
| source_scope | jsonb | not null | 回放数据范围 |
| target_type | varchar(64) | not null | `STRATEGY` / `MODEL` / `ACCOUNT` |
| target_ref | varchar(255) | not null | 回放目标引用 |
| status | varchar(32) | not null | `PENDING` / `RUNNING` / `FAILED` / `COMPLETED` |
| metrics | jsonb |  | 回放结果指标 |
| started_at | timestamptz |  | 开始时间 |
| finished_at | timestamptz |  | 结束时间 |
| created_at | timestamptz | not null | 创建时间 |

## 14.4 `replay.shadow_deployments`

用途：

- Shadow / Canary 部署记录

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| deployment_code | varchar(64) | UK, not null | 部署编号 |
| strategy_id | bigint | FK | 策略 |
| model_version_id | bigint | FK | 模型版本 |
| source_deployment_id | bigint | FK | 来源部署 |
| environment | varchar(32) | not null | `PAPER` / `SHADOW` / `CANARY` |
| status | varchar(32) | not null | 状态 |
| start_time | timestamptz |  | 开始时间 |
| end_time | timestamptz |  | 结束时间 |
| metrics | jsonb |  | 对比指标 |
| created_at | timestamptz | not null | 创建时间 |

## 15. Options / MM / DEX 扩展 Schema

## 15.1 `opt.option_chains`

用途：

- 期权链快照表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| snapshot_time | timestamptz | not null | 快照时间 |
| exchange_id | bigint | FK, not null | 交易所 |
| underlying_instrument_id | bigint | FK, not null | 标的 |
| option_instrument_id | bigint | FK, not null | 期权合约 |
| expiry_date | date | not null | 到期日 |
| strike_price | numeric(32, 16) | not null | 行权价 |
| option_type | varchar(8) | not null | `CALL` / `PUT` |
| bid_price | numeric(32, 16) |  | 买价 |
| ask_price | numeric(32, 16) |  | 卖价 |
| mark_iv | numeric(16, 8) |  | IV |
| greeks | jsonb |  | Greeks |
| created_at | timestamptz | not null | 创建时间 |

## 15.2 `mm.market_making_configs`

用途：

- 做市策略配置表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| strategy_id | bigint | FK, not null | 策略 |
| instrument_id | bigint | FK, not null | 标的 |
| quoting_mode | varchar(64) | not null | `MID` / `VOL_ADJ` / `SKEWED` |
| spread_bps | numeric(16, 8) | not null | 报价价差 |
| inventory_target | numeric(32, 16) |  | 目标库存 |
| hedge_enabled | boolean | not null default false | 是否对冲 |
| created_at | timestamptz | not null | 创建时间 |

## 15.3 `dex.liquidity_positions`

用途：

- DEX LP 仓位

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| account_id | bigint | FK, not null | 账户 |
| protocol_name | varchar(128) | not null | 协议名 |
| pool_address | varchar(255) | not null | 池地址 |
| token0_symbol | varchar(32) | not null | 资产 0 |
| token1_symbol | varchar(32) | not null | 资产 1 |
| lower_tick | int |  | 下边界 |
| upper_tick | int |  | 上边界 |
| liquidity | numeric(32, 16) | not null | 流动性值 |
| fee_earned | numeric(32, 16) |  | 已赚手续费 |
| impermanent_loss | numeric(32, 16) |  | 无常损失 |
| snapshot_time | timestamptz | not null | 快照时间 |
| created_at | timestamptz | not null | 创建时间 |

## 16. 与现有 MVP 表的关键关系

增强版表与 MVP 表的关键关系如下：

- `universe.universe_snapshots.instrument_id` -> `ref.instruments.id`
- `feature.feature_values.instrument_id` -> `ref.instruments.id`
- `research.projects.owner_user_id` -> `sys.users.id`
- `ml.experiment_runs.strategy_run_id` -> `strat.strategy_runs.id`
- `ml.model_deployments` 可关联 `strat.strategy_deployments`
- `ems.order_baskets` 1:N `ems.order_basket_items`
- `ems.order_basket_items.order_id` -> `trade.orders.id`
- `ems.order_baskets.router_policy_id` -> `ems.router_policies.id`
- `ems.router_policies` 可用于 `ems.router_decisions`
- `ems.router_decisions.order_id` -> `trade.orders.id`
- `ledger.virtual_accounts.parent_account_id` -> `acct.trading_accounts.id`
- `ledger.transfers.from_virtual_account_id` -> `ledger.virtual_accounts.id`
- `ledger.transfers.to_virtual_account_id` -> `ledger.virtual_accounts.id`
- `replay.shadow_deployments.strategy_id` -> `strat.strategies.id`
- `opt.option_chains.underlying_instrument_id` -> `ref.instruments.id`

## 17. 强烈建议优先落地的增强表

如果不想一次性把所有增强模块都实现，建议先落以下表：

- `universe.universes`
- `universe.universe_rules`
- `universe.universe_snapshots`
- `feature.feature_definitions`
- `feature.feature_versions`
- `feature.feature_values`
- `ml.experiments`
- `ml.experiment_runs`
- `ml.models`
- `ml.model_versions`
- `audit.audit_jobs`
- `audit.audit_results`
- `ems.execution_algorithms`
- `ems.order_baskets`
- `ems.order_basket_items`
- `ems.router_policies`
- `ems.router_decisions`
- `alt.dataset_records`
- `replay.replay_jobs`
- `replay.event_logs`
- `replay.state_snapshots`

## 18. 索引与分区建议

### 18.1 建议分区的大表

- `feature.feature_values`
- `replay.event_logs`
- `universe.universe_snapshots`
- `opt.option_chains`

### 18.2 索引重点

- 时间 + 业务对象复合索引
- 所有业务编码加唯一索引
- 高频查询对象建立倒序时间索引
- JSONB 中常用筛选字段考虑 GIN 索引

## 19. 结论

这份增强版 Schema 让 `quant_exchange` 从“可运行的量化交易系统”提升为“可研究、可实验、可审计、可扩展的量化交易平台”。

如果继续推进，建议下一步输出：

- 增强版 DDL 草案
- 增强版 API 接口定义
- 数据生命周期与归档设计
