# 量化交易工具数据库表结构设计文档

> 文档层级：平台设计层
>  
> 推荐读者：后端工程师、数据库工程师、数据平台工程师
>  
> 建议前置阅读：[系统架构设计](./quant_trading_system_architecture_design.md) / [详细功能规格](./quant_trading_detailed_function_spec.md)
>  
> 相关文档：[增强版 Schema](./quant_trading_enhanced_database_schema_design.md) / [编码前准备](./quant_trading_pre_implementation_readiness_plan.md)

## 1. 文档目标

本文档定义 `quant_exchange` 的数据库结构设计，覆盖以下内容：

- 数据库选型与分层
- 逻辑 schema 规划
- 核心表结构
- 主键、唯一键、索引设计
- MVP 必要表与二阶段扩展表

本文档基于以下设计前提：

- MVP 采用 PostgreSQL 作为主业务库
- 使用 TimescaleDB 承载时序 K 线和快照数据
- Redis 只做缓存和事件通道，不做最终事实存储

## 2. 数据库设计原则

- 业务事实与分析结果分层存储
- 配置、订单、风险、审计数据必须强一致持久化
- 市场数据按时间和标的维度优化查询
- 所有关键业务对象必须带创建时间、更新时间
- 所有高风险配置变更必须可追溯
- 表结构优先满足 MVP 闭环，扩展字段预留但不盲目过度设计

## 3. 数据域与 Schema 规划

建议在 PostgreSQL 中按业务域划分 schema：

- `sys`
  - 用户、角色、审计、系统配置
- `ref`
  - 交易所、标的、枚举参考数据
- `acct`
  - 交易账户、API 凭证、账户快照
- `strat`
  - 策略、版本、参数、部署、运行记录
- `intel`
  - 情报源、新闻文档、情绪评分、方向信号
- `mkt`
  - 市场 K 线、行情快照
- `trade`
  - 订单、成交、持仓、持仓快照
- `risk`
  - 风控规则、风控事件、熔断记录
- `rpt`
  - 报表结果、绩效快照
- `ops`
  - 调度任务、告警、通知日志

## 4. 数据库选型与职责分工

### 4.1 PostgreSQL

用于存储：

- 用户和权限
- 账户与策略配置
- 订单与成交流水
- 持仓和账户快照
- 情报源配置、新闻文档、情绪信号
- 风控规则与事件
- 报表结果
- 调度和告警日志

### 4.2 TimescaleDB

用于存储：

- K 线数据
- 账户权益快照
- 持仓快照

MVP 不强制引入 Tick 和盘口表；如需要二阶段可接入 ClickHouse。

### 4.3 Redis

用于：

- 幂等键
- 实时事件
- 短期缓存
- 分布式锁

## 5. 通用字段规范

所有业务主表建议统一具备以下字段：

- `id`
- `created_at`
- `updated_at`
- `created_by`，如适用
- `updated_by`，如适用
- `remark`，如适用

字段类型建议：

- 主键：`bigserial` 或 `uuid`
- 金额：`numeric(32, 16)`
- 价格：`numeric(32, 16)`
- 数量：`numeric(32, 16)`
- 状态：`varchar(32)` 或 PostgreSQL enum
- 时间：`timestamptz`
- 结构化配置：`jsonb`

命名规则建议：

- 表名小写下划线
- 外键字段以 `_id` 结尾
- 业务唯一编码单独使用 `code`

## 6. 核心表结构设计

## 6.1 `sys.users`

用途：

- 系统用户表

字段设计：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 用户主键 |
| username | varchar(64) | UK, not null | 用户名 |
| password_hash | varchar(255) | not null | 密码哈希 |
| display_name | varchar(128) |  | 显示名 |
| email | varchar(128) |  | 邮箱 |
| mobile | varchar(32) |  | 手机号 |
| status | varchar(32) | not null | `ACTIVE` / `DISABLED` |
| last_login_at | timestamptz |  | 最后登录时间 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

索引建议：

- unique index on `username`
- index on `status`

## 6.2 `sys.roles`

用途：

- 系统角色表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 角色主键 |
| role_code | varchar(64) | UK, not null | 角色编码 |
| role_name | varchar(128) | not null | 角色名称 |
| description | text |  | 描述 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.3 `sys.user_roles`

用途：

- 用户和角色映射表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| user_id | bigint | FK, not null | 用户 ID |
| role_id | bigint | FK, not null | 角色 ID |
| created_at | timestamptz | not null | 创建时间 |

唯一约束：

- unique on `(user_id, role_id)`

## 6.4 `sys.audit_logs`

用途：

- 审计日志，记录所有关键操作

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| user_id | bigint | FK | 操作用户 |
| action_type | varchar(64) | not null | 操作类型 |
| target_type | varchar(64) | not null | 目标对象类型 |
| target_id | varchar(64) |  | 目标对象 ID |
| request_id | varchar(64) |  | 请求 ID |
| payload_before | jsonb |  | 变更前 |
| payload_after | jsonb |  | 变更后 |
| ip_address | varchar(64) |  | 来源 IP |
| created_at | timestamptz | not null | 操作时间 |

索引建议：

- index on `(action_type, created_at desc)`
- index on `(target_type, target_id)`

## 6.5 `ref.exchanges`

用途：

- 交易所基础信息

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| exchange_code | varchar(32) | UK, not null | 交易所或通道编码，如 `BINANCE` / `SSE` / `HKEX` / `NYSE` |
| exchange_name | varchar(128) | not null | 名称 |
| market_type | varchar(32) | not null | `CRYPTO` / `FUTURES` / `EQUITY` / `MULTI_ASSET` |
| status | varchar(32) | not null | 状态 |
| api_base_url | varchar(255) |  | REST 地址 |
| ws_base_url | varchar(255) |  | WebSocket 地址 |
| rate_limit_config | jsonb |  | 限频配置 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.6 `ref.instruments`

用途：

- 统一标的主数据表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| exchange_id | bigint | FK, not null | 所属交易所 |
| symbol | varchar(64) | not null | 平台统一标的代码 |
| exchange_symbol | varchar(64) | not null | 交易所或券商原始代码 |
| market_region | varchar(16) | not null | `CN` / `HK` / `US` / `GLOBAL` |
| base_asset | varchar(32) | not null | 基础资产；股票场景下可取证券标识 |
| quote_asset | varchar(32) | not null | 计价资产；股票场景下一般为 `CNY` / `HKD` / `USD` |
| settlement_asset | varchar(32) |  | 结算资产或结算币种 |
| settlement_cycle | varchar(16) |  | `T+1` / `T+2` 等结算周期 |
| instrument_type | varchar(32) | not null | `SPOT` / `PERPETUAL` / `DELIVERY_FUTURE` / `FUTURE` / `EQUITY` |
| contract_size | numeric(32, 16) |  | 合约面值 |
| contract_multiplier | numeric(32, 16) |  | 合约乘数 |
| expiry_at | timestamptz |  | 到期或交割时间，现货和永续为空 |
| tick_size | numeric(32, 16) | not null | 最小价格变动 |
| lot_size | numeric(32, 16) | not null | 最小数量变动 |
| min_qty | numeric(32, 16) |  | 最小下单量 |
| min_notional | numeric(32, 16) |  | 最小成交额 |
| short_sellable | boolean |  | 是否允许融券或卖空 |
| trading_session | jsonb |  | 交易时段与交易日历信息 |
| trading_rule_profile | jsonb |  | 涨跌停、盘前盘后、board lot、竞价阶段等规则摘要 |
| corporate_action_profile | jsonb |  | 分红、送配、拆合股、停牌等公司行为摘要 |
| status | varchar(32) | not null | `ACTIVE` / `INACTIVE` |
| raw_metadata | jsonb |  | 原始元数据 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

唯一约束：

- unique on `(exchange_id, exchange_symbol)`
- unique on `(exchange_id, symbol)`

## 6.7 `acct.trading_accounts`

用途：

- 交易账户主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| account_code | varchar(64) | UK, not null | 账户编码 |
| account_name | varchar(128) | not null | 账户名称 |
| exchange_id | bigint | FK, not null | 交易所 |
| account_type | varchar(32) | not null | `SPOT` / `FUTURES` / `EQUITY_CASH` / `EQUITY_MARGIN` / `PAPER` |
| environment | varchar(32) | not null | `TEST` / `PAPER` / `PROD` |
| status | varchar(32) | not null | 状态 |
| currency | varchar(32) |  | 计价币种 |
| leverage_default | numeric(16, 8) |  | 默认杠杆 |
| risk_profile | jsonb |  | 风险参数摘要 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.8 `acct.account_api_credentials`

用途：

- 账户 API 凭证

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| account_id | bigint | FK, not null | 账户 ID |
| api_key_encrypted | text | not null | 加密后的 API Key |
| api_secret_encrypted | text | not null | 加密后的 Secret |
| passphrase_encrypted | text |  | 某些交易所使用 |
| key_scope | varchar(32) | not null | `READ_ONLY` / `TRADE` |
| status | varchar(32) | not null | `ACTIVE` / `DISABLED` |
| rotated_at | timestamptz |  | 最近轮换时间 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

注意：

- 此表严禁明文存储敏感信息
- 应限制只有服务端进程可读

## 6.9 `acct.account_snapshots`

用途：

- 账户权益快照表

建议作为 TimescaleDB hypertable。

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| snapshot_time | timestamptz | not null | 快照时间 |
| account_id | bigint | not null | 账户 ID |
| equity | numeric(32, 16) | not null | 总权益 |
| cash_balance | numeric(32, 16) |  | 现金余额 |
| available_balance | numeric(32, 16) |  | 可用余额 |
| margin_used | numeric(32, 16) |  | 已用保证金 |
| unrealized_pnl | numeric(32, 16) |  | 未实现盈亏 |
| realized_pnl | numeric(32, 16) |  | 已实现盈亏 |
| raw_payload | jsonb |  | 原始快照 |
| created_at | timestamptz | not null | 入库时间 |

主键建议：

- primary key on `(snapshot_time, account_id)`

## 6.10 `strat.strategies`

用途：

- 策略主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| strategy_code | varchar(64) | UK, not null | 策略编码 |
| strategy_name | varchar(128) | not null | 策略名称 |
| category | varchar(64) |  | 趋势、均值、套利等 |
| owner_user_id | bigint | FK | 策略负责人 |
| runtime_type | varchar(32) | not null | `BACKTEST` / `PAPER` / `LIVE` |
| status | varchar(32) | not null | `DRAFT` / `ACTIVE` / `PAUSED` / `ARCHIVED` |
| description | text |  | 描述 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.11 `strat.strategy_versions`

用途：

- 策略版本表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| strategy_id | bigint | FK, not null | 策略 ID |
| version_no | varchar(32) | not null | 版本号 |
| code_ref | varchar(255) |  | Git commit / tag |
| config_schema | jsonb |  | 参数结构定义 |
| changelog | text |  | 更新说明 |
| created_by | bigint | FK | 创建人 |
| created_at | timestamptz | not null | 创建时间 |

唯一约束：

- unique on `(strategy_id, version_no)`

## 6.12 `strat.strategy_parameters`

用途：

- 策略参数实例表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| strategy_version_id | bigint | FK, not null | 版本 ID |
| parameter_set_name | varchar(128) | not null | 参数集名称 |
| parameters | jsonb | not null | 参数 JSON |
| is_default | boolean | not null default false | 是否默认 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.13 `strat.strategy_deployments`

用途：

- 策略部署记录，记录策略在哪个账户、什么模式下运行

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| strategy_id | bigint | FK, not null | 策略 ID |
| strategy_version_id | bigint | FK, not null | 版本 ID |
| parameter_id | bigint | FK, not null | 参数集 ID |
| account_id | bigint | FK, not null | 账户 ID |
| mode | varchar(32) | not null | `BACKTEST` / `PAPER` / `LIVE` |
| status | varchar(32) | not null | `CREATED` / `RUNNING` / `PAUSED` / `STOPPED` |
| capital_limit | numeric(32, 16) |  | 资金上限 |
| symbol_scope | jsonb |  | 标的范围 |
| risk_profile | jsonb |  | 部署级风控参数 |
| started_at | timestamptz |  | 开始时间 |
| stopped_at | timestamptz |  | 结束时间 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

索引建议：

- index on `(account_id, mode, status)`
- index on `(strategy_id, status)`

## 6.14 `strat.strategy_runs`

用途：

- 运行实例表，记录一次回测、模拟盘或实盘运行

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| run_id | varchar(64) | UK, not null | 运行唯一 ID |
| deployment_id | bigint | FK | 部署 ID |
| strategy_id | bigint | FK, not null | 策略 ID |
| strategy_version_id | bigint | FK, not null | 版本 ID |
| account_id | bigint | FK | 账户 ID |
| mode | varchar(32) | not null | `BACKTEST` / `PAPER` / `LIVE` |
| status | varchar(32) | not null | `PENDING` / `RUNNING` / `FAILED` / `COMPLETED` |
| start_time | timestamptz |  | 运行起始时间 |
| end_time | timestamptz |  | 运行结束时间 |
| run_config | jsonb | not null | 运行配置 |
| result_summary | jsonb |  | 结果摘要 |
| error_message | text |  | 错误信息 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.15 `mkt.market_klines`

用途：

- 历史 K 线表

建议作为 TimescaleDB hypertable。

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| open_time | timestamptz | not null | K 线开始时间 |
| close_time | timestamptz | not null | K 线结束时间 |
| exchange_id | bigint | not null | 交易所 |
| instrument_id | bigint | not null | 标的 ID |
| interval | varchar(16) | not null | 周期，如 `1m` |
| open_price | numeric(32, 16) | not null | 开盘价 |
| high_price | numeric(32, 16) | not null | 最高价 |
| low_price | numeric(32, 16) | not null | 最低价 |
| close_price | numeric(32, 16) | not null | 收盘价 |
| volume | numeric(32, 16) | not null | 成交量 |
| turnover | numeric(32, 16) |  | 成交额 |
| trade_count | bigint |  | 成交笔数 |
| source | varchar(32) |  | 数据来源 |
| created_at | timestamptz | not null | 入库时间 |

主键建议：

- primary key on `(open_time, exchange_id, instrument_id, interval)`

索引建议：

- index on `(instrument_id, interval, open_time desc)`

## 6.16 `trade.orders`

用途：

- 内部订单主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| internal_order_id | varchar(64) | UK, not null | 内部订单号 |
| client_order_id | varchar(64) | UK | 幂等业务号 |
| exchange_order_id | varchar(128) |  | 交易所订单号 |
| account_id | bigint | FK, not null | 账户 ID |
| strategy_id | bigint | FK | 策略 ID |
| run_id | varchar(64) |  | 运行 ID |
| instrument_id | bigint | FK, not null | 标的 ID |
| side | varchar(16) | not null | `BUY` / `SELL` |
| position_side | varchar(16) |  | `LONG` / `SHORT` |
| order_type | varchar(32) | not null | `MARKET` / `LIMIT` / `STOP` |
| time_in_force | varchar(16) |  | `GTC` / `IOC` / `FOK` |
| price | numeric(32, 16) |  | 委托价 |
| quantity | numeric(32, 16) | not null | 委托数量 |
| filled_quantity | numeric(32, 16) | not null default 0 | 已成交数量 |
| avg_fill_price | numeric(32, 16) |  | 平均成交价 |
| status | varchar(32) | not null | 订单状态 |
| submit_status | varchar(32) | not null | 提交状态 |
| error_code | varchar(64) |  | 错误码 |
| error_message | text |  | 错误信息 |
| request_payload | jsonb |  | 下单请求原文 |
| exchange_response | jsonb |  | 交易所响应 |
| created_at | timestamptz | not null | 创建时间 |
| submitted_at | timestamptz |  | 提交时间 |
| finished_at | timestamptz |  | 结束时间 |
| updated_at | timestamptz | not null | 更新时间 |

索引建议：

- unique on `internal_order_id`
- unique on `client_order_id`
- index on `(account_id, status, created_at desc)`
- index on `(strategy_id, created_at desc)`
- index on `(exchange_order_id)`

## 6.17 `trade.order_events`

用途：

- 订单状态变更流水表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| order_id | bigint | FK, not null | 订单主键 |
| event_type | varchar(64) | not null | 事件类型 |
| from_status | varchar(32) |  | 原状态 |
| to_status | varchar(32) |  | 新状态 |
| payload | jsonb |  | 事件上下文 |
| event_time | timestamptz | not null | 事件时间 |
| created_at | timestamptz | not null | 写入时间 |

索引建议：

- index on `(order_id, event_time asc)`

## 6.18 `trade.executions`

用途：

- 成交流水表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| execution_id | varchar(128) | UK, not null | 成交唯一号 |
| order_id | bigint | FK, not null | 订单 ID |
| exchange_trade_id | varchar(128) |  | 交易所成交号 |
| account_id | bigint | FK, not null | 账户 ID |
| strategy_id | bigint | FK | 策略 ID |
| instrument_id | bigint | FK, not null | 标的 ID |
| side | varchar(16) | not null | 买卖方向 |
| fill_price | numeric(32, 16) | not null | 成交价 |
| fill_quantity | numeric(32, 16) | not null | 成交量 |
| fill_amount | numeric(32, 16) |  | 成交额 |
| fee_amount | numeric(32, 16) |  | 手续费 |
| fee_asset | varchar(32) |  | 手续费币种 |
| liquidity_type | varchar(16) |  | `MAKER` / `TAKER` |
| execution_time | timestamptz | not null | 成交时间 |
| raw_payload | jsonb |  | 原始回报 |
| created_at | timestamptz | not null | 创建时间 |

索引建议：

- index on `(account_id, execution_time desc)`
- index on `(strategy_id, execution_time desc)`
- index on `(instrument_id, execution_time desc)`

## 6.19 `trade.positions`

用途：

- 当前持仓表，只保留当前最新状态

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| account_id | bigint | FK, not null | 账户 ID |
| strategy_id | bigint | FK | 策略 ID，可为空表示账户总持仓 |
| instrument_id | bigint | FK, not null | 标的 ID |
| position_side | varchar(16) | not null | `LONG` / `SHORT` / `NET` |
| quantity | numeric(32, 16) | not null | 持仓数量 |
| available_quantity | numeric(32, 16) |  | 可平数量 |
| avg_open_price | numeric(32, 16) |  | 开仓均价 |
| mark_price | numeric(32, 16) |  | 标记价格 |
| unrealized_pnl | numeric(32, 16) |  | 未实现盈亏 |
| realized_pnl | numeric(32, 16) |  | 已实现盈亏 |
| margin_used | numeric(32, 16) |  | 已用保证金 |
| updated_from | varchar(32) |  | `EXCHANGE` / `SYSTEM` |
| snapshot_time | timestamptz | not null | 状态时间 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

唯一约束：

- unique on `(account_id, strategy_id, instrument_id, position_side)`

## 6.20 `trade.position_snapshots`

用途：

- 持仓历史快照表

建议作为 TimescaleDB hypertable。

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| snapshot_time | timestamptz | not null | 快照时间 |
| account_id | bigint | not null | 账户 ID |
| strategy_id | bigint |  | 策略 ID |
| instrument_id | bigint | not null | 标的 ID |
| position_side | varchar(16) | not null | 持仓方向 |
| quantity | numeric(32, 16) | not null | 持仓数量 |
| avg_open_price | numeric(32, 16) |  | 均价 |
| mark_price | numeric(32, 16) |  | 标记价 |
| unrealized_pnl | numeric(32, 16) |  | 未实现盈亏 |
| margin_used | numeric(32, 16) |  | 保证金 |
| created_at | timestamptz | not null | 入库时间 |

## 6.21 `risk.risk_rules`

用途：

- 风控规则主表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| rule_code | varchar(64) | UK, not null | 规则编码 |
| rule_name | varchar(128) | not null | 规则名称 |
| rule_type | varchar(64) | not null | `ORDER` / `POSITION` / `ACCOUNT` / `SYSTEM` |
| scope_type | varchar(64) | not null | `GLOBAL` / `ACCOUNT` / `STRATEGY` / `INSTRUMENT` |
| expression_type | varchar(64) | not null | `THRESHOLD` / `SCRIPT` |
| parameters | jsonb | not null | 阈值或配置参数 |
| action_type | varchar(64) | not null | `WARN` / `BLOCK` / `REDUCE` / `STOP` |
| priority | int | not null default 100 | 优先级 |
| status | varchar(32) | not null | `ACTIVE` / `INACTIVE` |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.22 `risk.risk_rule_bindings`

用途：

- 风控规则绑定范围表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| rule_id | bigint | FK, not null | 规则 ID |
| account_id | bigint | FK | 账户 ID |
| strategy_id | bigint | FK | 策略 ID |
| instrument_id | bigint | FK | 标的 ID |
| binding_scope | varchar(32) | not null | 绑定范围 |
| created_at | timestamptz | not null | 创建时间 |

## 6.23 `risk.risk_events`

用途：

- 风控事件表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| event_id | varchar(64) | UK, not null | 事件编号 |
| rule_id | bigint | FK | 命中的规则 |
| account_id | bigint | FK | 账户 ID |
| strategy_id | bigint | FK | 策略 ID |
| instrument_id | bigint | FK | 标的 ID |
| order_id | bigint | FK | 关联订单 |
| severity | varchar(32) | not null | `INFO` / `WARN` / `CRITICAL` |
| action_taken | varchar(64) | not null | 实际动作 |
| reason_code | varchar(64) | not null | 原因码 |
| event_payload | jsonb | not null | 事件详情 |
| event_time | timestamptz | not null | 事件时间 |
| created_at | timestamptz | not null | 创建时间 |

索引建议：

- index on `(account_id, event_time desc)`
- index on `(strategy_id, event_time desc)`
- index on `(severity, event_time desc)`

## 6.24 `risk.kill_switch_events`

用途：

- 全局停机 / 策略停机事件表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| target_type | varchar(32) | not null | `GLOBAL` / `ACCOUNT` / `STRATEGY` |
| target_id | varchar(64) |  | 目标对象 |
| trigger_source | varchar(32) | not null | `SYSTEM` / `USER` |
| reason | text | not null | 触发原因 |
| status | varchar(32) | not null | `ACTIVE` / `RELEASED` |
| triggered_at | timestamptz | not null | 触发时间 |
| released_at | timestamptz |  | 恢复时间 |
| created_by | bigint | FK | 操作人 |
| created_at | timestamptz | not null | 创建时间 |

## 6.25 `rpt.daily_account_metrics`

用途：

- 账户日报指标表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| metric_date | date | not null | 指标日期 |
| account_id | bigint | FK, not null | 账户 ID |
| equity_start | numeric(32, 16) |  | 期初权益 |
| equity_end | numeric(32, 16) |  | 期末权益 |
| pnl_daily | numeric(32, 16) |  | 当日盈亏 |
| pnl_cumulative | numeric(32, 16) |  | 累计盈亏 |
| max_drawdown | numeric(16, 8) |  | 最大回撤 |
| order_count | int |  | 下单数 |
| trade_count | int |  | 成交数 |
| win_rate | numeric(16, 8) |  | 胜率 |
| fee_total | numeric(32, 16) |  | 总手续费 |
| slippage_cost | numeric(32, 16) |  | 滑点成本 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

唯一约束：

- unique on `(metric_date, account_id)`

## 6.26 `rpt.daily_strategy_metrics`

用途：

- 策略日报指标表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| metric_date | date | not null | 日期 |
| strategy_id | bigint | FK, not null | 策略 ID |
| account_id | bigint | FK | 账户 ID |
| run_mode | varchar(32) | not null | 模式 |
| pnl_daily | numeric(32, 16) |  | 当日盈亏 |
| pnl_cumulative | numeric(32, 16) |  | 累计盈亏 |
| max_drawdown | numeric(16, 8) |  | 最大回撤 |
| sharpe | numeric(16, 8) |  | 夏普 |
| win_rate | numeric(16, 8) |  | 胜率 |
| turnover | numeric(32, 16) |  | 换手率 |
| order_count | int |  | 下单数 |
| trade_count | int |  | 成交数 |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

唯一约束：

- unique on `(metric_date, strategy_id, account_id, run_mode)`

## 6.27 `ops.scheduled_jobs`

用途：

- 定时任务定义表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| job_code | varchar(64) | UK, not null | 任务编码 |
| job_name | varchar(128) | not null | 任务名称 |
| job_type | varchar(64) | not null | `DATA_SYNC` / `REPORT` / `RECONCILE` |
| cron_expr | varchar(64) | not null | 调度表达式 |
| payload | jsonb |  | 参数 |
| status | varchar(32) | not null | `ACTIVE` / `PAUSED` |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.28 `ops.job_runs`

用途：

- 定时任务运行日志表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| job_id | bigint | FK, not null | 任务 ID |
| run_no | varchar(64) | not null | 运行编号 |
| status | varchar(32) | not null | `RUNNING` / `SUCCESS` / `FAILED` |
| started_at | timestamptz | not null | 开始时间 |
| finished_at | timestamptz |  | 结束时间 |
| result_summary | jsonb |  | 结果摘要 |
| error_message | text |  | 错误信息 |
| created_at | timestamptz | not null | 创建时间 |

## 6.29 `ops.alerts`

用途：

- 告警事件表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| alert_code | varchar(64) | not null | 告警编码 |
| severity | varchar(32) | not null | `INFO` / `WARN` / `CRITICAL` |
| source_type | varchar(64) | not null | 来源模块 |
| source_id | varchar(64) |  | 来源对象 ID |
| title | varchar(255) | not null | 标题 |
| content | text | not null | 内容 |
| status | varchar(32) | not null | `OPEN` / `ACKED` / `CLOSED` |
| triggered_at | timestamptz | not null | 触发时间 |
| acknowledged_at | timestamptz |  | 确认时间 |
| closed_at | timestamptz |  | 关闭时间 |
| created_at | timestamptz | not null | 创建时间 |

## 6.30 `ops.notification_logs`

用途：

- 通知发送日志表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| alert_id | bigint | FK | 关联告警 |
| channel | varchar(32) | not null | `EMAIL` / `TELEGRAM` / `WECHAT` |
| receiver | varchar(255) | not null | 接收人 |
| send_status | varchar(32) | not null | `SUCCESS` / `FAILED` |
| response_payload | jsonb |  | 发送响应 |
| sent_at | timestamptz | not null | 发送时间 |
| created_at | timestamptz | not null | 创建时间 |

## 6.31 `intel.sources`

用途：

- 实时情报源配置表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| source_code | varchar(64) | UK, not null | 源编码 |
| source_name | varchar(128) | not null | 源名称 |
| source_type | varchar(32) | not null | `ANNOUNCEMENT` / `NEWS` / `SOCIAL` / `MACRO` |
| channel_type | varchar(32) | not null | `REST` / `RSS` / `WS` / `SCRAPER` |
| base_url | varchar(255) |  | 数据源地址 |
| language | varchar(16) |  | 主要语言 |
| poll_interval_seconds | int |  | 轮询间隔 |
| auth_config | jsonb |  | 鉴权配置 |
| source_config | jsonb |  | 其他配置 |
| status | varchar(32) | not null | `ACTIVE` / `INACTIVE` |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

## 6.32 `intel.documents`

用途：

- 采集到的原始新闻、公告、帖子文档表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| source_id | bigint | FK, not null | 来源 ID |
| document_uid | varchar(128) | UK, not null | 外部文档唯一标识 |
| source_type | varchar(32) | not null | 文档来源类型 |
| title | varchar(512) |  | 标题 |
| content_text | text |  | 清洗后的正文 |
| content_hash | varchar(64) | not null | 内容哈希，用于去重 |
| url | varchar(1024) |  | 原文链接 |
| author | varchar(255) |  | 作者 |
| language | varchar(16) |  | 语言 |
| published_at | timestamptz |  | 原始发布时间 |
| fetched_at | timestamptz | not null | 系统采集时间 |
| related_symbols | jsonb |  | 关联标的列表 |
| event_tags | jsonb |  | 初步事件标签 |
| raw_payload | jsonb |  | 原始内容 |
| process_status | varchar(32) | not null | `NEW` / `PARSED` / `SCORED` / `FAILED` |
| created_at | timestamptz | not null | 创建时间 |
| updated_at | timestamptz | not null | 更新时间 |

索引建议：

- unique on `document_uid`
- index on `(source_id, fetched_at desc)`
- index on `(published_at desc)`
- index on `content_hash`

## 6.33 `intel.sentiment_scores`

用途：

- 文档级或标的级情绪评分结果表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| document_id | bigint | FK, not null | 文档 ID |
| instrument_id | bigint | FK | 关联标的 |
| model_name | varchar(128) | not null | 模型名称 |
| sentiment_label | varchar(32) | not null | `BULLISH` / `BEARISH` / `NEUTRAL` |
| sentiment_score | numeric(16, 8) | not null | 情绪分数 |
| relevance_score | numeric(16, 8) |  | 相关性分数 |
| heat_score | numeric(16, 8) |  | 热度分数 |
| confidence_score | numeric(16, 8) |  | 置信度 |
| horizon | varchar(32) |  | 适用时间窗口 |
| topics | jsonb |  | 主题标签 |
| entities | jsonb |  | 实体标签 |
| explanation | text |  | 简要解释 |
| created_at | timestamptz | not null | 创建时间 |

索引建议：

- index on `(instrument_id, created_at desc)`
- index on `(sentiment_label, created_at desc)`

## 6.34 `intel.directional_signals`

用途：

- 标的级方向偏置信号表

字段：

| 字段 | 类型 | 约束 | 说明 |
| --- | --- | --- | --- |
| id | bigserial | PK | 主键 |
| signal_id | varchar(64) | UK, not null | 信号唯一编号 |
| instrument_id | bigint | FK, not null | 标的 ID |
| signal_time | timestamptz | not null | 信号时间 |
| horizon | varchar(32) | not null | `5M` / `30M` / `4H` / `1D` |
| direction_label | varchar(32) | not null | `BULLISH` / `BEARISH` / `NEUTRAL` |
| up_probability | numeric(16, 8) | not null | 上涨概率 |
| down_probability | numeric(16, 8) | not null | 下跌概率 |
| neutral_probability | numeric(16, 8) | not null | 中性概率 |
| confidence_score | numeric(16, 8) | not null | 置信度 |
| sentiment_score | numeric(16, 8) |  | 聚合情绪分数 |
| market_confirmation_score | numeric(16, 8) |  | 市场确认分数 |
| supporting_features | jsonb |  | 支撑特征 |
| model_name | varchar(128) | not null | 模型名称 |
| valid_until | timestamptz |  | 失效时间 |
| created_at | timestamptz | not null | 创建时间 |

索引建议：

- unique on `signal_id`
- index on `(instrument_id, signal_time desc, horizon)`
- index on `(direction_label, signal_time desc)`

## 7. 表关系概览

关键关系如下：

- `ref.exchanges` 1:N `ref.instruments`
- `ref.exchanges` 1:N `acct.trading_accounts`
- `acct.trading_accounts` 1:N `acct.account_api_credentials`
- `strat.strategies` 1:N `strat.strategy_versions`
- `strat.strategy_versions` 1:N `strat.strategy_parameters`
- `strat.strategies` 1:N `strat.strategy_deployments`
- `strat.strategy_deployments` 1:N `strat.strategy_runs`
- `acct.trading_accounts` 1:N `trade.orders`
- `trade.orders` 1:N `trade.order_events`
- `trade.orders` 1:N `trade.executions`
- `acct.trading_accounts` 1:N `trade.positions`
- `risk.risk_rules` 1:N `risk.risk_rule_bindings`
- `risk.risk_rules` 1:N `risk.risk_events`
- `intel.sources` 1:N `intel.documents`
- `intel.documents` 1:N `intel.sentiment_scores`
- `ref.instruments` 1:N `intel.sentiment_scores`
- `ref.instruments` 1:N `intel.directional_signals`

## 8. MVP 必须落地的表

以下表建议纳入 MVP 首批 DDL：

- `sys.users`
- `sys.roles`
- `sys.user_roles`
- `sys.audit_logs`
- `ref.exchanges`
- `ref.instruments`
- `acct.trading_accounts`
- `acct.account_api_credentials`
- `acct.account_snapshots`
- `strat.strategies`
- `strat.strategy_versions`
- `strat.strategy_parameters`
- `strat.strategy_deployments`
- `strat.strategy_runs`
- `mkt.market_klines`
- `trade.orders`
- `trade.order_events`
- `trade.executions`
- `trade.positions`
- `trade.position_snapshots`
- `risk.risk_rules`
- `risk.risk_rule_bindings`
- `risk.risk_events`
- `risk.kill_switch_events`
- `rpt.daily_account_metrics`
- `rpt.daily_strategy_metrics`
- `ops.scheduled_jobs`
- `ops.job_runs`
- `ops.alerts`
- `ops.notification_logs`
- `intel.sources`
- `intel.documents`
- `intel.sentiment_scores`
- `intel.directional_signals`

如果 MVP 首个落地市场选择 A 股、港股或美股，则建议将以下两类表前置到首批 DDL：

- `ref.trading_calendars`
- `ref.corporate_actions`

## 9. 二阶段可扩展表

以下表可在系统稳定后增加：

- `mkt.market_trades`
- `mkt.orderbook_snapshots`
- `mkt.funding_rates`
- `mkt.settlement_prices`
- `mkt.continuous_contract_mappings`
- `ref.trading_calendars`
- `ref.corporate_actions`
- `mkt.market_status_events`
- `intel.document_entities`
- `intel.topic_trends`
- `trade.position_lots`
- `rpt.backtest_equity_curves`
- `rpt.backtest_trade_metrics`
- `ops.incident_tickets`

## 10. 索引与性能建议

### 10.1 高频查询路径

重点优化以下查询：

- 某账户的最近订单
- 某策略的最近成交流水
- 某标的某周期的历史 K 线
- 某账户的最新持仓和权益
- 某策略的日报和累计绩效

### 10.2 索引原则

- 所有外键字段建立索引
- 时间序列表优先复合索引：业务维度 + 时间倒序
- 状态查询字段建立组合索引，如 `(account_id, status)`
- 大表注意分区或 hypertable 策略

### 10.3 分区建议

- `mkt.market_klines` 按时间分区
- `acct.account_snapshots` 按时间分区
- `trade.position_snapshots` 按时间分区
- `trade.executions` 可按月分区

## 11. 数据治理建议

### 11.1 审计与追溯

- 配置变更写审计日志
- 风控事件不可物理删除
- 订单和成交流水不可更新为历史失真状态

### 11.2 清理策略

- 订单和成交流水长期保留
- 行情原始明细可按冷热分层归档
- 告警和任务日志可设置 180 到 365 天保留策略

### 11.3 对账原则

- 订单对账以交易所为事实源
- 策略报表以成交和快照为事实源
- Redis 状态丢失后必须从数据库恢复

## 12. 结论

本数据库设计优先服务 MVP 闭环：

- 能支撑账户、策略、订单、风控、报表完整闭环
- 能满足 K 线级回测和实盘交易
- 保留未来扩展到 Tick 数据、多账户、多策略的空间

后续建议继续补充：

- SQL DDL 脚本
- 索引创建脚本
- 初始化数据字典
- 数据迁移规范
