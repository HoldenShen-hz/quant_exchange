# 量化交易平台增强版 API 接口定义文档

> 文档层级：平台增强设计层
>  
> 推荐读者：后端工程师、前端工程师、集成工程师、测试工程师
>  
> 建议前置阅读：[系统架构设计](./quant_trading_system_architecture_design.md) / [增强版 Schema](./quant_trading_enhanced_database_schema_design.md)
>  
> 相关文档：[基础数据库设计](./quant_trading_database_schema_design.md) / [编码前准备](./quant_trading_pre_implementation_readiness_plan.md)

## 1. 文档目标

本文档定义 `quant_exchange` 的增强版 API 接口，覆盖从研究、实验、特征、执行、路由、审计到回放的完整控制面接口。

本文档承接以下设计：

- [quant_trading_system_architecture_design.md](./quant_trading_system_architecture_design.md)
- [quant_trading_enhanced_database_schema_design.md](./quant_trading_enhanced_database_schema_design.md)
- [quant_trading_competitive_analysis_and_enhanced_requirements.md](./quant_trading_competitive_analysis_and_enhanced_requirements.md)

## 2. API 设计原则

- 统一 REST 风格，必要时补充 WebSocket / SSE
- 所有关键接口支持审计和幂等
- 查询与控制分离
- 长任务采用异步 Job 模式
- 研究、实验、回测、训练等高耗时任务使用 `job_id`
- 所有资源支持分页、过滤、排序

## 3. 通用约定

### 3.1 基础路径

- 管理接口前缀：`/api/v1`
- 内部服务接口前缀：`/internal/v1`

### 3.2 通用请求头

- `Authorization: Bearer <token>`
- `X-Request-Id`
- `X-Idempotency-Key`

### 3.3 通用响应结构

```json
{
  "code": "OK",
  "message": "success",
  "request_id": "req_123",
  "data": {},
  "meta": {}
}
```

### 3.4 错误码建议

- `AUTH_UNAUTHORIZED`
- `AUTH_FORBIDDEN`
- `VALIDATION_ERROR`
- `RESOURCE_NOT_FOUND`
- `CONFLICT_DUPLICATE`
- `RISK_BLOCKED`
- `ORDER_REJECTED`
- `JOB_RUNNING`
- `JOB_FAILED`
- `MODEL_NOT_READY`

## 4. 认证与权限接口

### 4.1 登录

- `POST /api/v1/auth/login`

请求体：

```json
{
  "username": "alice",
  "password": "******"
}
```

响应：

```json
{
  "code": "OK",
  "data": {
    "access_token": "jwt",
    "expires_in": 7200,
    "user": {
      "id": 1,
      "username": "alice",
      "roles": ["ADMIN"]
    }
  }
}
```

### 4.2 当前用户信息

- `GET /api/v1/auth/me`

### 4.3 用户列表

- `GET /api/v1/users`

### 4.4 角色授权

- `POST /api/v1/users/{user_id}/roles`

## 5. 参考数据与账户接口

### 5.1 交易所列表

- `GET /api/v1/exchanges`

### 5.2 标的查询

- `GET /api/v1/instruments`

过滤参数建议：

- `exchange_code`
- `market_type`
- `market_region`
- `instrument_type`
- `symbol`
- `status`

### 5.2.1 交易日历查询

- `GET /api/v1/trading-calendars`

过滤参数建议：

- `exchange_code`
- `market_region`
- `date_from`
- `date_to`

### 5.2.2 公司行为查询

- `GET /api/v1/corporate-actions`

过滤参数建议：

- `exchange_code`
- `market_region`
- `symbol`
- `action_type`
- `effective_date_from`
- `effective_date_to`

### 5.3 账户列表

- `GET /api/v1/accounts`

### 5.4 创建账户

- `POST /api/v1/accounts`

### 5.5 账户快照

- `GET /api/v1/accounts/{account_id}/snapshots`

### 5.6 当前持仓

- `GET /api/v1/accounts/{account_id}/positions`

## 6. 市场数据接口

### 6.1 K 线查询

- `GET /api/v1/market/klines`

查询参数：

- `instrument_id`
- `interval`
- `start_time`
- `end_time`
- `limit`

### 6.2 行情订阅

- `GET /api/v1/ws/market`

WebSocket 事件：

- `market.kline.closed`
- `market.trade.tick`
- `market.orderbook.updated`

### 6.3 历史数据同步任务

- `POST /api/v1/market/jobs/sync-klines`

### 6.4 标的同步任务

- `POST /api/v1/market/jobs/sync-instruments`

## 7. 实时情报与情绪接口

### 7.1 情报源列表

- `GET /api/v1/intel/sources`

### 7.2 创建情报源

- `POST /api/v1/intel/sources`

### 7.3 文档查询

- `GET /api/v1/intel/documents`

过滤参数：

- `source_type`
- `instrument_id`
- `published_from`
- `published_to`
- `event_tag`

### 7.4 文档详情

- `GET /api/v1/intel/documents/{document_id}`

### 7.5 情绪评分查询

- `GET /api/v1/intel/sentiment-scores`

### 7.6 方向偏置信号查询

- `GET /api/v1/intel/directional-signals`

过滤参数：

- `instrument_id`
- `horizon`
- `from_time`
- `to_time`

### 7.7 情报流订阅

- `GET /api/v1/ws/intel`

事件：

- `intel.document.ingested`
- `intel.sentiment.updated`
- `intel.directional_bias.updated`

## 8. Universe / Screener 接口

### 8.1 Universe 列表

- `GET /api/v1/universes`

### 8.2 创建 Universe

- `POST /api/v1/universes`

请求体：

```json
{
  "universe_code": "top_liq_sentiment",
  "universe_name": "High Liquidity Sentiment Universe",
  "asset_class": "PERP",
  "scope_type": "DYNAMIC"
}
```

### 8.3 新增筛选规则

- `POST /api/v1/universes/{universe_id}/rules`

### 8.4 查询 Universe 快照

- `GET /api/v1/universes/{universe_id}/snapshots`

### 8.5 触发 Universe 重算

- `POST /api/v1/universes/{universe_id}/jobs/rebuild`

## 9. Feature Store 接口

### 9.1 特征定义列表

- `GET /api/v1/features`

### 9.2 创建特征定义

- `POST /api/v1/features`

### 9.3 特征版本列表

- `GET /api/v1/features/{feature_id}/versions`

### 9.4 发布特征版本

- `POST /api/v1/features/{feature_id}/versions`

### 9.5 查询特征值

- `GET /api/v1/features/values`

查询参数：

- `feature_code`
- `instrument_id`
- `start_time`
- `end_time`

### 9.6 触发特征重算

- `POST /api/v1/features/{feature_id}/jobs/recompute`

## 10. Research Lab 接口

### 10.1 研究项目列表

- `GET /api/v1/research/projects`

### 10.2 创建研究项目

- `POST /api/v1/research/projects`

### 10.3 Notebook 列表

- `GET /api/v1/research/projects/{project_id}/notebooks`

### 10.4 注册 Notebook

- `POST /api/v1/research/projects/{project_id}/notebooks`

### 10.5 数据集注册

- `POST /api/v1/research/datasets`

## 11. 策略接口

### 11.1 策略列表

- `GET /api/v1/strategies`

### 11.2 创建策略

- `POST /api/v1/strategies`

### 11.3 策略版本列表

- `GET /api/v1/strategies/{strategy_id}/versions`

### 11.4 发布策略版本

- `POST /api/v1/strategies/{strategy_id}/versions`

### 11.5 参数集列表

- `GET /api/v1/strategies/{strategy_id}/parameters`

### 11.6 创建参数集

- `POST /api/v1/strategies/{strategy_id}/parameters`

### 11.7 策略部署

- `POST /api/v1/strategy-deployments`

### 11.8 启停策略

- `POST /api/v1/strategy-deployments/{deployment_id}/start`
- `POST /api/v1/strategy-deployments/{deployment_id}/stop`

## 12. 回测与优化接口

### 12.1 发起回测

- `POST /api/v1/backtests`

请求体建议：

```json
{
  "strategy_id": 1001,
  "strategy_version_id": 2001,
  "parameter_id": 3001,
  "account_id": 4001,
  "run_config": {
    "start_time": "2025-01-01T00:00:00Z",
    "end_time": "2025-03-01T00:00:00Z",
    "interval": "1m"
  }
}
```

### 12.2 发起参数优化

- `POST /api/v1/backtests/optimize`

### 12.3 查询回测任务

- `GET /api/v1/backtests/{run_id}`

### 12.4 查询回测结果

- `GET /api/v1/backtests/{run_id}/report`

### 12.5 查询回测订单与成交

- `GET /api/v1/backtests/{run_id}/orders`
- `GET /api/v1/backtests/{run_id}/executions`

## 13. 实验与模型接口

### 13.1 实验列表

- `GET /api/v1/experiments`

### 13.2 创建实验

- `POST /api/v1/experiments`

### 13.3 实验运行列表

- `GET /api/v1/experiments/{experiment_id}/runs`

### 13.4 提交训练任务

- `POST /api/v1/experiments/{experiment_id}/jobs/train`

### 13.5 模型注册表

- `GET /api/v1/models`
- `POST /api/v1/models`

### 13.6 模型版本

- `GET /api/v1/models/{model_id}/versions`
- `POST /api/v1/models/{model_id}/versions`

### 13.7 模型部署

- `POST /api/v1/model-deployments`

### 13.8 模型漂移监控

- `GET /api/v1/models/{model_id}/drift-metrics`

## 14. Bias Audit 接口

### 14.1 创建审计任务

- `POST /api/v1/audit/jobs`

请求体：

```json
{
  "audit_type": "LOOKAHEAD",
  "target_type": "STRATEGY",
  "target_ref": "strategy:1001:version:2001"
}
```

### 14.2 审计任务列表

- `GET /api/v1/audit/jobs`

### 14.3 审计结果查询

- `GET /api/v1/audit/jobs/{audit_job_id}/results`

## 15. 风控接口

### 15.1 风控规则列表

- `GET /api/v1/risk/rules`

### 15.2 创建风控规则

- `POST /api/v1/risk/rules`

### 15.3 绑定风控规则

- `POST /api/v1/risk/rules/{rule_id}/bindings`

### 15.4 风控事件查询

- `GET /api/v1/risk/events`

### 15.5 Kill Switch

- `POST /api/v1/risk/kill-switch`
- `POST /api/v1/risk/kill-switch/release`

## 16. OMS / EMS / Execution 接口

### 16.1 订单列表

- `GET /api/v1/orders`

### 16.2 手动下单

- `POST /api/v1/orders`

### 16.3 订单详情

- `GET /api/v1/orders/{order_id}`

### 16.4 撤单

- `POST /api/v1/orders/{order_id}/cancel`

### 16.5 批量母单

- `POST /api/v1/ems/order-baskets`

### 16.6 查询母单状态

- `GET /api/v1/ems/order-baskets/{basket_id}`

### 16.7 执行算法列表

- `GET /api/v1/ems/execution-algorithms`

### 16.8 路由决策查询

- `GET /api/v1/ems/router-decisions`

## 17. Smart Router / Venue 接口

### 17.1 Venue 最优报价查询

- `GET /api/v1/router/best-quotes`

### 17.2 路由模拟

- `POST /api/v1/router/simulate`

### 17.3 路由策略列表

- `GET /api/v1/router/policies`

### 17.4 路由策略配置

- `POST /api/v1/router/policies`

## 18. Strategy Controller / Executor 接口

### 18.1 Controller 列表

- `GET /api/v1/controllers`

### 18.2 创建 Controller

- `POST /api/v1/controllers`

### 18.3 Executor 列表

- `GET /api/v1/executors`

### 18.4 创建 Executor

- `POST /api/v1/executors`

### 18.5 绑定 Controller 与 Executor

- `POST /api/v1/controllers/{controller_id}/executors`

## 19. Ledger / Subaccount 接口

### 19.1 虚拟账户列表

- `GET /api/v1/virtual-accounts`

### 19.2 创建虚拟账户

- `POST /api/v1/virtual-accounts`

### 19.3 账本分录查询

- `GET /api/v1/ledger/entries`

### 19.4 内部资金划拨

- `POST /api/v1/ledger/transfers`

## 20. Alternative Data 接口

### 20.1 另类数据源列表

- `GET /api/v1/alt/sources`

### 20.2 注册另类数据源

- `POST /api/v1/alt/sources`

### 20.3 注册另类数据集

- `POST /api/v1/alt/datasets`

### 20.4 另类数据查询

- `GET /api/v1/alt/datasets/{dataset_id}/records`

## 21. Replay / Snapshot / Shadow 接口

### 21.1 事件回放任务

- `POST /api/v1/replay/jobs`

### 21.2 查询回放结果

- `GET /api/v1/replay/jobs/{job_id}`

### 21.3 状态快照列表

- `GET /api/v1/replay/state-snapshots`

### 21.4 创建 Shadow Deployment

- `POST /api/v1/replay/shadow-deployments`

### 21.5 Shadow 结果查询

- `GET /api/v1/replay/shadow-deployments/{deployment_id}`

## 22. Options / MM / DEX 接口

### 22.1 期权链查询

- `GET /api/v1/options/chains`

### 22.2 Greeks 查询

- `GET /api/v1/options/greeks`

### 22.3 做市配置列表

- `GET /api/v1/market-making/configs`

### 22.4 创建做市配置

- `POST /api/v1/market-making/configs`

### 22.5 DEX LP 仓位查询

- `GET /api/v1/dex/liquidity-positions`

## 23. 报表与监控接口

### 23.1 账户日报

- `GET /api/v1/reports/accounts/daily`

### 23.2 策略日报

- `GET /api/v1/reports/strategies/daily`

### 23.3 情绪日报

- `GET /api/v1/reports/intel/daily`

### 23.4 回测偏差报告

- `GET /api/v1/reports/backtest-bias`

### 23.5 模型健康报告

- `GET /api/v1/reports/models/health`

### 23.6 告警列表

- `GET /api/v1/alerts`

## 24. WebSocket / SSE 增强接口

推荐提供以下实时流：

- `GET /api/v1/ws/orders`
- `GET /api/v1/ws/risk`
- `GET /api/v1/ws/intel`
- `GET /api/v1/ws/strategies`
- `GET /api/v1/ws/alerts`

事件类型建议：

- `order.updated`
- `trade.executed`
- `position.updated`
- `risk.event.triggered`
- `intel.directional_bias.updated`
- `strategy.run.updated`
- `alert.triggered`

## 25. 内部服务接口建议

以下接口建议仅供内部服务调用：

- `POST /internal/v1/execution/route-order`
- `POST /internal/v1/risk/pre-check`
- `POST /internal/v1/features/materialize`
- `POST /internal/v1/models/predict`
- `POST /internal/v1/intel/classify-document`
- `POST /internal/v1/replay/append-event`

## 26. 长任务 Job 接口规范

对回测、训练、审计、回放、特征重算等任务，建议统一：

- 创建任务：`POST /api/v1/jobs/<type>`
- 查询任务：`GET /api/v1/jobs/{job_id}`
- 取消任务：`POST /api/v1/jobs/{job_id}/cancel`

响应体建议：

```json
{
  "code": "OK",
  "data": {
    "job_id": "job_123",
    "job_type": "BACKTEST",
    "status": "RUNNING"
  }
}
```

## 27. 审计与幂等要求

以下接口必须要求 `X-Idempotency-Key`：

- 下单
- 撤单
- 发起回测
- 发起训练
- 创建部署
- Kill Switch
- 内部资金划拨

以下接口必须写审计日志：

- 风控规则变更
- 模型部署
- 策略部署
- 手动下单和撤单
- 子账户划拨
- Shadow / Canary 发布

## 28. 版本演进建议

### 第一批先实现

- 现有 MVP 接口
- 情报与情绪接口
- Universe 接口
- Feature 接口
- 实验与模型注册接口
- Bias Audit 接口

### 第二批实现

- EMS / Router 接口
- Controller / Executor 接口
- Replay / Shadow 接口
- Ledger 接口

### 第三批实现

- Alternative Data 深度接口
- Auto-Retrain 与在线 Serving 接口

## 29. 结论

这份增强版 API 文档定义的不是一个简单“交易 bot API”，而是一个完整量化交易平台的控制面接口。

如果继续推进，下一步建议直接产出：

- OpenAPI 草案
- Pydantic 请求响应模型
- 路由模块目录设计
- 增强版权限矩阵文档
