# `quant_exchange` 文档总导航

本目录包含量化交易平台 `quant_exchange` 的完整设计文档集。

为了让整套文档更易读、可检索、可实施，建议按“分层阅读”和“按角色阅读”两种方式使用。

## 1. 文档分层

### 1.1 产品与范围层

用于回答“平台做什么、边界是什么、阶段目标是什么”。

- [quant_trading_requirements_analysis.md](./quant_trading_requirements_analysis.md) **← 主需求基线，已融合竞品增强需求，并按“范围 / 编号 / 优先级 / 验收”重构**
- [quant_trading_detailed_function_spec.md](./quant_trading_detailed_function_spec.md) **← 已按新需求编号体系重构**
- [quant_trading_mvp_development_plan.md](./quant_trading_mvp_development_plan.md)

### 1.2 市场与策略层

用于回答“支持哪些市场、用哪些研究方法、因子和交易框架怎么组织”。

- [quant_trading_futures_and_crypto_support_design.md](./quant_trading_futures_and_crypto_support_design.md)
- [quant_trading_stock_market_support_design.md](./quant_trading_stock_market_support_design.md)
- [quant_trading_stock_screener_web_workbench_design.md](./quant_trading_stock_screener_web_workbench_design.md)
- [quant_trading_factor_library_and_implementation_guide.md](./quant_trading_factor_library_and_implementation_guide.md)
- [quant_trading_portfolio_risk_and_trading_methods_framework.md](./quant_trading_portfolio_risk_and_trading_methods_framework.md)
- [quant_trading_market_intelligence_sentiment_tool_design.md](./quant_trading_market_intelligence_sentiment_tool_design.md)

### 1.3 平台设计层

用于回答“系统如何拆分、数据库如何设计、接口如何定义”。

- [quant_trading_system_architecture_design.md](./quant_trading_system_architecture_design.md)
- [quant_trading_polyglot_technology_stack_design.md](./quant_trading_polyglot_technology_stack_design.md)
- [quant_trading_database_schema_design.md](./quant_trading_database_schema_design.md)
- [quant_trading_enhanced_database_schema_design.md](./quant_trading_enhanced_database_schema_design.md)
- [quant_trading_enhanced_api_interface_definition.md](./quant_trading_enhanced_api_interface_definition.md)

### 1.4 规划与增强层

用于回答”对标世界级平台后还缺什么、如何演进”。

- [quant_trading_competitive_analysis_and_enhanced_requirements.md](./quant_trading_competitive_analysis_and_enhanced_requirements.md) **← 新增：竞品分析与增强需求**
- [quant_trading_pre_implementation_readiness_plan.md](./quant_trading_pre_implementation_readiness_plan.md)
- [quant_trading_test_plan_and_acceptance_cases.md](./quant_trading_test_plan_and_acceptance_cases.md) **← 已同步为完整需求-测试追踪矩阵**
- [quant_trading_implementation_coverage_audit.md](./quant_trading_implementation_coverage_audit.md)

### 1.5 公共基础层

用于统一术语和公共认知。

- [quant_trading_bilingual_glossary.md](./quant_trading_bilingual_glossary.md)

## 2. 推荐阅读顺序

### 2.1 第一次整体阅读

建议顺序：

1. [quant_trading_requirements_analysis.md](./quant_trading_requirements_analysis.md)
2. [quant_trading_detailed_function_spec.md](./quant_trading_detailed_function_spec.md)
3. [quant_trading_system_architecture_design.md](./quant_trading_system_architecture_design.md)
4. [quant_trading_database_schema_design.md](./quant_trading_database_schema_design.md)
5. [quant_trading_mvp_development_plan.md](./quant_trading_mvp_development_plan.md)
6. [quant_trading_pre_implementation_readiness_plan.md](./quant_trading_pre_implementation_readiness_plan.md)

### 2.2 量化研究阅读路径

建议顺序：

1. [quant_trading_requirements_analysis.md](./quant_trading_requirements_analysis.md)
2. [quant_trading_factor_library_and_implementation_guide.md](./quant_trading_factor_library_and_implementation_guide.md)
3. [quant_trading_portfolio_risk_and_trading_methods_framework.md](./quant_trading_portfolio_risk_and_trading_methods_framework.md)
4. [quant_trading_market_intelligence_sentiment_tool_design.md](./quant_trading_market_intelligence_sentiment_tool_design.md)
5. [quant_trading_futures_and_crypto_support_design.md](./quant_trading_futures_and_crypto_support_design.md)
6. [quant_trading_stock_market_support_design.md](./quant_trading_stock_market_support_design.md)
7. [quant_trading_stock_screener_web_workbench_design.md](./quant_trading_stock_screener_web_workbench_design.md)

### 2.3 后端与架构阅读路径

建议顺序：

1. [quant_trading_system_architecture_design.md](./quant_trading_system_architecture_design.md)
2. [quant_trading_polyglot_technology_stack_design.md](./quant_trading_polyglot_technology_stack_design.md)
3. [quant_trading_database_schema_design.md](./quant_trading_database_schema_design.md)
4. [quant_trading_enhanced_database_schema_design.md](./quant_trading_enhanced_database_schema_design.md)
5. [quant_trading_enhanced_api_interface_definition.md](./quant_trading_enhanced_api_interface_definition.md)
6. [quant_trading_pre_implementation_readiness_plan.md](./quant_trading_pre_implementation_readiness_plan.md)
7. [quant_trading_test_plan_and_acceptance_cases.md](./quant_trading_test_plan_and_acceptance_cases.md)

### 2.4 项目管理与落地阅读路径

建议顺序：

1. [quant_trading_requirements_analysis.md](./quant_trading_requirements_analysis.md)
2. [quant_trading_competitive_analysis_and_enhanced_requirements.md](./quant_trading_competitive_analysis_and_enhanced_requirements.md) **← 新增**
3. [quant_trading_mvp_development_plan.md](./quant_trading_mvp_development_plan.md)
4. [quant_trading_pre_implementation_readiness_plan.md](./quant_trading_pre_implementation_readiness_plan.md)
5. [quant_trading_test_plan_and_acceptance_cases.md](./quant_trading_test_plan_and_acceptance_cases.md)
6. [quant_trading_implementation_coverage_audit.md](./quant_trading_implementation_coverage_audit.md)

## 3. 按主题查找

| 主题 | 首选文档 |
| --- | --- |
| 产品目标、范围、优先级与总体验收 | `quant_trading_requirements_analysis.md` |
| 功能定义、模块拆分与交付物 | `quant_trading_detailed_function_spec.md` |
| 系统架构 | `quant_trading_system_architecture_design.md` |
| 多语言技术栈与模块语言分工 | `quant_trading_polyglot_technology_stack_design.md` |
| 核心数据库设计 | `quant_trading_database_schema_design.md` |
| 增强版数据库与平台能力 | `quant_trading_enhanced_database_schema_design.md` |
| API 设计 | `quant_trading_enhanced_api_interface_definition.md` |
| MVP 排期 | `quant_trading_mvp_development_plan.md` |
| 编码前检查 | `quant_trading_pre_implementation_readiness_plan.md` |
| 测试方案、验收用例与需求-测试映射 | `quant_trading_test_plan_and_acceptance_cases.md` |
| 代码实现覆盖审计 | `quant_trading_implementation_coverage_audit.md` |
| 因子体系 | `quant_trading_factor_library_and_implementation_guide.md` |
| 组合、风控、交易方法 | `quant_trading_portfolio_risk_and_trading_methods_framework.md` |
| 情绪与情报 | `quant_trading_market_intelligence_sentiment_tool_design.md` |
| 加密货币与期货市场支持 | `quant_trading_futures_and_crypto_support_design.md` |
| 股票市场支持 | `quant_trading_stock_market_support_design.md` |
| 股票筛选网页工作台 | `quant_trading_stock_screener_web_workbench_design.md` |
| 竞品分析与增强需求 | `quant_trading_competitive_analysis_and_enhanced_requirements.md` |
| 术语表 | `quant_trading_bilingual_glossary.md` |
| 文档整理与实现状态 | `README.md` / `quant_trading_implementation_coverage_audit.md` |

## 4. 当前文档状态判断

当前文档集已经具备：

- 平台蓝图与产品定位
- 主需求基线与详细规格（15 个核心模块 + 16 个增强模块）
- **竞品分析与增强需求（18 个竞品，16 个增强模块）← 新增**
- 市场覆盖框架（股票/期货/加密/外汇/期权）
- 因子与策略框架
- 数据与接口主设计
- 网页工作台与终端设计主线

当前仍待继续补齐的，主要是实施级与运维级文档：

- 可执行 DDL
- OpenAPI / Pydantic
- 权限矩阵与审批流
- 风控规则手册
- adapter 规范
- 订单状态机
- Runbook
- 发布与回滚策略
- 灾备与连续性方案
- 社交交易社区详细设计
- AI/LLM 集成技术方案
- 高级图表引擎技术选型
- 预设机器人详细设计
- Webhook 与工作流引擎设计

## 5. 本次整理删除的冗余文档

以下历史文档的内容已经被主线文档吸收，本次整理后已删除：

- `quant_trading_open_source_benchmark_and_full_feature_roadmap.md`
  其开源对标与增强路线已并入 [quant_trading_competitive_analysis_and_enhanced_requirements.md](./quant_trading_competitive_analysis_and_enhanced_requirements.md) 和 [quant_trading_requirements_analysis.md](./quant_trading_requirements_analysis.md)
- `quant_trading_ui_enhancement_requirements.md`
  其界面增强要求已并入 [quant_trading_stock_screener_web_workbench_design.md](./quant_trading_stock_screener_web_workbench_design.md) 和 [quant_trading_competitive_analysis_and_enhanced_requirements.md](./quant_trading_competitive_analysis_and_enhanced_requirements.md)
- `quant_trading_full_documentation_completeness_review_and_final_supplement.md`
  其文档治理与缺口判断已并入 [README.md](./README.md)、[quant_trading_pre_implementation_readiness_plan.md](./quant_trading_pre_implementation_readiness_plan.md) 和 [quant_trading_implementation_coverage_audit.md](./quant_trading_implementation_coverage_audit.md)

## 6. 使用建议

- 讨论产品边界时，优先看需求、详细规格、竞品增强和编码前准备文档。
- 做量化研究时，优先看因子、交易方法、情绪工具和市场支持文档。
- 做实现时，优先看架构、数据库、API、编码前准备文档。
- 做管理和排期时，优先看 MVP 排期、实施前检查、测试映射和实现覆盖审计文档。

这份 `README` 的目标不是替代原始文档，而是让整套文档体系更容易进入、更容易导航、更容易按角色使用。
