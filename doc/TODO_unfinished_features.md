# TODO: 未完成功能清单

> 基于 doc/ 全部需求文档与代码库的逐项审计，截至 2026-03-20
> 整体完成度约 87%（本次更新：MD-08全市场快照、RP-03/PP-06漂移分析、RK-06黑天鹅保护、PF-05归因增强、FT-07跨期套利、BOT-02/03机器人控制台）

---

## 一、核心交易链路（P0）— 完成度 ~82%

### 1.1 已完成
- [x] MD-01~MD-07: 多源行情接入、历史数据、统一模型、查询API
- [x] ST-01~ST-07: 策略基类、参数配置、版本管理、实验记录
- [x] BT-01~BT-07: 事件驱动回测、日/分钟级、手续费滑点、订单模拟、资金管理、结果导出
- [x] PP-01~PP-05, PP-07: 模拟账户、实时模拟、状态机、数据持久化、Web交互、目标vs实际
- [x] EX-01~EX-05: 多交易所适配、基础订单类型、状态同步、幂等重试、盘前风控
- [x] RK-01~RK-05, RK-07: 账户/策略/标的/订单级风控、熔断、审计
- [x] PF-01~PF-04: 资产汇总、再平衡、多策略预算、风险聚合
- [x] MO-01~MO-03: 服务监控、账户KPI、四级告警
- [x] RP-01~RP-02, RP-05: 报告类型、核心指标、日报
- [x] SE-01~SE-04: 认证登录、基础RBAC、审计日志（部分）、多用户隔离
- [x] IN-01~IN-06: 多源情报、文本处理、情感评分、市场融合、方向偏差、信号归档

### 1.2 未完成 / 部分完成

| 编号 | 功能 | 状态 | 缺失细节 |
|------|------|------|----------|
| MD-05 | 数据分层（raw/标准/特征层） | PARTIAL | 特征层未实现 |
| MD-08 | 全市场快照 | YES | market_breadth(跨市场涨跌家数/A/D比/情绪指数)、advance/decline/unchanged统计、per-market sentiment、global_sentiment_index |
| MD-10 | 参考数据（交易日历、公司行为） | YES | CorporateAction模型已实现(分红/拆股/权利发行/并购)，支持前复权/后复权因子计算 |
| ST-05 | 因子库 | PARTIAL | 仅基础因子(SMA/EMA/RSI/MACD)，缺ML因子、截面因子 |
| ST-08 | DSL/可视化/自然语言策略 | NO | 未实现 |
| BT-06 | 批量/滚动回测 | YES | WalkForwardResult实现，真正walk-forward优化(train→参数优化→test)，滚动窗口聚合收益曲线，walk-forward效率比，purge跨验，支持参数稳定性分析 |
| BT-08 | 偏差审计(前视偏差检测) | PARTIAL | BiasAuditService已集成进BacktestEngine，回测结果包含审计报告 |
| PP-06 | 回测-实盘漂移分析 | YES | drift_score(复合漂移分)、slippage_analysis(逐笔滑点分析)、signal_divergence(信号方向偏离检测)、drift_recommendations(可操作建议) |
| EX-06 | 三级权限审批 | YES | ApprovalService实现，L1(操作员)/L2(风控)/L3(合规+风控)三级审批流，支持approve/reject/cancel/expiry，审计日志完整 |
| EX-07 | 异常处理(全链路) | PARTIAL | 部分场景缺失 |
| EX-08 | 高级OMS/EMS | NO | 未实现 |
| RK-06 | 黑天鹅保护 | YES | Cornish-Fisher VaR(偏度/峰度调整)、Expected Shortfall(CVaR)、check_circuit_breakers(L1/L2/L3熔断)、detect_correlation_spike(滚动相关阵)、calculate_conditional_drawdown_risk(CDaR) |
| PF-05 | 归因分析 | YES | volatility_attribution(边际波动贡献分解)、drawdown_attribution(回撤期持仓归因)、sector_brinson_attribution(行业Brinson归因含instrument breakdown) |
| PF-06 | 多账户管理 | YES | MultiAccountAllocator实现，账户层级/转账/统一资金分配/自动再平衡，支持父子账户关系 |
| MO-04 | 告警渠道(邮件/微信/短信) | YES | Telegram/DingTalk/WeChat Work/Email均已实现，支持真实HTTP调用 |
| MO-05 | 告警去重 | YES | 内容哈希去重(content_based_dedup参数)，SHA256消息+上下文去重，保留code级escalation，支持抑制窗口maintenance |
| MO-06 | 告警Web查询UI | YES | 后端API已实现，前端告警历史展示已添加 |
| RP-03 | 回测vs实盘漂移报告 | YES | slippage_analysis(逐笔vs信号价/方向性 adverse favorable分类)、signal_divergence(direction_mismatch/timing_diff)、drift_score composite(0-100评分+level)、drift_recommendations actionable |
| RP-04 | 归因/异常报告 | PARTIAL | 基础归因有，异常检测不足 |
| RP-06 | 合规报告 | YES | ComplianceReportService实现，支持持仓限额/日损失/杠杆/保证金/订单拒绝率/登录安全检查，生成合规报告含监管注意事项 |
| SE-03 | 审计日志(完整) | PARTIAL | ControlPlaneAPI所有操作均已接入audit，包含订单提交/撤销/回测/期货交易 |
| SE-05 | 凭证加密/保险库 | YES | AES-256加密存储(PBKDF2密钥派生)，支持HMAC验证，防盗保护 |
| SE-06 | 双因素认证/审批流 | YES | 真实TOTP实现(RFC 6238)，HMAC-SHA1，±1时间窗口容错，otpauth URI生成，支持QR码显示 |
| SE-07 | 访问控制(细粒度) | YES | authorize_resource()实现，user×resource×action三层权限链(explicit_grant→role→deny)，支持grant/revoke/list方法，审计日志完整 |
| IN-07 | NLP/LLM解读 | NO | 未实现 |

---

## 二、第一阶段增强（P1）— 完成度 ~82%

### 2.1 已完成
- [x] SW-01~SW-13: 股票研究工作台（多市场、多维筛选、F10、K线、对比、下载中心等）
- [x] CR-01~CR-05: 加密货币页面（资产概览、K线、用户状态、刷新策略）
- [x] LH-01~LH-07: 学习中心（知识库、搜索、课程计划、自动评分、进度保存）

### 2.2 未完成

| 编号 | 功能 | 状态 | 缺失细节 |
|------|------|------|----------|
| SW-14 | AI智能选股 | YES | 自然语言查询输入，AI解析RSI/MA/PE/成交量/基本面等条件，结果以匹配度排序展示，可跳转到个股详情 |
| CR-06 | 真实交易所数据 | YES | CoinGeckoClient实现，HTTP实时获取BTC/ETH/SOL等市场价格，60秒缓存，自动降级模拟 |
| CHART-01 | 专业绘图工具(趋势线/斐波那契) | YES | 趋势线(两点绘制)/斐波那契回撤(8级)，点击K线即可绘制，支持清除 |
| CHART-02 | 多技术指标叠加 | YES | MACD/KDJ/BOLL指标面板已实现 |
| CHART-03 | 多周期联动 | YES | 多周期按钮点击展开日线/4H/1H联动图表，支持关闭切换 |
| CHART-04 | 分时图 | YES | 分时图已完成，支持分钟级走势渲染 |
| CHART-05 | 筹码分布图 | YES | 筹码分布SVG渲染（横向柱状图，绿色成本区/红色套牢区，85%VA标注，当前价指示线） |
| CHART-06 | Footprint图 | YES | 订单流SVG渲染(买卖量堆叠柱、delta净买标签、颜色编码)，模拟OHLCV分布算法，6-12价格层级 |
| CHART-07 | TPO/Market Profile | YES | TPO字母块(A-Z)时间价格机会图，70%VA高亮，当前价格线 |
| MOB-01~05 | PWA移动端适配 | YES | manifest.json(快捷方式/图标)、service-worker.js(cache-first静态/network-first API)、beforeinstallprompt安装提示、PWA更新横幅 |

---

## 三、期货交易页（FT）— 完成度 ~90%

| 编号 | 功能 | 状态 | 缺失细节 |
|------|------|------|----------|
| FT-01 | 期货合约列表 | YES | 11个模拟合约 |
| FT-02 | 行情概览 | YES | 涨跌幅/成交量 |
| FT-03 | K线图表 | YES | 基础K线+持仓量(open_interest)、基差(basis/basis_pct/spot_price)指标，支持20+期货合约 |
| FT-04 | 合约详情 | YES | 合约乘数/到期日/交易规则 |
| FT-05 | 期货模拟交易 | YES | 下单面板、持仓展示、账户仪表盘已完成，支持做多/做空、市价/限价 |
| FT-06 | 期货风控(保证金/强平) | YES | 保证金占用/维持保证金/风控等级(安全/警告/危险/强平)，支持逐仓/全仓，可视化风险指示器 |
| FT-07 | 跨期套利工具 | YES | get_calendar_spread(近远月价差/价差%/年化Roll估算/z-score)、analyze_spread_history(均值/标准差/区间/z-score/趋势/回归潜力)、get_spread_trading_signal(BUY/SELL/SPREAD信号+置信度+驱动因素) |
| FT-08 | 期现套利工具 | NO | 未实现 |
| FT-09 | 持仓分析 | YES | get_position_analytics()实现，持仓时长/盈亏比/保证金效率/组合占比/多空敞口/集中度HHI，支持逐仓分析 |
| FT-10 | 真实期货数据源 | NO | 仅模拟数据 |

---

## 四、第二阶段增强（P2）— 完成度 0%

| 编号 | 功能 | 状态 |
|------|------|------|
| AI-01~AI-07 | AI/LLM助手(自然语言策略、智能问答、代码生成) | NO |
| SOC-01~SOC-06 | 社区交流(论坛/动态/策略分享) | NO |
| COPY-01~COPY-06 | 跟单交易(信号订阅/风控/分成) | NO |
| MKT-01~MKT-06 | 策略市场(上架/评价/交易) | NO |
| DSL-01~DSL-05 | 量化脚本语言(QuantScript) | NO |
| VIS-01~VIS-05 | 可视化策略编辑器(拖拽/流程图) | NO |
| HOOK-01~HOOK-05 | Webhook与工作流编排 | NO |
| COMP-01~COMP-04 | 量化竞赛平台 | NO |

---

## 五、第三阶段增强（P3）— 完成度 0%

| 编号 | 功能 | 状态 |
|------|------|------|
| ACCT-01~ACCT-04 | 多账户管理(子账户/统一视图) | NO |
| OPT-01~OPT-04 | 期权交易工具(Greeks/波动率曲面/策略构建) | NO |
| FX-01~FX-04 | 外汇交易(主流货币对/数据/回测) | NO |
| TAX-01~TAX-04 | 税务合规报告 | NO |
| COLLAB-01~COLLAB-04 | 多人协作(团队空间/策略协作) | NO |

---

## 六、机器人/自动交易（BOT）— 完成度 ~35%

| 编号 | 功能 | 状态 | 缺失细节 |
|------|------|------|----------|
| BOT-01 | 快速创建交易机器人 | PARTIAL | service骨架已实现，6种策略模板 |
| BOT-02 | 机器人控制(启停/参数调整) | YES | update_strategy_bot_params() API端点(/api/bots/params)、set_param命令封装、SSE bot_params_updated事件广播 |
| BOT-03 | 状态展示(运行/收益/风险) | YES | estimated_pnl_pct/estimated_pnl_abs字段、SSE实时bot_state_changed含完整PnL数据、前端实时更新bot-card无需全量刷新 |
| BOT-04 | 预设模板(网格/追踪/均值回归) | YES | 网格/均线/追踪止损/均值回归模板已实现 |
| BOT-05 | 通知集成 | YES | 已接入Telegram/DingTalk/WeChat Work/Email，支持真实HTTP调用，支持Markdown格式 |
| BOT-06 | 高级机器人(多策略组合) | NO | 未实现 |
| BOT-07 | AI调参 | NO | 未实现 |

---

## 七、基础设施与架构待完善

| 项目 | 状态 | 说明 |
|------|------|------|
| WebSocket实时推送 | PARTIAL | SSE Server-Sent Events已实现，支持bot状态/订单实时推送，HTTP轮询保留备用 |
| PostgreSQL + TimescaleDB | NO | 当前使用SQLite，生产环境需迁移 |
| Redis缓存 | YES | RedisCacheService实现，cache-aside模式，自动降级in-memory，支持klines/instruments/prices缓存，60秒健康检查 |
| Prometheus/Grafana监控 | PARTIAL | 指标收集器定义有，未集成Prometheus |
| CI/CD流水线 | YES | GitHub Actions workflow已创建 (ci.yml) |
| Docker容器化 | YES | Dockerfile + Dockerfile.prod + docker-compose.yml 已创建 |
| 性能压测 | NO | 无压力测试 |
| 端到端测试 | NO | 仅单元测试和集成测试 |

---

## 八、前端UI细节待完善

| 项目 | 状态 | 说明 |
|------|------|------|
| 告警历史查询页面 | YES | 已实现 |
| 报告生成与导出页面 | YES | 已实现日报/周报/月报 |
| 资金流向图表 | YES | 资金流向SVG渲染(累计净流入/流出，绿色 inflow/红色 outflow，gradient填充) |
| 龙虎榜 | YES | 龙虎榜面板(大单异动、机构席位、涨跌异动评分，买入/卖出/活跃三列展示) |
| 五档盘口 | YES | 已实现 |
| 成交明细 | YES | 已实现 |
| 分时图 | YES | 已实现分时SVG渲染 |
| 自选股分组管理 | YES | 已实现分组CRUD |
| 策略运行监控页面 | YES | 策略运行状态卡片展示PnL/订单数/目标权重，SSE实时推送 |

---

## 优先级建议

### 短期（1-2周内可完成）— 已全部完成
1. ✅ 告警查询UI（MO-06）
2. ✅ 报告展示页面（RP 相关前端）
3. ✅ 分时图（CHART-04）
4. ✅ 五档盘口与成交明细展示
5. ✅ 自选股分组管理
6. ✅ 期货模拟交易（FT-05）
7. ✅ 更多技术指标图表（MACD/KDJ/BOLL）
8. ✅ AI智能选股（SW-14）
9. ✅ 筹码分布图（CHART-05）
10. ✅ 多周期联动（CHART-03）
11. ✅ TPO/Market Profile（CHART-07）

### 中期（1-2个月）
8. ✅ WebSocket实时推送替换轮询（SSE已实现）
9. BOT机器人框架完善（BOT-01~04）
10. ✅ 高级图表工具（趋势线/斐波那契/筹码分布/TPO） — CHART-01/03/05/07 均已完成
11. ✅ 策略运行监控页面
12. ✅ 邮件/微信告警渠道（支持Telegram/DingTalk/WeChat Work/Email）
13. ✅ Docker容器化 + CI/CD
14. ✅ 凭证加密存储（SE-05 AES-256）

### 长期（3-6个月）
15. AI/LLM助手集成
16. DSL量化脚本语言
17. 可视化策略编辑器
18. 社区/跟单/策略市场
19. 期权/外汇支持
20. PostgreSQL迁移 + 分布式部署
