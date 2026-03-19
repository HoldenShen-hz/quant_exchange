# 量化因子库与实现方法指南

> 文档层级：研究与策略层
>  
> 推荐读者：量化研究员、策略开发者、数据工程师、回测工程师
>  
> 建议前置阅读：[需求分析](./quant_trading_requirements_analysis.md) / [详细功能规格](./quant_trading_detailed_function_spec.md)
>  
> 相关文档：[组合、风控与交易方法框架](./quant_trading_portfolio_risk_and_trading_methods_framework.md) / [情绪工具设计](./quant_trading_market_intelligence_sentiment_tool_design.md)

## 1. 文档目标

本文档用于系统整理 `quant_exchange` 平台中对量化研究、回测、实盘和风险控制有帮助的主流因子，并说明：

- 因子的金融含义
- 适用市场
- 常见计算公式或计算口径
- 推荐实现方法
- 工程落地时的注意事项

需要先说明一个边界：

不存在对所有市场、所有周期、所有策略都永远有效的“万能因子”，也不可能用一份文档穷尽所有变种。本文档的目标不是声称列出了宇宙中全部因子，而是尽量完整地覆盖平台应该支持的主流因子家族、代表性因子和落地实现方法。

## 2. 因子设计总原则

### 2.1 因子不是指标堆砌

一个可用因子通常应满足以下至少一条：

- 能解释未来收益的横截面差异
- 能解释未来收益的时间序列方向偏置
- 能解释未来风险、滑点、成交概率或流动性变化
- 能辅助组合构建、仓位控制和风险预算

### 2.2 因子必须区分用途

同一个因子在平台中可能用于不同环节：

- Alpha 因子：预测未来收益方向或超额收益
- Risk 因子：预测波动、回撤、尾部风险、流动性风险
- Execution 因子：预测成交难度、滑点、冲击成本
- Overlay 因子：用于过滤、加权、降杠杆、择时或停机

### 2.3 因子必须做到点时一致

任何因子计算都必须满足：

- 使用 point-in-time 数据
- 不得提前看到未来值
- 基本面数据要使用公告时点而不是报告期终点
- 公司行为调整口径必须清晰
- 跨市场时区、交易日历和结算周期必须明确

### 2.4 因子工程标准流程

任一因子建议统一经过以下步骤：

1. 原始数据采集
2. 对齐时间与标的
3. 缺失值处理
4. 去极值
5. 标准化
6. 中性化
7. 滚动计算
8. 落库与版本化
9. 评估 IC、收益、换手、衰减

## 3. 通用实现模板

### 3.1 常用计算模板

```text
return_n = close_t / close_t-n - 1
log_return_n = ln(close_t / close_t-n)
rolling_mean_n = mean(x[t-n+1:t])
rolling_std_n = std(x[t-n+1:t])
zscore_n = (x_t - rolling_mean_n) / rolling_std_n
ewm_mean = exponentially_weighted_mean(x, halflife)
rank_cs = cross_sectional_rank(x_t at same timestamp)
beta = cov(asset_ret, benchmark_ret) / var(benchmark_ret)
residual = y - Xb
```

### 3.2 通用工程实现方式

- 批处理日频因子：`SQL + window function`、`Pandas`、`Polars`
- 中高频因子：事件流聚合、分钟级 materialize job
- 跨截面因子：按交易日对截面做 rank / zscore / neutralization
- 文本因子：先做 NLP 解析，再产出结构化特征
- 链上因子：按区块时间或自然时间聚合后再与行情对齐

### 3.3 标准化建议

- 趋势类因子：常用截面 rank、时间序列 zscore
- 基本面类因子：常用行业中性化 + 截面标准化
- 流动性类因子：常用对数变换 + winsorize
- 情绪类因子：常用 `[-1, 1]` 或 `[0, 1]` 标准化区间

### 3.4 中性化建议

股票市场常见：

- 市值中性化
- 行业中性化
- Beta 中性化
- 国家 / 交易所中性化

期货和加密货币常见：

- 品种组中性化
- 杠杆与流动性分层后再标准化

## 4. 趋势与动量因子

### 4.1 收益率动量因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `ret_1d` / `ret_5d` / `ret_20d` / `ret_60d` | 过去 N 日收益率 | `close_t / close_t-n - 1` | 直接滚动计算 | 股票、期货、加密货币 |
| `log_ret_n` | 对数收益率 | `ln(close_t / close_t-n)` | 对收益平滑，便于叠加 | 全市场 |
| `skip_short_mom` | 跳过最近短期噪声的动量 | 如 `ret_252d - ret_21d` | 常用于股票横截面动量 | 股票 |
| `ts_mom_sign` | 时间序列动量方向 | `sign(ret_n)` | 正为多头偏置，负为空头偏置 | 期货、加密货币 |
| `cs_mom_rank` | 横截面动量排序 | 当日截面对 `ret_n` 排名 | 用于 long-short 排序 | 股票、期货、加密货币 |
| `residual_momentum` | 剔除市场和行业暴露后的动量 | 回归残差的累计收益 | 先回归再用残差收益构造 | 股票 |
| `trend_slope` | 趋势斜率 | 回归 `log(price)` 对时间斜率 | 滚动线性回归 | 全市场 |
| `ema_gap` | 价格与均线偏离 | `close / ema_n - 1` | 多周期 EMA 组合 | 全市场 |
| `breakout_pct` | 突破强度 | `(close - rolling_min) / (rolling_max - rolling_min)` | Donchian / 通道类计算 | 全市场 |

### 4.2 趋势强化因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `ma_cross_strength` | 均线多空强度 | `ema_short / ema_long - 1` | 5/20、20/60、50/200 等 | 全市场 |
| `adx` | 趋势强弱 | DMI / ADX 体系 | 用高低收计算方向强度 | 全市场 |
| `price_above_ma_ratio` | 价格站上均线的程度 | `close > ma_n` 的布尔或距离 | 可多周期拼接为因子向量 | 全市场 |
| `channel_break_days` | 突破持续天数 | 连续位于通道上沿之上的天数 | 用状态机累积 | 股票、期货、加密货币 |
| `trend_consistency` | 趋势一致性 | 多周期收益同号比例 | 1d/5d/20d/60d 方向一致统计 | 全市场 |

## 5. 反转与均值回归因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `short_term_reversal` | 短期反转 | `-ret_1d`、`-ret_3d` | 股票中常用隔日反转 | 股票 |
| `intraday_reversal` | 日内反转 | `-(close / open - 1)` | 与隔夜收益组合更常见 | 股票、期货、加密货币 |
| `overnight_gap_reversal` | 隔夜缺口回补 | `open_t / close_t-1 - 1` 的反向 | 需分隔夜和日内收益 | 股票、期货 |
| `bollinger_zscore` | 相对均值偏离程度 | `(close - ma_n) / std_n` | 偏离越大越可能回归 | 全市场 |
| `rsi` | 超买超卖 | RSI 计算式 | 可直接用或做 zscore | 全市场 |
| `stoch_position` | 价格处于近期区间的位置 | `(close - low_n) / (high_n - low_n)` | 适合震荡过滤 | 全市场 |
| `mean_reversion_half_life` | 回归速度 | AR(1) 估计半衰期 | 用于选择回归窗口 | 股票、期货 |
| `pair_spread_zscore` | 配对价差偏离 | `(spread - mean) / std` | 先确定配对关系或协整关系 | 股票、期货、加密货币 |

## 6. 波动率与风险因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `realized_vol` | 已实现波动率 | `std(ret_n) * sqrt(annualizer)` | 最基础风险因子 | 全市场 |
| `ewm_vol` | EWMA 波动率 | 指数加权标准差 | 对近期变化更敏感 | 全市场 |
| `atr` | 平均真实波幅 | ATR 公式 | 常用于止损和仓位缩放 | 全市场 |
| `parkinson_vol` | 基于高低价的波动率 | `ln(high/low)` 系列公式 | 适合日频估计 | 股票、期货 |
| `garman_klass_vol` | 基于 OHLC 的波动率 | GK 公式 | 比简单收盘收益更稳定 | 股票、期货 |
| `downside_vol` | 下行波动率 | 仅统计负收益标准差 | 常用于 Sortino 风格风险建模 | 全市场 |
| `realized_skew` | 收益偏度 | `skew(ret_n)` | 用于尾部方向风险识别 | 全市场 |
| `realized_kurtosis` | 收益峰度 | `kurtosis(ret_n)` | 用于肥尾和极端事件监控 | 全市场 |
| `max_drawdown_window` | 窗口最大回撤 | 滚动峰谷回撤 | 适合作为风险过滤器 | 全市场 |
| `beta_market` | 对市场因子的 Beta | `cov(asset, market)/var(market)` | 股票中用于中性化 | 股票 |
| `idiosyncratic_vol` | 特质波动率 | 收益回归后残差波动 | 先做市场 / 行业回归 | 股票 |
| `tail_loss_ratio` | 尾部损失比率 | 下分位损失均值 / 总体波动 | 适合高风险资产过滤 | 全市场 |

## 7. 成交量、换手、流动性与订单流因子

### 7.1 基础成交量因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `volume_ratio_n` | 当前成交量相对历史均值 | `vol_t / mean(vol_n)` | 量能放大识别 | 全市场 |
| `turnover_ratio` | 换手率 | `volume / float_shares` 或 `notional / mcap` | 股票最常用 | 股票 |
| `volume_zscore` | 成交量异常程度 | `(vol - mean) / std` | 与价格突破结合常用 | 全市场 |
| `obv` | 能量潮 | 累计 `sign(ret) * volume` | 趋势确认型因子 | 股票、期货、加密货币 |
| `pvt` | 价量趋势 | 累计 `ret * volume` | 与 OBV 类似但连续化 | 全市场 |
| `money_flow_ratio` | 资金流向强弱 | 正负成交额对比 | 可构造 MFI 类指标 | 全市场 |

### 7.2 流动性与冲击成本因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `amihud_illiq` | 单位成交额价格冲击 | `abs(ret) / notional` | 流动性越差越大 | 股票、期货、加密货币 |
| `turnover_decay` | 流动性衰减 | 近短期成交额 / 中长期成交额 | 判断近期是否失去流动性 | 全市场 |
| `bid_ask_spread` | 买卖价差 | `(ask-bid)/mid` | 高频执行和过滤关键因子 | 股票、期货、加密货币 |
| `depth_topn` | 前 N 档深度 | `sum(bid_size_i + ask_size_i)` | 衡量可成交性 | 期货、加密货币、部分股票 |
| `depth_imbalance` | 深度失衡 | `(bid_depth - ask_depth)/(bid_depth + ask_depth)` | 高频方向因子 | 期货、加密货币 |
| `order_imbalance` | 委托不平衡 | `buy_qty - sell_qty` 标准化 | 需订单簿或逐笔委托数据 | 高频市场 |
| `signed_volume_imbalance` | 主动买卖量差 | `taker_buy - taker_sell` | 常用于微观结构信号 | 期货、加密货币 |
| `vwap_deviation` | 与 VWAP 偏离 | `(price - vwap)/vwap` | 可做回归或执行参考 | 全市场 |
| `vpin` | 流动性毒性 | VPIN 近似公式 | 高级微观结构因子 | 高频市场 |
| `cancel_ratio` | 撤单比率 | `cancel_count / new_order_count` | 高频做市或低流动市场风险信号 | 高频市场 |

## 8. 股票基本面、估值、质量与成长因子

### 8.1 估值因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `pe` / `ep` | 市盈率 / 盈利收益率 | `price / eps` 或 `eps / price` | 更推荐使用 `ep` 便于线性组合 | 股票 |
| `pb` / `bp` | 市净率 / 账面市值比 | `price / book_value` | 价值风格经典因子 | 股票 |
| `ps` / `sp` | 市销率 / 销售收益率 | `sales / mcap` | 适合利润波动大的行业 | 股票 |
| `pcf` | 市现率 | `operating_cash_flow / mcap` | 现金流质量更强时有用 | 股票 |
| `ev_ebitda` | 企业价值估值 | `EV / EBITDA` | 资本结构敏感行业常用 | 股票 |
| `dividend_yield` | 股息率 | `dividend / price` | 稳健型策略常用 | 股票 |
| `fcf_yield` | 自由现金流收益率 | `free_cash_flow / enterprise_value` | 质量价值结合 | 股票 |

### 8.2 质量与盈利能力因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `roe` | 净资产收益率 | `net_income / equity` | 需 point-in-time 财报 | 股票 |
| `roa` | 总资产收益率 | `net_income / total_assets` | 与资本结构弱相关 | 股票 |
| `gross_profitability` | 毛利盈利能力 | `gross_profit / assets` | 学术上较稳健 | 股票 |
| `operating_margin` | 营业利润率 | `op_income / revenue` | 质量与护城河代理变量 | 股票 |
| `asset_turnover` | 资产周转率 | `revenue / assets` | 经营效率 | 股票 |
| `cash_conversion` | 现金转化能力 | `operating_cf / net_income` | 识别利润含金量 | 股票 |
| `accruals` | 应计项质量 | `(net_income - operating_cf) / assets` | 应计越高质量越弱 | 股票 |
| `net_operating_assets` | 运营资产扩张 | N.O.A. 变化率 | 过度扩张常与低未来收益相关 | 股票 |

### 8.3 杠杆、偿债与稳健性因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `debt_to_equity` | 资产负债杠杆 | `total_debt / equity` | 高杠杆过滤 | 股票 |
| `interest_coverage` | 利息覆盖 | `ebit / interest_expense` | 低于阈值时风险高 | 股票 |
| `current_ratio` | 流动比率 | `current_assets / current_liabilities` | 短期偿债能力 | 股票 |
| `altman_z_proxy` | 财务稳健代理 | 多项财务比率组合 | 可做信用风险过滤 | 股票 |

### 8.4 成长与修正因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `revenue_growth` | 收入增长 | `sales_t / sales_t-4q - 1` | 季度同比或滚动 12M | 股票 |
| `earnings_growth` | 盈利增长 | `ni_t / ni_t-4q - 1` | 需处理亏损翻正异常值 | 股票 |
| `margin_improvement` | 利润率改善 | `margin_t - margin_t-4q` | 经营拐点信号 | 股票 |
| `analyst_revision` | 分析师预期上修 | 预期 EPS / revenue 上修幅度 | 需外部数据源 | 股票 |
| `earnings_surprise` | 业绩超预期 | `(actual - estimate) / abs(estimate)` | 事件驱动核心因子 | 股票 |
| `post_earnings_drift` | 业绩后漂移 | 公告后 1-20 日持续偏置 | 需事件对齐回测 | 股票 |

### 8.5 规模与市场结构因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `size` | 规模因子 | `ln(market_cap)` | 常用于风格分层或中性化 | 股票 |
| `free_float_mcap` | 自由流通市值 | `price * float_shares` | 比总市值更可交易 | 股票 |
| `share_turnover` | 股本换手 | `volume / float_shares` | A 股和港股常用 | 股票 |
| `short_interest_ratio` | 卖空拥挤度 | `short_interest / avg_volume` | 需券商或交易所数据 | 港股、美股 |
| `borrow_fee` | 融券成本 | 年化借券费率 | 对做空策略很重要 | 港股、美股 |

## 9. 期货与商品因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `basis` | 基差 | `futures_price - spot_price` | 需现货或指数参考价 | 期货、加密货币交割合约 |
| `annualized_basis` | 年化基差 | `basis / spot * 365 / days_to_expiry` | 便于跨期限比较 | 期货、加密货币 |
| `roll_yield` | 展期收益 | 近月与远月价差按期限年化 | 期货 carry 核心因子 | 期货 |
| `term_structure_slope` | 期限结构斜率 | `(far - near) / tenor_diff` | 识别 contango / backwardation | 期货、加密货币 |
| `open_interest_change` | 未平仓量变化 | `OI_t / OI_t-n - 1` | 趋势确认和拥挤度代理 | 期货、加密货币 |
| `price_oi_confirmation` | 价格与 OI 同步性 | `sign(ret) * sign(delta_oi)` | 判断增仓上涨或减仓反弹 | 期货、加密货币 |
| `volume_oi_ratio` | 成交 / 持仓比 | `volume / open_interest` | 识别换手和挤仓 | 期货、加密货币 |
| `inventory_proxy` | 库存代理因子 | 交易所库存、仓单、可交割量 | 需外部数据 | 商品期货 |
| `hedging_pressure` | 套保压力 | 基于 COT 或持仓结构数据 | 高级外部因子 | 海外期货 |
| `calendar_spread_zscore` | 跨期价差偏离 | `spread_zscore(near-far)` | 套利和期限结构策略常用 | 期货 |
| `settlement_deviation` | 收盘价相对结算价偏离 | `(close - settle) / settle` | 反映尾盘扭曲 | 期货 |
| `main_contract_roll_signal` | 主力切换信号 | 主连映射变化与持仓迁移 | 需 continuous mapping | 期货 |

## 10. 加密货币与链上因子

### 10.1 交易所与衍生品因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `funding_rate_level` | 资金费率水平 | 当期 funding rate | 直接采集并对齐到 K 线 | 加密货币永续 |
| `funding_rate_change` | 资金费率变化 | `fr_t - fr_t-n` | 识别拥挤方向变化 | 加密货币永续 |
| `perp_spot_premium` | 永续相对现货溢价 | `(perp - spot) / spot` | 可与 funding 联用 | 加密货币 |
| `mark_index_gap` | 标记价与指数价偏离 | `(mark - index) / index` | 强平与异常波动监控 | 加密货币衍生品 |
| `liquidation_imbalance` | 多空爆仓失衡 | `(long_liq - short_liq) / total_liq` | 需 liquidation feed | 加密货币 |
| `long_short_ratio` | 多空账户比 | 交易所提供指标 | 做拥挤度辅助，不宜单独使用 | 加密货币 |
| `taker_buy_ratio` | 主动买盘占比 | `taker_buy_volume / total_volume` | 高频方向和拥挤度因子 | 加密货币 |
| `oi_notional` | 名义未平仓 | `open_interest * mark_price` | 更适合跨品种比较 | 加密货币 |
| `oi_acceleration` | OI 加速变化 | `delta_oi_short - delta_oi_long` | 检测挤仓与情绪升温 | 加密货币 |
| `basis_funding_combo` | 基差和 funding 组合 | 标准化后线性或非线性融合 | 常用于 carry 交易 | 加密货币衍生品 |

### 10.2 资金流与链上因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `exchange_inflow` | 流入交易所规模 | 单位时间净流入 | 常被视为潜在卖压 | 加密货币 |
| `exchange_outflow` | 流出交易所规模 | 单位时间净流出 | 常被视为持币和冷存储倾向 | 加密货币 |
| `stablecoin_netflow` | 稳定币净流入 | 交易所稳定币流入减流出 | 风险偏好代理 | 加密货币 |
| `active_addresses` | 活跃地址数 | 时间窗口内活跃地址数量 | 链上活跃度因子 | 加密货币 |
| `tx_count` | 链上交易笔数 | 时间窗口交易数 | 网络活跃度 | 加密货币 |
| `nvt` | 网络价值交易比 | `market_cap / tx_volume` | 类似链上估值因子 | 加密货币 |
| `mvrv` | 市值 / 已实现市值比 | `market_cap / realized_cap` | 周期顶部和底部常用因子 | 加密货币 |
| `sopr` | 已花费输出利润率 | SOPR 指标 | 获利了结压力代理 | 加密货币 |
| `whale_concentration` | 大户集中度 | Top 地址持仓占比 | 拥挤度和操纵风险代理 | 加密货币 |
| `miner_selling_proxy` | 矿工卖压代理 | 矿工地址转出规模 | 比特币等 PoW 资产较常见 | 加密货币 |

## 11. 股票市场结构与本地化因子

### 11.1 A 股常见增强因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `northbound_netflow` | 北向资金净流入 | 资金净买入 / 成交额 | 需互联互通数据 | A 股 |
| `limit_up_proximity` | 接近涨停程度 | `(limit_up_price - close) / close` | 可做拥挤或封板强度代理 | A 股 |
| `suspension_risk` | 停牌风险代理 | 历史停牌频率、公告事件 | 风险过滤因子 | A 股 |
| `theme_heat` | 题材热度 | 同主题涨停数、新闻热度、成交占比 | 需概念映射表 | A 股 |
| `auction_strength` | 集合竞价强度 | 竞价量价与前收比较 | 需竞价数据 | A 股 |

### 11.2 港股常见增强因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `southbound_netflow` | 南向资金净流入 | 南向资金净买入 | 需互联互通数据 | 港股 |
| `board_lot_liquidity` | 整手流动性约束 | `board_lot * price / avg_daily_notional` | 评估最小交易成本 | 港股 |
| `cas_participation` | 收市竞价参与度 | CAS 成交占全天比 | 尾盘成交特征 | 港股 |
| `dual_listed_spread` | 双重上市价差 | 本地股与 ADR / A-H 溢价 | 需跨市场映射 | 港股 |

### 11.3 美股常见增强因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `pre_market_gap` | 盘前缺口 | `pre_open / prev_close - 1` | 需盘前数据 | 美股 |
| `after_hours_move` | 盘后异动 | `after_close / regular_close - 1` | 事件驱动常用 | 美股 |
| `short_interest_days` | 空头回补天数 | `short_interest / avg_daily_volume` | 挤仓代理 | 美股 |
| `borrow_fee_change` | 借券费变化 | `fee_t - fee_t-n` | 做空拥挤度变化 | 美股 |
| `pdt_risk_score` | 日内交易限制风险 | 当周 day trade 次数与权益约束 | 账户维度风控因子 | 美股 |

## 12. 情绪、事件与另类数据因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `news_sentiment` | 新闻情绪分数 | 文档级 `[-1,1]` 或分类概率 | NLP 情感分类 + source weighting | 全市场 |
| `event_impact_score` | 事件冲击分数 | 事件类别 * 严重度 * 新颖性 | 事件抽取与规则融合 | 全市场 |
| `document_heat` | 热度因子 | 转载数、阅读量、作者权重、传播速度 | 文档级聚合 | 全市场 |
| `source_credibility` | 来源可信度 | 历史命中率、权威级别、白名单 | 元数据评分 | 全市场 |
| `social_sentiment` | 社交情绪 | 帖文情感均值 * 互动权重 | 社媒文本情感模型 | 股票、加密货币 |
| `search_trend` | 搜索热度 | 关键词搜索指数变化 | 外部搜索趋势数据 | 股票、加密货币、宏观 |
| `novelty_score` | 新颖性 | 当前文本与历史文本 embedding 距离 | 去重后做新颖度评估 | 全市场 |
| `entity_attention` | 主体关注度 | 同一标的 / 公司文档计数与热度 | 做实体级聚合 | 全市场 |
| `policy_tone` | 政策语气 | 政策文本 hawkish / dovish / neutral | 宏观和行业事件因子 | 股票、期货 |
| `earnings_call_tone` | 业绩会情绪 | 会议纪要情绪、风险词频、指引修正 | NLP + 词典 / 模型 | 股票 |
| `web_traffic_growth` | 网站流量增长 | 站点访问趋势变化 | 另类数据供应商 | 股票 |
| `app_rank_change` | 应用排名变化 | 排名变化率 | 消费、互联网行业辅助因子 | 股票 |

## 13. 宏观、市场宽度与跨资产因子

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `benchmark_return` | 基准市场收益 | 主要指数收益 | 大盘择时或 Beta 过滤 | 股票、期货、加密货币 |
| `vix_level` / `iv_regime` | 波动率制度 | VIX 或同类隐波指标 | 风险状态识别 | 股票、期货 |
| `yield_curve_slope` | 利率曲线斜率 | 长端国债收益率 - 短端收益率 | 宏观风格因子 | 股票、期货 |
| `credit_spread` | 信用利差 | 公司债利差 / 高收益利差 | 风险偏好代理 | 股票、期货 |
| `dxy` / `usd_strength` | 美元强弱 | 美元指数变动 | 商品、加密货币、海外资产 | 期货、加密货币、股票 |
| `term_liquidity_proxy` | 货币流动性代理 | R007、SOFR、逆回购余额等 | 市场流动性状态 | 股票、期货 |
| `advance_decline_ratio` | 涨跌广度 | 上涨家数 / 下跌家数 | 市场宽度因子 | 股票 |
| `new_high_ratio` | 新高占比 | 创 N 日新高标的比率 | 趋势广度确认 | 股票、期货、加密货币 |
| `sector_dispersion` | 板块分化度 | 行业收益横截面标准差 | 风格轮动和 alpha 容量判断 | 股票 |
| `correlation_regime` | 相关性制度 | 截面平均相关系数 | 相关性升高时 alpha 容易失效 | 全市场 |
| `cross_asset_momentum` | 跨资产动量 | 股、债、汇、商品的综合趋势 | 做宏观 regime filter | 股票、期货、加密货币 |
| `macro_surprise` | 宏观数据超预期 | `(actual - consensus) / historical_std` | 事件驱动因子 | 股票、期货、外汇 |

## 14. 期权衍生因子

虽然当前平台核心范围不是期权实盘，但很多期权派生特征对股票、期货和加密货币标的都很有帮助。

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `implied_vol_level` | 隐含波动率水平 | ATM IV 或 25D IV | 从期权链提取 | 股票、期货、加密货币 |
| `iv_rv_spread` | 隐波与实波利差 | `iv - realized_vol` | 波动率风险溢价 | 股票、期货、加密货币 |
| `put_call_ratio` | 看跌看涨比 | put volume / call volume | 情绪和对冲需求代理 | 股票、期货 |
| `skew` | 偏斜 | OTM put IV - OTM call IV | 尾部风险偏好 | 股票、期货、加密货币 |
| `term_structure_iv` | 隐波期限结构 | 远月 IV - 近月 IV | 事件和波动预期 | 股票、期货、加密货币 |
| `gamma_exposure_proxy` | Gamma 暴露代理 | 按 strike 聚合 OI * gamma | 用于理解价格磁吸和挤压 | 股票、加密货币 |

## 15. 组合构建与风险控制辅助因子

这些因子不一定直接预测收益，但对稳定收益非常重要。

| 因子 | 含义 | 核心口径 | 实现方法 | 适用市场 |
| --- | --- | --- | --- | --- |
| `corr_to_book` | 与组合相关性 | 与当前组合收益相关系数 | 低相关更有分散价值 | 全市场 |
| `factor_crowding_score` | 因子拥挤度 | 暴露相似性、容量、成交占比组合 | 防止拥挤交易 | 全市场 |
| `capacity_score` | 容量评分 | `avg_volume / target_notional` | 判断可放大资金量 | 全市场 |
| `slippage_risk_score` | 滑点风险评分 | spread、深度、冲击成本组合 | 实盘执行前过滤 | 全市场 |
| `vol_target_scale` | 波动目标缩放因子 | `target_vol / realized_vol` | 仓位控制最常用 | 全市场 |
| `drawdown_guard_score` | 回撤保护分数 | 组合回撤、市场波动、流动性联合评分 | 用于动态降仓 | 全市场 |

## 16. 因子的推荐实现方法

### 16.1 日频和分钟频批处理

适合：

- 价格动量
- 波动率
- 基本面估值
- 跨截面排序
- 大部分股票和期货因子

推荐实现：

- PostgreSQL / TimescaleDB 做基础聚合
- Python `pandas` / `polars` 做特征计算
- 落库到 Feature Store

如果以下条件同时成立，建议把热点算子迁移到 Rust：

- 因子定义已经比较稳定
- 需要流式或高频实时计算
- Python 已经成为 CPU 或内存瓶颈
- 需要在实时执行链路中直接使用

### 16.2 事件驱动流式计算

适合：

- 订单簿不平衡
- 主动买卖量差
- 深度失衡
- liquidation feed
- 新闻流、社交流、公告流

推荐实现：

- WebSocket / queue ingest
- Redis / stream 做缓冲
- worker 做分钟或秒级 materialize
- 结果落库并附上 `as_of_time`

### 16.3 Point-in-time 基本面实现

适合：

- 股票估值因子
- 财务质量因子
- 盈利增长因子

推荐实现：

- 原始财报表保留公告时间
- 因子计算时按公告时点 join
- 不得用未来修订值覆盖历史可见值

### 16.4 文本与情绪因子实现

适合：

- 新闻情绪
- 公告事件
- 社交情绪
- 政策语气

推荐实现：

1. 文本采集
2. 去重和实体识别
3. 情感 / 事件分类
4. 热度和可信度评分
5. 时间窗口聚合到标的维度
6. 输出标准化特征和方向偏置

### 16.5 链上因子实现

适合：

- 活跃地址
- 交易所净流量
- 稳定币净流入
- MVRV / SOPR / NVT

推荐实现：

- 使用链上供应商或自建索引节点
- 按区块时间聚合到分钟 / 小时 / 日
- 再与市场数据统一到交易时钟

## 17. 因子工程中的关键注意事项

### 17.1 防止数据泄漏

必须防止以下错误：

- 用收盘后才知道的值指导收盘前交易
- 用财报期末值替代公告时可见值
- 用复权后价格计算真实当时无法看到的信号
- 用未来样本做标准化参数

### 17.2 因子稳定性与衰减

每个因子至少应评估：

- IC
- Rank IC
- 分组收益
- 因子衰减曲线
- 换手率
- 容量
- 子时期稳定性

### 17.3 不同市场要用不同因子子集

不要把所有因子都强塞到所有市场中：

- 股票更适合基本面、估值、质量、公司行为、情绪和资金流因子
- 期货更适合 carry、基差、期限结构、库存、OI 和宏观 regime 因子
- 加密货币更适合 funding、basis、OI、liquidation、链上和情绪因子

### 17.4 因子组合优于单因子迷信

大多数有效策略不是依赖单一因子，而是：

- 同类因子聚合
- 不同类因子互补
- 结合风险和执行过滤
- 动态按 regime 加权

## 18. 平台建议优先落地的因子集合

### 18.1 MVP 股票因子集合

- 20 日 / 60 日动量
- 5 日反转
- 成交量放大
- 换手率
- 波动率
- 市值
- PE / PB / EP / BP
- ROE
- 营收增长
- 新闻情绪
- 事件冲击分数
- 公司行为风险标记

### 18.2 MVP 期货因子集合

- 时间序列动量
- ATR / realized vol
- 基差
- 年化基差
- roll yield
- OI 变化
- 价格和 OI 确认
- 宏观 regime filter
- 新闻和政策事件因子

### 18.3 MVP 加密货币因子集合

- 20 日 / 60 日动量
- 1 日 / 3 日反转
- realized vol
- volume ratio
- funding rate level / change
- perp premium
- OI 和 OI acceleration
- taker buy ratio
- liquidation imbalance
- 新闻情绪
- 社交情绪
- 链上净流量

## 19. 与本项目的关系

本文档应直接服务于以下模块：

- 策略开发模块
- Feature Store
- 因子表达式引擎
- 回测引擎
- 模拟盘和实盘策略运行时
- 情报与情绪分析模块
- 报表与因子评估模块

建议后续继续补充三类实现级文档：

- 因子表达式 DSL 设计
- Feature Store DDL 与数据分层
- 因子评估与淘汰规则文档

## 20. 结论

对量化真正有帮助的因子，不只是“预测涨跌”的因子，还包括风险、流动性、执行、情绪、宏观和制度因子。

平台建设时最重要的不是把因子名字列得越多越好，而是把以下几件事做好：

- 因子定义清晰
- 数据口径统一
- 计算过程可复现
- 点时一致
- 评估方法标准化
- 因子与策略、执行、风控链路打通

只有这样，因子库才能从“研究员的零散脚本”变成“平台级资产”。
