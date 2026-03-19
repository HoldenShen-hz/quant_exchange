"""Tests for enhanced new modules: Charting, AIAssistant, SmartScreener, DSL, VisualEditor, CopyTrading, InformationSources."""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from quant_exchange.enhanced import (
    # Charting
    ChartingService,
    ChartType,
    IndicatorType,
    AnnotationType,
    IndicatorConfig,
    # AI Assistant
    AIAssistantService,
    AIIntent,
    AIGlobalConfig,
    ConversationTurn,
    ConfidenceScore,
    KnowledgeGraphEntry,
    ResearchExplanation,
    RiskAdvisory,
    RiskLevel,
    StrategyDraft,
    TradingRecommendation,
    RecommendationAction,
    # Smart Screener
    SmartScreenerService,
    ScreenerDefinition,
    ScreeningResult,
    FactorCondition,
    PatternMatch,
    PatternType,
    ScreenDirection,
    ScreenEntity,
    ScreenerPerformance,
    # DSL
    DSLService,
    DSLStrategy,
    DSLValue,
    DSLNodeType,
    CompiledFactor,
    FactorUniverse,
    DSLLexer,
    DSLParser,
    DSLEvaluator,
    # Visual Editor
    VisualEditorService,
    VisualNode,
    CanvasState,
    Connection,
    NodeType,
    NodeCategory,
    # Copy Trading
    CopyTradingService,
    CopyMode,
    CopyStatus,
    CopyTrade,
    CopiedPosition,
    DeviationAlert,
    SignalEvent,
    SignalProvider,
    SignalType,
    # Information Sources
    InformationService,
    InformationSource,
    NewsArticle,
    SocialPost,
    FilingDocument,
    ResearchReport,
    SentimentSummary,
    AlternativeDataPoint,
    SourceType,
    SentimentLabel,
    TwitterSentimentAdapter,
    RedditSentimentAdapter,
    ReutersNewsAdapter,
    EdgarFilingAdapter,
    EconomicCalendarAdapter,
)


# Import EconomicEvent from information_sources (name conflicts with fx.EconomicEvent)
from quant_exchange.enhanced.information_sources import EconomicEvent as InfoEconomicEvent
from quant_exchange.enhanced.portfolio_allocators import (
    PortfolioAllocatorService,
    AllocatorConfig,
    AllocatorType,
    AllocationResult,
    PortfolioAllocation,
    RebalanceTrigger,
    RebalancePlan,
    RiskBudget,
    # PF-04
    RiskExposureAggregator,
    AggregatedExposure,
    StrategyExposure,
    # PF-05
    AttributionAnalyzer,
    AttributionResult,
    # PF-06
    MultiAccountAllocator,
    Account,
    TransferRequest,
    CapitalAllocationPlan,
)


# ─────────────────────────────────────────────────────────────────────────────
# Charting Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestChartingService(unittest.TestCase):
    def setUp(self):
        self.charting = ChartingService()

    def test_create_chart(self):
        chart = self.charting.create_chart(
            user_id="user1",
            instrument_id="AAPL",
            chart_type=ChartType.KLINE,
            period="1d",
        )
        self.assertTrue(chart.chart_id.startswith("chart:"))
        self.assertEqual(chart.user_id, "user1")
        self.assertEqual(chart.instrument_id, "AAPL")
        self.assertEqual(chart.period, "1d")

    def test_add_panel(self):
        chart = self.charting.create_chart("user1", "AAPL", ChartType.KLINE)
        panel = self.charting.add_panel(
            chart.chart_id,
            indicators=[
                IndicatorConfig(indicator_type=IndicatorType.MA, params={"period": 20})
            ],
            height_ratio=1.5,
        )
        self.assertIsNotNone(panel)
        self.assertEqual(chart.chart_id, panel.chart_id)

    def test_add_annotation(self):
        chart = self.charting.create_chart("user1", "AAPL", ChartType.KLINE)
        ann = self.charting.add_trendline(
            chart.chart_id,
            start_time=datetime.now(timezone.utc),
            end_time=datetime.now(timezone.utc),
            start_price=100.0,
            end_price=110.0,
        )
        self.assertIsNotNone(ann)
        self.assertEqual(ann.annotation_type, AnnotationType.TRENDLINE)

    def test_compute_ma(self):
        prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        ma = self.charting.compute_ma(prices, 3)
        self.assertEqual(ma[:2], [None, None])
        self.assertIsNotNone(ma[2])

    def test_compute_ema(self):
        prices = [1.0, 2.0, 3.0, 4.0, 5.0]
        ema = self.charting.compute_ema(prices, 3)
        self.assertEqual(ema[:2], [None, None])
        self.assertIsNotNone(ema[2])

    def test_compute_rsi(self):
        prices = [44.0, 44.5, 45.0, 44.8, 44.2, 43.5, 44.0, 44.5, 45.5, 46.0, 47.0, 48.0, 47.5, 48.0]
        rsi = self.charting.compute_rsi(prices, 14)
        self.assertTrue(len(rsi) == len(prices))
        for r in rsi:
            if r is not None:
                self.assertGreaterEqual(r, 0.0)
                self.assertLessEqual(r, 100.0)

    def test_compute_macd(self):
        prices = [100.0 + i for i in range(50)]
        macd, signal, hist = self.charting.compute_macd(prices)
        self.assertEqual(len(macd), len(prices))
        self.assertEqual(len(signal), len(prices))
        self.assertEqual(len(hist), len(prices))

    def test_compute_bollinger_bands(self):
        prices = [100.0 + i for i in range(30)]
        upper, middle, lower = self.charting.compute_bollinger_bands(prices, 20, 2.0)
        self.assertEqual(len(upper), len(prices))
        self.assertEqual(len(lower), len(prices))

    def test_compute_atr(self):
        highs = [110.0, 112.0, 111.0, 113.0]
        lows = [98.0, 99.0, 97.0, 100.0]
        closes = [105.0, 108.0, 106.0, 109.0]
        atr = self.charting.compute_atr(highs, lows, closes, 14)
        self.assertEqual(len(atr), len(highs))

    def test_snapshot(self):
        chart = self.charting.create_chart("user1", "AAPL", ChartType.KLINE)
        snap = self.charting.save_snapshot(chart.chart_id, "user1", image_data="base64data", description="test")
        self.assertTrue(snap.snapshot_id.startswith("snap:"))

    def test_comparison(self):
        comp = self.charting.create_comparison("user1", ["AAPL", "MSFT"], ChartType.LINE, normalized=True)
        self.assertTrue(comp.comparison_id.startswith("comp:"))
        self.assertEqual(len(comp.instrument_ids), 2)


# ─────────────────────────────────────────────────────────────────────────────
# AI Assistant Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestAIAssistantService(unittest.TestCase):
    def setUp(self):
        self.ai = AIAssistantService()

    def test_detect_intent_strategy(self):
        intent = self.ai.detect_intent("Write a mean reversion strategy")
        self.assertEqual(intent, AIIntent.STRATEGY_DRAFT)

    def test_detect_intent_recommendation(self):
        intent = self.ai.detect_intent("Should I buy AAPL now?")
        self.assertEqual(intent, AIIntent.RECOMMENDATION)

    def test_detect_intent_risk(self):
        intent = self.ai.detect_intent("What's my portfolio risk?")
        self.assertEqual(intent, AIIntent.RISK_ADVISORY)

    def test_create_conversation(self):
        conv_id = self.ai.create_conversation("user1")
        self.assertTrue(conv_id.startswith("conv:"))

    def test_add_turn(self):
        conv_id = self.ai.create_conversation("user1")
        turn = self.ai.add_turn(conv_id, "user1", "Write a momentum strategy")
        self.assertIsNotNone(turn)
        self.assertEqual(turn.intent, AIIntent.STRATEGY_DRAFT)

    def test_draft_strategy(self):
        draft = self.ai.draft_strategy("user1", "A mean reversion strategy on SPY using RSI", language="python")
        self.assertTrue(draft.draft_id.startswith("draft:"))
        self.assertIn("RSI", draft.indicators)
        self.assertIn("SPY", draft.instruments)

    def test_explain_topic(self):
        explanation = self.ai.explain_topic("user1", "RSI indicator")
        self.assertTrue(explanation.explanation_id.startswith("expl:"))
        self.assertIn("RSI", explanation.content)

    def test_generate_recommendation(self):
        rec = self.ai.generate_recommendation(
            "user1", "AAPL", RecommendationAction.BUY,
            reasoning="Oversold RSI", entry_price=150.0,
            target_price=160.0, stop_loss=145.0,
        )
        self.assertTrue(rec.recommendation_id.startswith("rec:"))
        self.assertEqual(rec.action, RecommendationAction.BUY)

    def test_generate_risk_advisory(self):
        advisory = self.ai.generate_risk_advisory(
            "user1", "port1", RiskLevel.MEDIUM,
            var_pct=5.0, max_drawdown_pct=10.0,
            concentration_risk={"AAPL": 0.3},
        )
        self.assertTrue(advisory.advisory_id.startswith("adv:"))
        self.assertEqual(advisory.risk_level, RiskLevel.MEDIUM)

    def test_knowledge_graph(self):
        entry = self.ai.add_knowledge_entry(
            entity_type="indicator",
            name="RSI",
            description="Relative Strength Index",
        )
        self.assertTrue(entry.entity_id.startswith("kg:"))
        results = self.ai.search_knowledge("RSI")
        self.assertTrue(len(results) > 0)

    def test_user_config(self):
        config = self.ai.get_user_config("user1")
        self.assertEqual(config.default_model, "quant-gpt-4")
        updated = self.ai.update_user_config("user1", temperature=0.5)
        self.assertEqual(updated.temperature, 0.5)


# ─────────────────────────────────────────────────────────────────────────────
# Smart Screener Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestSmartScreenerService(unittest.TestCase):
    def setUp(self):
        self.screener = SmartScreenerService()

    def test_parse_natural_query_rsi(self):
        screener = self.screener.parse_natural_query("Find stocks with RSI above 70")
        self.assertTrue(screener.screener_id.startswith("scr:"))
        self.assertTrue(len(screener.conditions) >= 1)

    def test_parse_natural_query_pattern(self):
        screener = self.screener.parse_natural_query("Find stocks with a double bottom pattern")
        self.assertTrue(len(screener.pattern_filters) >= 1)

    def test_create_screener(self):
        conditions = [
            FactorCondition(factor="rsi", operator=ScreenDirection.BELOW, value=30),
            FactorCondition(factor="volume_ratio", operator=ScreenDirection.ABOVE, value=2.0),
        ]
        screener = self.screener.create_screener(
            "user1", "Oversold High Volume", conditions
        )
        self.assertEqual(len(screener.conditions), 2)
        self.assertEqual(screener.limit, 50)

    def test_run_screener(self):
        screener = self.screener.create_screener(
            "user1", "Test Screener",
            conditions=[FactorCondition(factor="rsi", operator=ScreenDirection.BELOW, value=50)],
        )
        results = self.screener.run_screener(screener.screener_id)
        self.assertIsInstance(results, list)

    def test_add_pattern(self):
        pattern = self.screener.add_pattern(
            "AAPL", PatternType.DOUBLE_BOTTOM, 0.85,
            start_time=datetime.now(timezone.utc).isoformat(),
            end_time=datetime.now(timezone.utc).isoformat(),
            price_start=100.0, price_end=100.0,
            breakout_direction="bullish",
        )
        self.assertTrue(pattern.match_id.startswith("pat:"))

    def test_watchlist(self):
        watchlist = self.screener.create_watchlist("user1", "My Watchlist")
        self.assertTrue(watchlist["watchlist_id"].startswith("wl:"))
        result = self.screener.add_to_watchlist(watchlist["watchlist_id"], "AAPL")
        self.assertTrue(result)


# ─────────────────────────────────────────────────────────────────────────────
# DSL Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestDSLService(unittest.TestCase):
    def setUp(self):
        self.dsl = DSLService()

    def test_compile_simple_expression(self):
        result = self.dsl.compile("sma(close, 20)", "SMA20")
        self.assertTrue(result.success)
        self.assertIsNotNone(result.compiled)

    def test_compile_strategy(self):
        code = """
entry long when sma(close, 20) > sma(close, 50)
exit when sma(close, 20) < sma(close, 50)
"""
        result = self.dsl.compile(code, "MA Crossover")
        self.assertTrue(result.success)

    def test_evaluate_expression(self):
        result = self.dsl.evaluate("2 + 3 * 4")
        self.assertTrue(result.success)
        self.assertEqual(result.output, 14.0)

    def test_evaluate_with_data(self):
        result = self.dsl.evaluate("price * 2", {"price": 5.0})
        self.assertTrue(result.success)
        self.assertEqual(result.output, 10.0)

    def test_compile_arithmetic(self):
        result = self.dsl.compile("ma(close, 20) + ema(close, 12)", "Combined")
        self.assertTrue(result.success)

    def test_compile_comparison(self):
        result = self.dsl.compile("rsi(close, 14) > 70", "RSI Overbought")
        self.assertTrue(result.success)

    def test_factor_creation(self):
        factor = self.dsl.create_factor("RSI_Score", "rsi(close, 14)", "RSI factor", tags=["momentum"])
        self.assertTrue(factor.factor_id.startswith("fac:"))
        self.assertEqual(factor.name, "RSI_Score")

    def test_factor_universe(self):
        factor1 = self.dsl.create_factor("Factor1", "sma(close, 20)", "Factor 1")
        factor2 = self.dsl.create_factor("Factor2", "rsi(close, 14)", "Factor 2")
        universe = self.dsl.create_universe(
            "Test Universe",
            [factor1.factor_id, factor2.factor_id],
            weights={factor1.factor_id: 0.6, factor2.factor_id: 0.4},
        )
        self.assertTrue(universe.universe_id.startswith("univ:"))
        self.assertEqual(len(universe.factors), 2)

    def test_backtest_strategy(self):
        code = "entry long when sma(close, 20) > sma(close, 50)"
        result = self.dsl.compile(code, "MA Cross")
        self.assertTrue(result.success)
        bt = self.dsl.backtest_strategy(result.compiled.strategy_id, "2023-01-01", "2023-12-31")
        self.assertIn("total_return_pct", bt)


# ─────────────────────────────────────────────────────────────────────────────
# Visual Editor Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestVisualEditorService(unittest.TestCase):
    def setUp(self):
        self.editor = VisualEditorService()

    def test_create_canvas(self):
        canvas = self.editor.create_canvas("user1", "My Strategy")
        self.assertTrue(canvas.canvas_id.startswith("canvas:"))
        self.assertEqual(canvas.name, "My Strategy")

    def test_add_node(self):
        canvas = self.editor.create_canvas("user1", "Test")
        node = self.editor.add_node(canvas.canvas_id, NodeType.MA, (100, 200), parameters={"period": 20})
        self.assertIsNotNone(node)
        self.assertEqual(node.node_type, NodeType.MA)
        self.assertEqual(len(canvas.nodes), 1)

    def test_add_multiple_nodes(self):
        canvas = self.editor.create_canvas("user1", "Test")
        self.editor.add_node(canvas.canvas_id, NodeType.PRICE_DATA, (0, 0))
        self.editor.add_node(canvas.canvas_id, NodeType.MA, (200, 100), parameters={"period": 20})
        self.editor.add_node(canvas.canvas_id, NodeType.RSI, (400, 100), parameters={"period": 14})
        self.assertEqual(len(canvas.nodes), 3)

    def test_add_connection(self):
        canvas = self.editor.create_canvas("user1", "Test")
        price_node = self.editor.add_node(canvas.canvas_id, NodeType.PRICE_DATA, (0, 0))
        ma_node = self.editor.add_node(canvas.canvas_id, NodeType.MA, (200, 100), parameters={"period": 20})

        conn = self.editor.add_connection(
            canvas.canvas_id,
            price_node.node_id, "output_price",
            ma_node.node_id, "input_price",
        )
        self.assertIsNotNone(conn)

    def test_validate_canvas_clean(self):
        canvas = self.editor.create_canvas("user1", "Test")
        price_node = self.editor.add_node(canvas.canvas_id, NodeType.PRICE_DATA, (0, 0))
        ma_node = self.editor.add_node(canvas.canvas_id, NodeType.MA, (200, 100), parameters={"period": 20})
        self.editor.add_connection(canvas.canvas_id, price_node.node_id, "output_price", ma_node.node_id, "input_price")
        # Also add BUY_SIGNAL connected to MA output
        buy_node = self.editor.add_node(canvas.canvas_id, NodeType.BUY_SIGNAL, (400, 100))
        self.editor.add_connection(canvas.canvas_id, ma_node.node_id, "output_ma", buy_node.node_id, "input_condition")

        result = self.editor.validate_canvas(canvas.canvas_id)
        self.assertTrue(result.is_valid)

    def test_export_to_python(self):
        canvas = self.editor.create_canvas("user1", "MAStrategy")
        self.editor.add_node(canvas.canvas_id, NodeType.MA, (100, 100), parameters={"period": 20})
        code = self.editor.export_to_python(canvas.canvas_id)
        self.assertIsNotNone(code)
        self.assertIn("class", code)

    def test_export_to_quant_script(self):
        canvas = self.editor.create_canvas("user1", "MAStrategy")
        self.editor.add_node(canvas.canvas_id, NodeType.PRICE_DATA, (100, 100))
        code = self.editor.export_to_quant_script(canvas.canvas_id)
        self.assertIsNotNone(code)
        self.assertIn("ref", code)

    def test_create_from_template(self):
        canvas = self.editor.create_canvas("user1", "Template Test")
        self.editor.create_from_template(canvas.canvas_id, "rsi_strategy")
        self.assertTrue(len(canvas.nodes) >= 2)


# ─────────────────────────────────────────────────────────────────────────────
# Copy Trading Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestCopyTradingService(unittest.TestCase):
    def setUp(self):
        self.copy = CopyTradingService()

    def test_register_provider(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL", "MSFT"])
        self.assertTrue(provider.provider_id.startswith("prov:"))
        self.assertEqual(provider.follower_count, 0)

    def test_start_copying(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL"])
        copy = self.copy.start_copying("follower1", provider.provider_id, CopyMode.PROPORTIONAL, 1000.0)
        self.assertIsNotNone(copy)
        self.assertEqual(copy.status, CopyStatus.ACTIVE)
        self.assertEqual(provider.follower_count, 1)

    def test_pause_copying(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL"])
        copy = self.copy.start_copying("follower1", provider.provider_id, CopyMode.PROPORTIONAL, 1000.0)
        paused = self.copy.pause_copying(copy.copy_id)
        self.assertEqual(paused.status, CopyStatus.PAUSED)

    def test_stop_copying(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL"])
        copy = self.copy.start_copying("follower1", provider.provider_id, CopyMode.PROPORTIONAL, 1000.0)
        stopped = self.copy.stop_copying(copy.copy_id)
        self.assertEqual(stopped.status, CopyStatus.STOPPED)
        self.assertIsNotNone(stopped.stopped_at)
        self.assertEqual(provider.follower_count, 0)

    def test_emit_signal(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL"])
        signal = self.copy.emit_signal(
            provider.provider_id, "AAPL", SignalType.ENTRY_LONG,
            quantity=100.0, price=150.0, stop_loss=145.0,
        )
        self.assertTrue(signal.signal_id.startswith("sig:"))
        self.assertEqual(signal.signal_type, SignalType.ENTRY_LONG)

    def test_execute_signal_for_copy(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL"])
        copy = self.copy.start_copying("follower1", provider.provider_id, CopyMode.PROPORTIONAL, 1000.0)
        signal = self.copy.emit_signal(provider.provider_id, "AAPL", SignalType.ENTRY_LONG, quantity=10.0, price=150.0)
        position = self.copy.execute_signal_for_copy(copy.copy_id, signal)
        self.assertIsNotNone(position)
        self.assertEqual(position.instrument_id, "AAPL")

    def test_deviation_alert(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL"])
        copy = self.copy.start_copying("follower1", provider.provider_id, CopyMode.PROPORTIONAL, 1000.0)
        copy.unrealized_pnl = -200.0  # -20%
        alerts = self.copy.check_deviations(copy.copy_id)
        self.assertTrue(len(alerts) > 0)

    def test_leaderboard(self):
        provider1 = self.copy.register_provider("trader1", "Trader One", instruments=["AAPL"])
        self.copy.update_provider_stats(provider1.provider_id, total_return_pct=15.0, sharpe_ratio=1.5)
        provider2 = self.copy.register_provider("trader2", "Trader Two", instruments=["AAPL"])
        self.copy.update_provider_stats(provider2.provider_id, total_return_pct=25.0, sharpe_ratio=2.0)
        leaderboard = self.copy.get_leaderboard(limit=10)
        self.assertEqual(len(leaderboard), 2)
        self.assertEqual(leaderboard[0].name, "Trader Two")  # Higher return

    def test_sync_position(self):
        provider = self.copy.register_provider("trader1", "Top Trader", instruments=["AAPL"])
        copy = self.copy.start_copying("follower1", provider.provider_id, CopyMode.PROPORTIONAL, 1000.0)
        signal = self.copy.emit_signal(provider.provider_id, "AAPL", SignalType.ENTRY_LONG, quantity=10.0, price=150.0)
        position = self.copy.execute_signal_for_copy(copy.copy_id, signal)
        self.copy.sync_position(position.position_id, 155.0)
        self.assertGreater(position.unrealized_pnl, 0)


# ─────────────────────────────────────────────────────────────────────────────
# Information Sources Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestInformationService(unittest.TestCase):
    def setUp(self):
        self.info = InformationService()

    def test_get_all_sources(self):
        sources = self.info.get_all_sources()
        self.assertTrue(len(sources) >= 8)  # At least 8 default sources

    def test_get_sources_by_type(self):
        news_sources = self.info.get_sources_by_type(SourceType.NEWS)
        self.assertTrue(len(news_sources) >= 2)
        social_sources = self.info.get_sources_by_type(SourceType.SOCIAL)
        self.assertTrue(len(social_sources) >= 2)

    def test_connect_source(self):
        result = self.info.connect_source("reuters")
        self.assertTrue(result)
        self.assertTrue(self.info._adapters["reuters"].is_connected())

    def test_fetch_news(self):
        self.info.connect_source("reuters")
        self.info.connect_source("bloomberg")
        articles = self.info.fetch_news(instruments=["AAPL"], limit=10)
        self.assertIsInstance(articles, list)

    def test_fetch_social_sentiment(self):
        self.info.connect_source("twitter")
        self.info.connect_source("reddit")
        posts = self.info.fetch_social_sentiment(instruments=["AAPL"], limit=10)
        self.assertIsInstance(posts, list)

    def test_get_news_sentiment(self):
        self.info.connect_source("reuters")
        self.info.fetch_news(instruments=["AAPL"], limit=10)
        sentiment = self.info.get_news_sentiment("AAPL")
        self.assertIsNotNone(sentiment)
        self.assertIsInstance(sentiment, SentimentSummary)
        self.assertIn(sentiment.source, ["news", "social"])

    def test_get_social_sentiment(self):
        self.info.connect_source("twitter")
        self.info.fetch_social_sentiment(instruments=["AAPL"], limit=10)
        sentiment = self.info.get_social_sentiment("AAPL")
        self.assertIsNotNone(sentiment)

    def test_fetch_filings(self):
        self.info.connect_source("edgar")
        filings = self.info.fetch_filings(instruments=["AAPL", "MSFT"], limit=10)
        self.assertIsInstance(filings, list)

    def test_fetch_economic_events(self):
        self.info.connect_source("econ_calendar")
        events = self.info.fetch_economic_events(country="US", limit=10)
        self.assertTrue(len(events) > 0)
        self.assertTrue(all(isinstance(e, InfoEconomicEvent) for e in events))

    def test_fetch_research_reports(self):
        self.info.connect_source("research")
        reports = self.info.fetch_research_reports(instruments=["AAPL"], limit=5)
        self.assertIsInstance(reports, list)

    def test_enable_disable_source(self):
        source = self.info.disable_source("reuters")
        self.assertFalse(source.is_enabled)
        source = self.info.enable_source("reuters")
        self.assertTrue(source.is_enabled)


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio Allocator Tests
# ─────────────────────────────────────────────────────────────────────────────

class TestPortfolioAllocatorService(unittest.TestCase):
    def setUp(self):
        self.allocator = PortfolioAllocatorService()

    def test_calculate_allocation_equal_weight(self):
        config = AllocatorConfig(
            config_id="alloc-1",
            name="Equal Weight Allocator",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
        )
        expected_returns = {"AAPL": 0.12, "MSFT": 0.10, "GOOGL": 0.08}
        volatilities = {"AAPL": 0.20, "MSFT": 0.15, "GOOGL": 0.18}
        result = self.allocator.calculate_allocation(config, expected_returns, volatilities)
        self.assertIsInstance(result, AllocationResult)
        self.assertEqual(len(result.weights), 3)

    def test_calculate_allocation_risk_parity(self):
        config = AllocatorConfig(
            config_id="alloc-2",
            name="Risk Parity Allocator",
            allocator_type=AllocatorType.RISK_PARITY,
        )
        expected_returns = {"AAPL": 0.12, "TLT": 0.04, "GLD": 0.06}
        volatilities = {"AAPL": 0.25, "TLT": 0.08, "GLD": 0.12}
        result = self.allocator.calculate_allocation(config, expected_returns, volatilities)
        self.assertIsInstance(result, AllocationResult)
        self.assertTrue(len(result.weights) == 3)

    def test_calculate_allocation_black_litterman(self):
        config = AllocatorConfig(
            config_id="alloc-3",
            name="Black-Litterman Allocator",
            allocator_type=AllocatorType.BLACK_LITTERMAN,
            equilibrium_returns={"AAPL": 0.10, "MSFT": 0.08},
            views={"AAPL": 0.15, "MSFT": 0.06},
            view_confidence=0.6,
        )
        expected_returns = {"AAPL": 0.10, "MSFT": 0.08}
        volatilities = {"AAPL": 0.20, "MSFT": 0.18}
        result = self.allocator.calculate_allocation(config, expected_returns, volatilities)
        self.assertIsInstance(result, AllocationResult)

    def test_calculate_allocation_equal_risk(self):
        config = AllocatorConfig(
            config_id="alloc-4",
            name="Equal Risk Allocator",
            allocator_type=AllocatorType.EQUAL_RISK,
        )
        expected_returns = {"AAPL": 0.12, "MSFT": 0.10, "GOOGL": 0.08}
        volatilities = {"AAPL": 0.20, "MSFT": 0.15, "GOOGL": 0.18}
        result = self.allocator.calculate_allocation(config, expected_returns, volatilities)
        self.assertIsInstance(result, AllocationResult)
        self.assertTrue(len(result.weights) == 3)

    def test_calculate_allocation_min_variance(self):
        config = AllocatorConfig(
            config_id="alloc-5",
            name="Min Variance Allocator",
            allocator_type=AllocatorType.MIN_VARIANCE,
        )
        expected_returns = {"AAPL": 0.12, "MSFT": 0.10}
        volatilities = {"AAPL": 0.25, "MSFT": 0.15}
        correlations = {("AAPL", "MSFT"): 0.3, ("MSFT", "AAPL"): 0.3}
        result = self.allocator.calculate_allocation(
            config, expected_returns, volatilities, correlations
        )
        self.assertIsInstance(result, AllocationResult)

    def test_calculate_allocation_with_constraints(self):
        config = AllocatorConfig(
            config_id="alloc-6",
            name="Constrained Allocator",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            max_weight=0.5,
            min_weight=0.1,
        )
        expected_returns = {"AAPL": 0.12, "MSFT": 0.10, "GOOGL": 0.08}
        volatilities = {"AAPL": 0.20, "MSFT": 0.15, "GOOGL": 0.18}
        result = self.allocator.calculate_allocation(config, expected_returns, volatilities)
        self.assertIsInstance(result, AllocationResult)

    def test_check_rebalance_needed(self):
        config = self.allocator.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Rebalance Test Allocator",
            drift_threshold=0.05,
        )
        allocation = PortfolioAllocation(
            allocation_id="pa-rebal-1",
            user_id="user1",
            portfolio_id="pf-1",
            allocator_config=config,
            target_weights={"AAPL": 0.3, "MSFT": 0.4},
            current_weights={"AAPL": 0.5, "MSFT": 0.2},
            drift={"AAPL": 0.2, "MSFT": 0.2},
            needs_rebalance=True,
        )
        needed = self.allocator.check_rebalance_needed(
            allocation, current_weights={"AAPL": 0.5, "MSFT": 0.2}
        )
        self.assertIsInstance(needed, bool)

    def test_calculate_rebalance_plan(self):
        plan = self.allocator.calculate_rebalance_plan(
            portfolio_id="pf-1",
            target_weights={"AAPL": 0.3, "MSFT": 0.4, "GOOGL": 0.3},
            current_weights={"AAPL": 0.5, "MSFT": 0.2, "GOOGL": 0.3},
            current_prices={"AAPL": 150.0, "MSFT": 300.0, "GOOGL": 140.0},
            notional=100000.0,
        )
        self.assertIsInstance(plan, RebalancePlan)

    def test_create_allocator(self):
        created = self.allocator.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.EQUAL_WEIGHT,
            name="Test Allocator",
        )
        self.assertTrue(created.config_id.startswith("alloc:"))
        self.assertEqual(created.name, "Test Allocator")

    def test_get_allocator(self):
        created = self.allocator.create_allocator(
            user_id="user1",
            allocator_type=AllocatorType.RISK_PARITY,
            name="Get Test Allocator",
        )
        retrieved = self.allocator.get_allocator(created.config_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.config_id, created.config_id)


class TestRiskExposureAggregator(unittest.TestCase):
    """Tests for PF-04: Inter-Strategy Risk Exposure Aggregation."""

    def setUp(self):
        self.aggregator = RiskExposureAggregator()

    def test_aggregate_exposures(self):
        self.aggregator.register_instrument_sector("AAPL", "Technology")
        self.aggregator.register_instrument_sector("MSFT", "Technology")
        self.aggregator.register_instrument_sector("JPM", "Financials")

        self.aggregator.record_strategy_position(
            "strategy1",
            {"AAPL": 100, "MSFT": 50},
            cash=10000.0,
        )
        self.aggregator.record_strategy_position(
            "strategy2",
            {"JPM": 200, "MSFT": 25},
            cash=15000.0,
        )

        prices = {"AAPL": 150.0, "MSFT": 300.0, "JPM": 120.0}
        result = self.aggregator.aggregate_exposures(prices)

        self.assertIsInstance(result, AggregatedExposure)
        self.assertEqual(result.total_gross_exposure, 100 * 150.0 + 50 * 300.0 + 200 * 120.0 + 25 * 300.0)
        self.assertIn("strategy1", result.strategy_exposures)
        self.assertIn("strategy2", result.strategy_exposures)

    def test_strategy_risk_contribution(self):
        self.aggregator.record_strategy_position("strat1", {"AAPL": 100}, cash=10000.0)
        self.aggregator.record_strategy_position("strat2", {"MSFT": 50}, cash=10000.0)

        prices = {"AAPL": 150.0, "MSFT": 300.0}
        result = self.aggregator.aggregate_exposures(prices)

        contributions = self.aggregator.get_strategy_risk_contribution(result)
        self.assertIsInstance(contributions, dict)
        self.assertEqual(len(contributions), 2)


class TestAttributionAnalyzer(unittest.TestCase):
    """Tests for PF-05: Return Attribution Analysis."""

    def setUp(self):
        self.analyzer = AttributionAnalyzer()

    def test_brinson_attribution(self):
        portfolio_weights = {"AAPL": 0.4, "MSFT": 0.3, "GOOGL": 0.3}
        benchmark_weights = {"AAPL": 0.35, "MSFT": 0.35, "GOOGL": 0.3}
        portfolio_returns = {"AAPL": 0.12, "MSFT": 0.08, "GOOGL": 0.10}
        benchmark_returns = {"AAPL": 0.10, "MSFT": 0.09, "GOOGL": 0.08}

        result = self.analyzer.brinson_attribution(
            portfolio_weights, benchmark_weights,
            portfolio_returns, benchmark_returns,
        )

        self.assertIsInstance(result, AttributionResult)
        self.assertIsNotNone(result.allocation_effect)
        self.assertIsNotNone(result.selection_effect)
        self.assertIsNotNone(result.interaction_effect)

    def test_nav_attribution(self):
        positions = {"AAPL": 100, "MSFT": 50}
        prices_start = {"AAPL": 145.0, "MSFT": 290.0}
        prices_end = {"AAPL": 155.0, "MSFT": 310.0}
        benchmark_return = 0.05

        result = self.analyzer.calculate_nav_attribution(
            positions, prices_start, prices_end, benchmark_return,
        )

        self.assertIsInstance(result, AttributionResult)
        self.assertIn("AAPL", result.return_contributions)
        self.assertIn("MSFT", result.return_contributions)

    def test_top_contributors(self):
        contributions = {"AAPL": 0.05, "MSFT": -0.02, "GOOGL": 0.03, "JPM": 0.01}
        top, bottom = self.analyzer.get_top_contributors(contributions, n=2)

        self.assertEqual(len(top), 2)
        self.assertEqual(top[0][0], "AAPL")  # Highest contributor
        # Bottom 2 are JPM (0.01) and MSFT (-0.02) - MSFT is the lowest
        self.assertEqual(bottom[1][0], "MSFT")  # Lowest contributor


class TestMultiAccountAllocator(unittest.TestCase):
    """Tests for PF-06: Multi-Account Capital Allocation."""

    def setUp(self):
        self.allocator = MultiAccountAllocator()

    def test_create_account(self):
        account = self.allocator.create_account(
            user_id="user1",
            account_type="primary",
            initial_cash=100000.0,
        )
        self.assertIsInstance(account, Account)
        self.assertEqual(account.cash_balance, 100000.0)
        self.assertEqual(account.account_type, "primary")

    def test_transfer_funds(self):
        acc1 = self.allocator.create_account("user1", initial_cash=100000.0)
        acc2 = self.allocator.create_account("user1", initial_cash=50000.0)

        transfer = self.allocator.transfer_funds(
            acc1.account_id, acc2.account_id, 25000.0,
        )

        self.assertIsInstance(transfer, TransferRequest)
        self.assertEqual(transfer.status, "completed")
        self.assertEqual(self.allocator.get_account(acc1.account_id).cash_balance, 75000.0)
        self.assertEqual(self.allocator.get_account(acc2.account_id).cash_balance, 75000.0)

    def test_child_accounts(self):
        parent = self.allocator.create_account("user1", "primary")
        child1 = self.allocator.create_account("user1", "sub", parent_account_id=parent.account_id)
        child2 = self.allocator.create_account("user1", "sub", parent_account_id=parent.account_id)

        children = self.allocator.get_child_accounts(parent.account_id)
        self.assertEqual(len(children), 2)

    def test_allocation_plan(self):
        acc1 = self.allocator.create_account("user1", initial_cash=100000.0)
        acc2 = self.allocator.create_account("user1", initial_cash=100000.0)

        plan = self.allocator.create_allocation_plan(
            user_id="user1",
            total_capital=200000.0,
            account_weights={acc1.account_id: 0.6, acc2.account_id: 0.4},
        )

        self.assertIsInstance(plan, CapitalAllocationPlan)
        self.assertEqual(plan.account_allocations[acc1.account_id], 120000.0)
        self.assertEqual(plan.account_allocations[acc2.account_id], 80000.0)

    def test_rebalance_needed(self):
        acc1 = self.allocator.create_account("user1", initial_cash=100000.0)
        acc2 = self.allocator.create_account("user1", initial_cash=100000.0)

        plan = self.allocator.create_allocation_plan(
            user_id="user1",
            total_capital=200000.0,
            account_weights={acc1.account_id: 0.5, acc2.account_id: 0.5},
            rebalance_threshold=0.05,
        )

        # Acc1 grew to 120k, acc2 stayed at 100k - needs rebalance
        current_capitals = {acc1.account_id: 120000.0, acc2.account_id: 100000.0}
        needed, accounts = self.allocator.check_rebalance_needed(plan.plan_id, current_capitals)

        self.assertTrue(needed)
        self.assertIn(acc1.account_id, accounts)


class TestWebhookService(unittest.TestCase):
    """Tests for HOOK-01, HOOK-02, HOOK-03: Webhook management."""

    def setUp(self):
        from quant_exchange.enhanced.automation import WebhookService, WebhookDirection, WebhookMethod
        self.webhook = WebhookService()
        self.WebhookDirection = WebhookDirection
        self.WebhookMethod = WebhookMethod

    def test_register_endpoint(self):
        endpoint = self.webhook.register_endpoint(
            name="Test Endpoint",
            url="https://example.com/webhook",
            direction=self.WebhookDirection.OUTBOUND,
            event_types=("order.filled", "position.closed"),
        )
        self.assertTrue(endpoint.endpoint_id.startswith("wh_"))
        self.assertEqual(endpoint.name, "Test Endpoint")
        self.assertEqual(len(endpoint.secret), 32)

    def test_get_endpoint(self):
        endpoint = self.webhook.register_endpoint(
            name="Get Test",
            url="https://example.com/get",
            direction=self.WebhookDirection.OUTBOUND,
        )
        retrieved = self.webhook.get_endpoint(endpoint.endpoint_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "Get Test")

    def test_generate_signature(self):
        endpoint = self.webhook.register_endpoint(
            name="Sig Test",
            url="https://example.com/sig",
            direction=self.WebhookDirection.OUTBOUND,
        )
        sig = self.webhook.generate_signature(endpoint.endpoint_id, "test payload")
        self.assertTrue(sig.valid)
        self.assertEqual(sig.algorithm, "sha256")

    def test_validate_signature(self):
        endpoint = self.webhook.register_endpoint(
            name="Validate Test",
            url="https://example.com/validate",
            direction=self.WebhookDirection.INBOUND,
        )
        payload = "test payload"
        sig = self.webhook.generate_signature(endpoint.endpoint_id, payload)
        result = self.webhook.validate_signature(endpoint.endpoint_id, payload, sig.provided_signature)
        self.assertTrue(result.valid)

    def test_record_delivery(self):
        endpoint = self.webhook.register_endpoint(
            name="Delivery Test",
            url="https://example.com/delivery",
            direction=self.WebhookDirection.OUTBOUND,
        )
        delivery = self.webhook.record_delivery(
            endpoint_id=endpoint.endpoint_id,
            event_type="order.filled",
            payload={"order_id": "123", "status": "filled"},
        )
        self.assertTrue(delivery.delivery_id.startswith("del_"))
        self.assertEqual(delivery.status, "pending")

    def test_list_endpoints(self):
        self.webhook.register_endpoint("List 1", "https://example.com/1", self.WebhookDirection.OUTBOUND)
        self.webhook.register_endpoint("List 2", "https://example.com/2", self.WebhookDirection.OUTBOUND)
        self.webhook.register_endpoint("List 3", "https://example.com/3", self.WebhookDirection.INBOUND)

        all_endpoints = self.webhook.list_endpoints()
        self.assertEqual(len(all_endpoints), 3)

        outbound_only = self.webhook.list_endpoints(direction=self.WebhookDirection.OUTBOUND)
        self.assertEqual(len(outbound_only), 2)

    def test_rotate_secret(self):
        endpoint = self.webhook.register_endpoint(
            name="Rotate Test",
            url="https://example.com/rotate",
            direction=self.WebhookDirection.OUTBOUND,
        )
        old_secret = endpoint.secret
        new_secret = self.webhook.rotate_secret(endpoint.endpoint_id)
        self.assertIsNotNone(new_secret)
        self.assertNotEqual(new_secret, old_secret)


class TestAutomationWorkflowService(unittest.TestCase):
    """Tests for HOOK-04, HOOK-05: Workflow automation."""

    def setUp(self):
        from quant_exchange.enhanced.automation import (
            AutomationWorkflowService, WorkflowTrigger, WorkflowAction,
            WorkflowCondition, TriggerType,
        )
        self.workflow = AutomationWorkflowService()
        self.WorkflowTrigger = WorkflowTrigger
        self.WorkflowAction = WorkflowAction
        self.WorkflowCondition = WorkflowCondition
        self.TriggerType = TriggerType

    def test_create_workflow(self):
        trigger = self.WorkflowTrigger(trigger_type=self.TriggerType.MANUAL)
        action = self.WorkflowAction(action_type="log", params={"message": "Test"})
        workflow = self.workflow.create_workflow(
            name="Test Workflow",
            trigger=trigger,
            actions=(action,),
            description="A test workflow",
        )
        self.assertTrue(workflow.workflow_id.startswith("wf_"))
        self.assertEqual(workflow.name, "Test Workflow")
        self.assertEqual(workflow.version, 1)

    def test_get_workflow(self):
        trigger = self.WorkflowTrigger(trigger_type=self.TriggerType.MANUAL)
        workflow = self.workflow.create_workflow(
            name="Get Test",
            trigger=trigger,
            actions=(self.WorkflowAction(action_type="log", params={}),),
        )
        retrieved = self.workflow.get_workflow(workflow.workflow_id)
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.name, "Get Test")

    def test_evaluate_trigger_conditions(self):
        trigger = self.WorkflowTrigger(
            trigger_type=self.TriggerType.EVENT,
            conditions=(
                self.WorkflowCondition(field="payload.price", operator="gt", value=100),
            ),
        )
        workflow = self.workflow.create_workflow(
            name="Condition Test",
            trigger=trigger,
            actions=(self.WorkflowAction(action_type="log", params={}),),
        )

        # Should pass - price is 150 > 100
        result1 = self.workflow.evaluate_trigger(workflow, {"event_type": "test", "payload": {"price": 150}})
        self.assertTrue(result1)

        # Should fail - price is 50 < 100
        result2 = self.workflow.evaluate_trigger(workflow, {"event_type": "test", "payload": {"price": 50}})
        self.assertFalse(result2)

    def test_execute_workflow(self):
        trigger = self.WorkflowTrigger(trigger_type=self.TriggerType.MANUAL)
        action = self.WorkflowAction(
            action_type="log",
            params={"message": "Order {order_id} filled"},
        )
        workflow = self.workflow.create_workflow(
            name="Execute Test",
            trigger=trigger,
            actions=(action,),
        )

        execution = self.workflow.execute_workflow(
            workflow_id=workflow.workflow_id,
            trigger_payload={"order_id": "123"},
        )

        self.assertTrue(execution.execution_id.startswith("exec_"))
        self.assertEqual(execution.status.value, "completed")

    def test_workflow_import_export(self):
        trigger = self.WorkflowTrigger(trigger_type=self.TriggerType.SCHEDULE)
        action = self.WorkflowAction(
            action_type="notify",
            params={"channel": "email", "message": "Alert"},
        )
        workflow = self.workflow.create_workflow(
            name="Export Test",
            trigger=trigger,
            actions=(action,),
            tags=("test", "export"),
        )

        exported = self.workflow.export_workflow(workflow.workflow_id)
        self.assertIsNotNone(exported)
        self.assertEqual(exported["name"], "Export Test")
        self.assertIn("test", exported["tags"])

        # Import the exported workflow
        imported = self.workflow.import_workflow(exported, user_id="user_x")
        self.assertEqual(imported.name, "Export Test")
        self.assertEqual(imported.user_id, "user_x")

    def test_list_workflows(self):
        trigger = self.WorkflowTrigger(trigger_type=self.TriggerType.MANUAL)
        self.workflow.create_workflow(
            name="List WF 1",
            trigger=trigger,
            actions=(self.WorkflowAction(action_type="log", params={}),),
            user_id="user1",
            tags=("tag1",),
        )
        self.workflow.create_workflow(
            name="List WF 2",
            trigger=trigger,
            actions=(self.WorkflowAction(action_type="log", params={}),),
            user_id="user1",
            tags=("tag1",),
        )
        self.workflow.create_workflow(
            name="List WF 3",
            trigger=trigger,
            actions=(self.WorkflowAction(action_type="log", params={}),),
            user_id="user2",
            tags=("tag2",),
        )

        all_wf = self.workflow.list_workflows()
        self.assertEqual(len(all_wf), 3)

        user1_wf = self.workflow.list_workflows(user_id="user1")
        self.assertEqual(len(user1_wf), 2)

        tag1_wf = self.workflow.list_workflows(tag="tag1")
        self.assertEqual(len(tag1_wf), 2)


class TestMobileService(unittest.TestCase):
    """Tests for MobileService (MOB-01 ~ MOB-05)."""

    def setUp(self):
        from quant_exchange.enhanced import MobileService, NotificationCategory, BiometricType, GestureAction
        self.mobile = MobileService()
        self.NotificationCategory = NotificationCategory
        self.BiometricType = BiometricType
        self.GestureAction = GestureAction

    def test_get_pwa_manifest(self):
        """Verify PWA manifest is returned correctly."""
        manifest = self.mobile.get_pwa_manifest()
        self.assertEqual(manifest.app_name, "QuantExchange")
        self.assertEqual(manifest.display, "standalone")

    def test_get_offline_cache_config(self):
        """Verify offline cache config is returned."""
        config = self.mobile.get_offline_cache_config()
        self.assertTrue(config.enabled)
        self.assertIn("/api/v1/portfolio", config.cached_endpoints)

    def test_service_worker_config(self):
        """Verify service worker config is generated."""
        config = self.mobile.generate_service_worker_config()
        self.assertIn("strategies", config)
        self.assertIn("static", config["strategies"])
        self.assertIn("api", config["strategies"])

    def test_notification_preferences(self):
        """Verify notification preferences management."""
        prefs = self.mobile.get_notification_preferences("user1")
        self.assertEqual(prefs.user_id, "user1")
        self.assertTrue(prefs.enabled)

        updated = self.mobile.update_notification_preferences(
            "user1",
            enabled=False,
            sound_enabled=False,
        )
        self.assertFalse(updated.enabled)
        self.assertFalse(updated.sound_enabled)

    def test_should_send_notification(self):
        """Verify notification sending logic."""
        self.mobile.update_notification_preferences(
            "user1",
            categories=(self.NotificationCategory.TRADE_EXECUTION,),
        )
        # Should send for TRADE_EXECUTION
        self.assertTrue(self.mobile.should_send_notification(
            "user1", self.NotificationCategory.TRADE_EXECUTION
        ))
        # Should not send for NEWS (not in categories)
        self.assertFalse(self.mobile.should_send_notification(
            "user1", self.NotificationCategory.NEWS
        ))

    def test_register_device(self):
        """Verify device registration."""
        session = self.mobile.register_device(
            user_id="user1",
            device_id="device_abc",
            device_name="iPhone 15",
            os_version="iOS 17.0",
            app_version="1.0.0",
            push_token="token_xyz",
        )
        self.assertTrue(session.session_id.startswith("mob_"))
        self.assertEqual(session.user_id, "user1")
        self.assertEqual(session.device_name, "iPhone 15")

    def test_get_user_sessions(self):
        """Verify getting user sessions."""
        self.mobile.register_device("user1", "device1", "Phone1", "iOS16", "1.0.0")
        self.mobile.register_device("user1", "device2", "Phone2", "iOS17", "1.0.0")
        sessions = self.mobile.get_user_sessions("user1")
        self.assertEqual(len(sessions), 2)

    def test_gesture_preferences(self):
        """Verify gesture preferences management."""
        prefs = self.mobile.get_gesture_preferences("user1")
        self.assertIn(self.GestureAction.SWIPE_LEFT_ORDER, prefs.enabled_gestures)

        updated = self.mobile.update_gesture_preferences(
            "user1",
            haptic_feedback=False,
            gesture_sensitivity=1.5,
        )
        self.assertFalse(updated.haptic_feedback)
        self.assertEqual(updated.gesture_sensitivity, 1.5)

    def test_biometric_settings(self):
        """Verify biometric settings management."""
        settings = self.mobile.get_biometric_settings("user1")
        self.assertEqual(settings.biometric_type, self.BiometricType.NONE)
        self.assertFalse(settings.enabled)

        updated = self.mobile.update_biometric_settings(
            "user1",
            biometric_type=self.BiometricType.FACE_ID,
            enabled=True,
            require_for_withdrawal=True,
        )
        self.assertEqual(updated.biometric_type, self.BiometricType.FACE_ID)
        self.assertTrue(updated.enabled)
        self.assertTrue(updated.require_for_withdrawal)

    def test_requires_biometric(self):
        """Verify biometric requirement checks."""
        self.mobile.update_biometric_settings(
            "user1",
            biometric_type=self.BiometricType.TOUCH_ID,
            enabled=True,
            require_for_withdrawal=True,
        )
        # Biometric required for withdrawal
        self.assertTrue(self.mobile.requires_biometric("user1", "withdrawal"))
        # Not required for login (unless configured)
        self.assertFalse(self.mobile.requires_biometric("user1", "login"))


if __name__ == "__main__":
    unittest.main()
