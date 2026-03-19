"""Top-level facade that wires the MVP services into one runtime object."""

from __future__ import annotations

from pathlib import Path

from quant_exchange.adapters import (
    AdapterRegistry,
    SimulatedCryptoExchangeAdapter,
    SimulatedEquityBrokerAdapter,
    SimulatedFuturesBrokerAdapter,
)
from quant_exchange.api.control_plane import ControlPlaneAPI
from quant_exchange.backtest.engine import BacktestEngine
from quant_exchange.bots import StrategyBotService
from quant_exchange.config.settings import AppSettings
from quant_exchange.crypto import CryptoWorkbenchService
from quant_exchange.futures import FuturesWorkbenchService
from quant_exchange.enhanced import (
    AdvancedExecutionService,
    AlternativeDataService,
    BiasAuditService,
    DerivativesDexService,
    FeatureStoreService,
    LedgerService,
    ReplayService,
    ResearchMlService,
    UniverseService,
)
from quant_exchange.execution.oms import OrderManager, PaperExecutionEngine
from quant_exchange.ingestion.background_downloader import HistoryDownloadSupervisor
from quant_exchange.intelligence.service import IntelligenceEngine
from quant_exchange.learning import LearningHubService
from quant_exchange.marketdata.service import MarketDataStore
from quant_exchange.monitoring.service import MonitoringService
from quant_exchange.persistence.database import SQLitePersistence
from quant_exchange.portfolio.service import PortfolioManager
from quant_exchange.reporting.service import ReportingService
from quant_exchange.risk.service import RiskEngine
from quant_exchange.rules.engine import MarketRuleEngine
from quant_exchange.scheduler.service import JobScheduler
from quant_exchange.security.service import SecurityService
from quant_exchange.simulation import SimulatedTradingService
from quant_exchange.stocks import RealtimeMarketService, StockDirectoryService
from quant_exchange.strategy import MovingAverageSentimentStrategy, StrategyRegistry
from quant_exchange.webapp import StockScreenerWebApp
from quant_exchange.webapp.state import WebWorkspaceService


class QuantTradingPlatform:
    """Convenience facade to wire the documented MVP services together."""

    def __init__(self, settings: AppSettings | None = None) -> None:
        self.settings = settings or AppSettings()
        self.persistence = SQLitePersistence(self.settings.database.url)
        self.market_data = MarketDataStore()
        self.intelligence = IntelligenceEngine()
        self._seed_intelligence_data()
        self.learning = LearningHubService()
        self.risk = RiskEngine()
        self.monitoring = MonitoringService()
        self.reporting = ReportingService()
        self.security = SecurityService()
        self.portfolio = PortfolioManager()
        self.oms = OrderManager()
        self.paper_execution = PaperExecutionEngine()
        self.backtest = BacktestEngine()
        self.strategy_registry = StrategyRegistry()
        self.strategy_registry.register(MovingAverageSentimentStrategy(strategy_id="ma_sentiment"))
        self.adapters = AdapterRegistry()
        self.market_rules = MarketRuleEngine()
        self.scheduler = JobScheduler(self.persistence)
        self.universes = UniverseService(self.persistence)
        self.features = FeatureStoreService(self.persistence)
        self.research_ml = ResearchMlService(self.persistence)
        self.bias_audit = BiasAuditService(self.persistence)
        self.replay = ReplayService(self.persistence)
        self.ledger = LedgerService(self.persistence)
        self.alt_data = AlternativeDataService(self.persistence)
        self.advanced_execution = AdvancedExecutionService(self.persistence)
        self.derivatives_dex = DerivativesDexService(self.persistence)
        self.history_downloads = HistoryDownloadSupervisor(self._runtime_dir() / "history_downloads")
        self.stocks = StockDirectoryService(self.persistence, registrar=self.register_instrument)
        self.paper_trading = SimulatedTradingService(
            persistence=self.persistence,
            stock_directory=self.stocks,
            risk_engine=self.risk,
            market_rules=self.market_rules,
            backtest_engine=self.backtest,
            intelligence_engine=self.intelligence,
        )
        self.bot_center = StrategyBotService(self.persistence, self.stocks)
        self.web_workspace = WebWorkspaceService(self.persistence)
        self._register_default_adapters()
        self.crypto = CryptoWorkbenchService(self.adapters, self.market_data)
        self.futures = FuturesWorkbenchService(self.adapters, self.market_data)
        self.stocks.bootstrap_persisted_or_demo_directory()
        self.realtime_market = RealtimeMarketService(self.stocks, persist_minute_bars=False)
        self.paper_trading.realtime_market = self.realtime_market
        self.realtime_market.start()
        self.api = ControlPlaneAPI(
            platform=self,
            persistence=self.persistence,
            adapter_registry=self.adapters,
            scheduler=self.scheduler,
            market_rules=self.market_rules,
        )
        self.web_app = StockScreenerWebApp(self)

    def register_instrument(self, instrument) -> None:
        """Register an instrument across the data and portfolio modules."""

        self.market_data.add_instrument(instrument)
        self.portfolio.register_instrument(instrument)

    def close(self) -> None:
        """Release resources owned by the platform runtime."""

        self.realtime_market.stop()
        self.history_downloads.close()
        self.persistence.close()

    def _register_default_adapters(self) -> None:
        """Register reference adapters for crypto, futures, and equities."""

        for adapter in [
            SimulatedCryptoExchangeAdapter(),
            SimulatedFuturesBrokerAdapter(),
            SimulatedEquityBrokerAdapter(),
        ]:
            self.adapters.register_market_data(adapter.exchange_code(), adapter)
            self.adapters.register_execution(adapter.exchange_code(), adapter)

    def _seed_intelligence_data(self) -> None:
        """Seed demo intelligence documents so the sentiment dashboard has data."""
        from quant_exchange.core.models import MarketDocument
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        demo_docs = [
            MarketDocument(document_id="demo_001", source="newswire", instrument_id="BTCUSDT", published_at=now - timedelta(hours=1), title="Bitcoin Breaks Through $100K Resistance", content="Bitcoin rallied strongly past $100K, bullish momentum continues with institutional inflow.", language="en"),
            MarketDocument(document_id="demo_002", source="exchange_announcement", instrument_id="ETHUSDT", published_at=now - timedelta(hours=2), title="Ethereum升级方案通过", content="以太坊网络升级利好，链上活动增长强劲，看多情绪升温。", language="zh"),
            MarketDocument(document_id="demo_003", source="newswire", instrument_id="SOLUSDT", published_at=now - timedelta(hours=3), title="Solana Network Faces Congestion Issues", content="Solana experienced network congestion and downtime risk, operational stability concerns remain.", language="en"),
            MarketDocument(document_id="demo_004", source="research", instrument_id="600519.SH", published_at=now - timedelta(hours=4), title="茅台营收超预期", content="贵州茅台发布业绩预告，营收增长强劲，利润超预期，买入评级。", language="zh"),
            MarketDocument(document_id="demo_005", source="newswire", instrument_id="IF2506", published_at=now - timedelta(hours=5), title="A股市场情绪回暖", content="沪深300指数期货上涨，市场看多情绪增强，成交量放大。", language="zh"),
            MarketDocument(document_id="demo_006", source="social", instrument_id="DOGEUSDT", published_at=now - timedelta(hours=6), title="DOGE Community Hype on Social Media", content="Dogecoin rally driven by social media hype, retail sentiment bullish but volatile.", language="en"),
            MarketDocument(document_id="demo_007", source="newswire", instrument_id="CL2506", published_at=now - timedelta(hours=7), title="OPEC减产预期推高油价", content="OPEC+成员国讨论进一步减产，原油期货价格上涨预期强烈，看多。", language="zh"),
            MarketDocument(document_id="demo_008", source="research", instrument_id="AU2506", published_at=now - timedelta(hours=8), title="全球避险情绪升温推动金价", content="地缘政治风险加剧，黄金期货作为避险资产获得增长动力。", language="zh"),
        ]
        self.intelligence.ingest_documents(demo_docs)

    def _runtime_dir(self) -> Path:
        """Resolve the runtime directory used by background services."""

        if self.settings.database.url != ":memory:":
            return Path(self.settings.database.url).expanduser().resolve().parent
        return Path(__file__).resolve().parents[2] / "data" / "runtime"
