"""Information source adapters for intelligence module (IN-01 ~ IN-05).

Provides adapters for:
- News feeds (Reuters, Bloomberg, etc.)
- Social media sentiment (Twitter/X, Reddit, StockTwits)
- Alternative data (satellite, credit card, weather)
- Edgar/SEC filings
- Economic calendars
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class SourceType(str, Enum):
    NEWS = "news"
    SOCIAL = "social"
    FILING = "filing"
    ECONOMIC = "economic"
    ALTERNATIVE = "alternative"
    RESEARCH = "research"


class SentimentLabel(str, Enum):
    VERY_BEARISH = "very_bearish"
    BEARISH = "bearish"
    NEUTRAL = "neutral"
    BULLISH = "bullish"
    VERY_BULLISH = "very_bullish"


class NewsCategory(str, Enum):
    EARNINGS = "earnings"
    MACRO = "macro"
    COMMODITIES = "commodities"
    CRYPTO = "crypto"
    FEDERAL_RESERVE = "federal_reserve"
    GEOPOLITICAL = "geopolitical"
    MERGERS = "mergers"
    REGULATORY = "regulatory"
    PRODUCT = "product"
    MANAGEMENT = "management"
    ANALYST = "analyst"
    OTHER = "other"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class NewsArticle:
    """A news article from an information source."""

    article_id: str
    source: str  # e.g., "reuters", "bloomberg", "ap"
    headline: str
    summary: str
    url: str = ""
    category: NewsCategory = NewsCategory.OTHER
    instruments: tuple[str, ...] = field(default_factory=tuple)
    sentiment: SentimentLabel | None = None
    sentiment_score: float | None = None  # -1.0 to 1.0
    relevance_score: float = 0.0  # 0.0 to 1.0
    published_at: str = field(default_factory=_now)
    fetched_at: str = field(default_factory=_now)


@dataclass(slots=True)
class SocialPost:
    """A social media post with sentiment."""

    post_id: str
    platform: str  # twitter, reddit, stocktwits
    author: str
    content: str
    instruments: tuple[str, ...] = field(default_factory=tuple)
    sentiment: SentimentLabel | None = None
    sentiment_score: float | None = None  # -1.0 to 1.0
    engagement: int = 0
    followers: int = 0
    is_verified: bool = False
    is_retweet: bool = False
    language: str = "en"
    published_at: str = field(default_factory=_now)
    fetched_at: str = field(default_factory=_now)


@dataclass(slots=True)
class SentimentSummary:
    """Aggregated sentiment for an instrument."""

    instrument_id: str
    source: str
    timeframe: str  # 1h, 1d, 1w
    bullish_pct: float = 0.0
    bearish_pct: float = 0.0
    neutral_pct: float = 0.0
    avg_sentiment_score: float = 0.0
    mention_count: int = 0
    influence_score: float = 0.0  # weighted by follower count
    as_of: str = field(default_factory=_now)


@dataclass(slots=True)
class FilingDocument:
    """An SEC/EDGAR filing document."""

    filing_id: str
    instrument_id: str
    filing_type: str  # 10-K, 10-Q, 8-K, S-1, etc.
    form_type: str = ""
    description: str = ""
    url: str = ""
    filed_at: str = field(default_factory=_now)
    fetched_at: str = field(default_factory=_now)


@dataclass(slots=True)
class EconomicEvent:
    """An economic calendar event."""

    event_id: str
    name: str  # e.g., "Non-Farm Payrolls", "FOMC Rate Decision"
    country: str
    currency: str
    impact: str = "medium"  # low, medium, high
    previous_value: str = ""
    forecast_value: str = ""
    actual_value: str = ""
    released_at: str = field(default_factory=_now)


@dataclass(slots=True)
class AlternativeDataPoint:
    """An alternative data point (satellite, credit card, etc.)."""

    data_id: str
    source: str  # e.g., "satellite_provider", "credit_card_aggregator"
    data_type: str  # e.g., "store_traffic", "credit_spend", "weather"
    instrument_id: str | None = None
    sector: str | None = None
    country: str | None = None
    value: float = 0.0
    change_pct: float | None = None
    unit: str = ""
    period: str = ""  # e.g., "Q3 2024"
    as_of: str = field(default_factory=_now)


@dataclass(slots=True)
class ResearchReport:
    """A research report from analysts or internal research."""

    report_id: str
    title: str
    author: str
    firm: str
    instruments: tuple[str, ...] = field(default_factory=tuple)
    sectors: tuple[str, ...] = field(default_factory=tuple)
    rating: str = ""  # buy, sell, hold, outperform, underperform
    target_price: float | None = None
    current_price: float | None = None
    upside_pct: float | None = None
    summary: str = ""
    url: str = ""
    published_at: str = field(default_factory=_now)
    fetched_at: str = field(default_factory=_now)


@dataclass(slots=True)
class InformationSource:
    """Configuration for an information source adapter."""

    source_id: str
    name: str
    source_type: SourceType
    api_endpoint: str = ""
    api_key_required: bool = False
    rate_limit_per_minute: int = 60
    is_enabled: bool = True
    priority: int = 1  # 1 = highest priority
    auth_config: dict[str, str] = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Base Adapter Class
# ─────────────────────────────────────────────────────────────────────────────

class BaseSourceAdapter:
    """Base class for information source adapters."""

    def __init__(self, source: InformationSource) -> None:
        self.source = source
        self._client = None

    def connect(self) -> bool:
        """Establish connection to the source."""
        raise NotImplementedError

    def disconnect(self) -> None:
        """Close connection to the source."""
        raise NotImplementedError

    def is_connected(self) -> bool:
        """Check if connected."""
        raise NotImplementedError

    def fetch(self, **kwargs) -> list:
        """Fetch data from the source."""
        raise NotImplementedError


# ─────────────────────────────────────────────────────────────────────────────
# News Adapters
# ─────────────────────────────────────────────────────────────────────────────

class ReutersNewsAdapter(BaseSourceAdapter):
    """Reuters news feed adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(self, instruments: list[str] | None = None, limit: int = 50, **kwargs) -> list[NewsArticle]:
        """Fetch news articles."""
        # Simulated - would use actual Reuters API
        articles = []
        for i in range(min(limit, 10)):
            articles.append(NewsArticle(
                article_id=f"reuters:{uuid.uuid4().hex[:12]}",
                source="reuters",
                headline=f"Reuters Article {i+1}",
                summary="Market news summary...",
                instruments=tuple(instruments) if instruments else ("AAPL",),
                published_at=_now(),
            ))
        return articles


class BloombergNewsAdapter(BaseSourceAdapter):
    """Bloomberg news feed adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(self, instruments: list[str] | None = None, limit: int = 50, **kwargs) -> list[NewsArticle]:
        """Fetch news articles."""
        articles = []
        for i in range(min(limit, 10)):
            articles.append(NewsArticle(
                article_id=f"bloomberg:{uuid.uuid4().hex[:12]}",
                source="bloomberg",
                headline=f"Bloomberg Article {i+1}",
                summary="Market news from Bloomberg...",
                instruments=tuple(instruments) if instruments else ("MSFT",),
                published_at=_now(),
            ))
        return articles


# ─────────────────────────────────────────────────────────────────────────────
# Social Media Adapters
# ─────────────────────────────────────────────────────────────────────────────

class TwitterSentimentAdapter(BaseSourceAdapter):
    """Twitter/X sentiment data adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(
        self,
        instruments: list[str] | None = None,
        limit: int = 100,
        **kwargs
    ) -> list[SocialPost]:
        """Fetch social posts with sentiment."""
        posts = []
        for i in range(min(limit, 20)):
            sentiment_labels = list(SentimentLabel)
            posts.append(SocialPost(
                post_id=f"tw:{uuid.uuid4().hex[:12]}",
                platform="twitter",
                author=f"user_{i}",
                content=f"Tweet about {' '.join(instruments) if instruments else 'AAPL'}",
                instruments=tuple(instruments) if instruments else ("AAPL",),
                sentiment=sentiment_labels[i % len(sentiment_labels)],
                sentiment_score=((i % 10) - 5) / 5.0,  # -1.0 to 1.0
                engagement=i * 10,
                followers=(i + 1) * 1000,
                published_at=_now(),
            ))
        return posts


class RedditSentimentAdapter(BaseSourceAdapter):
    """Reddit sentiment data adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(
        self,
        instruments: list[str] | None = None,
        limit: int = 50,
        subreddits: list[str] | None = None,
        **kwargs
    ) -> list[SocialPost]:
        """Fetch Reddit posts with sentiment."""
        posts = []
        for i in range(min(limit, 15)):
            posts.append(SocialPost(
                post_id=f"reddit:{uuid.uuid4().hex[:12]}",
                platform="reddit",
                author=f"redditor_{i}",
                content=f"Reddit discussion about {' '.join(instruments) if instruments else 'stocks'}",
                instruments=tuple(instruments) if instruments else ("TSLA",),
                sentiment=SentimentLabel.NEUTRAL,
                sentiment_score=0.0,
                engagement=i * 50,
                followers=(i + 1) * 500,
                published_at=_now(),
            ))
        return posts


# ─────────────────────────────────────────────────────────────────────────────
# Filing Adapters
# ─────────────────────────────────────────────────────────────────────────────

class EdgarFilingAdapter(BaseSourceAdapter):
    """SEC EDGAR filing adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(
        self,
        instruments: list[str] | None = None,
        filing_types: list[str] | None = None,
        limit: int = 50,
        **kwargs
    ) -> list[FilingDocument]:
        """Fetch SEC filings."""
        filings = []
        form_types = filing_types or ["10-K", "10-Q", "8-K"]
        for i, instr in enumerate(instruments or ["AAPL", "MSFT", "GOOGL"][:3]):
            for j, form in enumerate(form_types[:2]):
                filings.append(FilingDocument(
                    filing_id=f"edgar:{uuid.uuid4().hex[:12]}",
                    instrument_id=instr,
                    filing_type=form,
                    form_type=form,
                    description=f"{instr} {form} filing",
                    filed_at=_now(),
                ))
        return filings


# ─────────────────────────────────────────────────────────────────────────────
# Economic Calendar Adapter
# ─────────────────────────────────────────────────────────────────────────────

class EconomicCalendarAdapter(BaseSourceAdapter):
    """Economic calendar data adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(
        self,
        country: str | None = None,
        impact: str | None = None,
        limit: int = 50,
        **kwargs
    ) -> list[EconomicEvent]:
        """Fetch economic calendar events."""
        events = [
            EconomicEvent(
                event_id=f"econ:{uuid.uuid4().hex[:12]}",
                name="Non-Farm Payrolls",
                country=country or "US",
                currency="USD",
                impact="high",
                previous_value="180K",
                forecast_value="175K",
                released_at=_now(),
            ),
            EconomicEvent(
                event_id=f"econ:{uuid.uuid4().hex[:12]}",
                name="FOMC Rate Decision",
                country="US",
                currency="USD",
                impact="high",
                previous_value="5.25%",
                forecast_value="5.25%",
                released_at=_now(),
            ),
            EconomicEvent(
                event_id=f"econ:{uuid.uuid4().hex[:12]}",
                name="CPI YoY",
                country=country or "US",
                currency="USD",
                impact="high",
                previous_value="3.2%",
                forecast_value="3.1%",
                released_at=_now(),
            ),
            EconomicEvent(
                event_id=f"econ:{uuid.uuid4().hex[:12]}",
                name="GDP Growth Rate",
                country="US",
                currency="USD",
                impact="medium",
                previous_value="2.1%",
                forecast_value="2.0%",
                released_at=_now(),
            ),
        ]
        return events[:limit]


# ─────────────────────────────────────────────────────────────────────────────
# Alternative Data Adapters
# ─────────────────────────────────────────────────────────────────────────────

class SatelliteDataAdapter(BaseSourceAdapter):
    """Satellite imagery alternative data adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(
        self,
        instruments: list[str] | None = None,
        data_type: str = "store_traffic",
        **kwargs
    ) -> list[AlternativeDataPoint]:
        """Fetch satellite-based alternative data."""
        points = []
        for instr in (instruments or ["WMT", "TGT", "COST"])[:3]:
            points.append(AlternativeDataPoint(
                data_id=f"sat:{uuid.uuid4().hex[:12]}",
                source="satellite_provider",
                data_type=data_type,
                instrument_id=instr,
                value=100.0 + (hash(instr) % 50),
                change_pct=(hash(instr) % 20) - 10,
                unit="index",
                period="Q4 2024",
                as_of=_now(),
            ))
        return points


class CreditCardDataAdapter(BaseSourceAdapter):
    """Credit card spending alternative data adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(
        self,
        sectors: list[str] | None = None,
        **kwargs
    ) -> list[AlternativeDataPoint]:
        """Fetch credit card spending data."""
        points = []
        for sector in (sectors or ["retail", "restaurants", "travel"])[:3]:
            points.append(AlternativeDataPoint(
                data_id=f"cc:{uuid.uuid4().hex[:12]}",
                source="credit_card_aggregator",
                data_type="credit_spend",
                sector=sector,
                value=100.0 + (hash(sector) % 30),
                change_pct=(hash(sector) % 15) - 5,
                unit="yoy_pct",
                period="Q4 2024",
                as_of=_now(),
            ))
        return points


# ─────────────────────────────────────────────────────────────────────────────
# Research Adapter
# ─────────────────────────────────────────────────────────────────────────────

class ResearchReportAdapter(BaseSourceAdapter):
    """Research report adapter."""

    def connect(self) -> bool:
        self._client = {"connected": True}
        return True

    def disconnect(self) -> None:
        self._client = None

    def is_connected(self) -> bool:
        return self._client is not None

    def fetch(
        self,
        instruments: list[str] | None = None,
        limit: int = 20,
        **kwargs
    ) -> list[ResearchReport]:
        """Fetch research reports."""
        reports = []
        firms = ["Goldman Sachs", "Morgan Stanley", "JP Morgan", "Bank of America", "Citi"]
        ratings = ["buy", "outperform", "hold", "underperform", "sell"]
        for i in range(min(limit, 10)):
            instr = (instruments or ["AAPL", "MSFT", "GOOGL"])[i % 3]
            current = 150.0 + i
            target = current * (1.0 + (i % 10) * 0.02)
            reports.append(ResearchReport(
                report_id=f"res:{uuid.uuid4().hex[:12]}",
                title=f"{firms[i % len(firms)]} Initiates Coverage on {instr}",
                author=f"Analyst {i}",
                firm=firms[i % len(firms)],
                instruments=(instr,),
                rating=ratings[i % len(ratings)],
                target_price=target,
                current_price=current,
                upside_pct=(target - current) / current * 100,
                summary=f"Research report on {instr}",
                published_at=_now(),
            ))
        return reports


# ─────────────────────────────────────────────────────────────────────────────
# Information Service
# ─────────────────────────────────────────────────────────────────────────────

class InformationService:
    """Information source service (IN-01 ~ IN-05).

    Provides unified access to:
    - News feeds
    - Social media sentiment
    - SEC filings
    - Economic calendars
    - Alternative data
    - Research reports
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._sources: dict[str, InformationSource] = {}
        self._adapters: dict[str, BaseSourceAdapter] = {}
        self._news_cache: list[NewsArticle] = []
        self._social_cache: list[SocialPost] = []
        self._last_fetch: dict[str, str] = {}
        self._register_default_sources()

    def _register_default_sources(self) -> None:
        """Register default information sources."""
        sources = [
            InformationSource(source_id="reuters", name="Reuters", source_type=SourceType.NEWS, priority=1),
            InformationSource(source_id="bloomberg", name="Bloomberg", source_type=SourceType.NEWS, priority=2),
            InformationSource(source_id="twitter", name="Twitter/X", source_type=SourceType.SOCIAL, priority=1),
            InformationSource(source_id="reddit", name="Reddit", source_type=SourceType.SOCIAL, priority=2),
            InformationSource(source_id="edgar", name="SEC EDGAR", source_type=SourceType.FILING, priority=1),
            InformationSource(source_id="econ_calendar", name="Economic Calendar", source_type=SourceType.ECONOMIC, priority=1),
            InformationSource(source_id="satellite", name="Satellite Data", source_type=SourceType.ALTERNATIVE, priority=1),
            InformationSource(source_id="credit_card", name="Credit Card Data", source_type=SourceType.ALTERNATIVE, priority=2),
            InformationSource(source_id="research", name="Research Reports", source_type=SourceType.RESEARCH, priority=1),
        ]
        for src in sources:
            self._sources[src.source_id] = src

    def get_source(self, source_id: str) -> InformationSource | None:
        """Get an information source by ID."""
        return self._sources.get(source_id)

    def get_sources_by_type(self, source_type: SourceType) -> list[InformationSource]:
        """Get all sources of a given type."""
        return [s for s in self._sources.values() if s.source_type == source_type and s.is_enabled]

    def connect_source(self, source_id: str) -> bool:
        """Connect to an information source."""
        source = self._sources.get(source_id)
        if not source:
            return False

        adapter = self._create_adapter(source)
        if adapter and adapter.connect():
            self._adapters[source_id] = adapter
            return True
        return False

    def disconnect_source(self, source_id: str) -> None:
        """Disconnect from an information source."""
        if source_id in self._adapters:
            self._adapters[source_id].disconnect()
            del self._adapters[source_id]

    def _create_adapter(self, source: InformationSource) -> BaseSourceAdapter | None:
        """Create an adapter for a source."""
        adapters = {
            "reuters": ReutersNewsAdapter,
            "bloomberg": BloombergNewsAdapter,
            "twitter": TwitterSentimentAdapter,
            "reddit": RedditSentimentAdapter,
            "edgar": EdgarFilingAdapter,
            "econ_calendar": EconomicCalendarAdapter,
            "satellite": SatelliteDataAdapter,
            "credit_card": CreditCardDataAdapter,
            "research": ResearchReportAdapter,
        }
        adapter_class = adapters.get(source.source_id)
        if adapter_class:
            return adapter_class(source)
        return None

    # ── News ───────────────────────────────────────────────────────────────

    def fetch_news(
        self,
        instruments: list[str] | None = None,
        sources: list[str] | None = None,
        limit: int = 50,
    ) -> list[NewsArticle]:
        """Fetch news articles from all connected sources."""
        articles: list[NewsArticle] = []
        news_sources = sources or [s.source_id for s in self.get_sources_by_type(SourceType.NEWS)]

        for src_id in news_sources:
            if src_id not in self._adapters:
                self.connect_source(src_id)
            adapter = self._adapters.get(src_id)
            if adapter and adapter.is_connected():
                try:
                    fetched = adapter.fetch(instruments=instruments, limit=limit)
                    articles.extend(fetched)
                except Exception:
                    pass

        articles.sort(key=lambda a: a.published_at, reverse=True)
        self._news_cache = articles[:limit]
        self._last_fetch["news"] = _now()
        return articles[:limit]

    def get_news_sentiment(
        self,
        instrument_id: str,
        timeframe: str = "1d",
    ) -> SentimentSummary | None:
        """Calculate aggregated sentiment for an instrument from news."""
        relevant = [a for a in self._news_cache if instrument_id in a.instruments]
        if not relevant:
            return None

        bullish = sum(1 for a in relevant if a.sentiment in (SentimentLabel.BULLISH, SentimentLabel.VERY_BULLISH))
        bearish = sum(1 for a in relevant if a.sentiment in (SentimentLabel.BEARISH, SentimentLabel.VERY_BEARISH))
        neutral = sum(1 for a in relevant if a.sentiment == SentimentLabel.NEUTRAL)
        total = len(relevant)

        scores = [a.sentiment_score for a in relevant if a.sentiment_score is not None]
        avg_score = sum(scores) / len(scores) if scores else 0.0

        return SentimentSummary(
            instrument_id=instrument_id,
            source="news",
            timeframe=timeframe,
            bullish_pct=bullish / total * 100 if total else 0,
            bearish_pct=bearish / total * 100 if total else 0,
            neutral_pct=neutral / total * 100 if total else 0,
            avg_sentiment_score=avg_score,
            mention_count=total,
        )

    # ── Social Sentiment ──────────────────────────────────────────────────

    def fetch_social_sentiment(
        self,
        instruments: list[str] | None = None,
        sources: list[str] | None = None,
        limit: int = 100,
    ) -> list[SocialPost]:
        """Fetch social media posts."""
        posts: list[SocialPost] = []
        social_sources = sources or [s.source_id for s in self.get_sources_by_type(SourceType.SOCIAL)]

        for src_id in social_sources:
            if src_id not in self._adapters:
                self.connect_source(src_id)
            adapter = self._adapters.get(src_id)
            if adapter and adapter.is_connected():
                try:
                    fetched = adapter.fetch(instruments=instruments, limit=limit)
                    posts.extend(fetched)
                except Exception:
                    pass

        posts.sort(key=lambda p: p.published_at, reverse=True)
        self._social_cache = posts[:limit]
        self._last_fetch["social"] = _now()
        return posts[:limit]

    def get_social_sentiment(
        self,
        instrument_id: str,
        timeframe: str = "1d",
    ) -> SentimentSummary | None:
        """Calculate aggregated social sentiment for an instrument."""
        relevant = [p for p in self._social_cache if instrument_id in p.instruments]
        if not relevant:
            return None

        bullish = sum(1 for p in relevant if p.sentiment in (SentimentLabel.BULLISH, SentimentLabel.VERY_BULLISH))
        bearish = sum(1 for p in relevant if p.sentiment in (SentimentLabel.BEARISH, SentimentLabel.VERY_BEARISH))
        neutral = sum(1 for p in relevant if p.sentiment == SentimentLabel.NEUTRAL)
        total = len(relevant)

        scores = [p.sentiment_score for p in relevant if p.sentiment_score is not None]
        avg_score = sum(scores) / len(scores) if scores else 0.0

        total_engagement = sum(p.engagement * p.followers for p in relevant)
        max_followers = max((p.followers for p in relevant), default=1)
        influence_score = total_engagement / (max_followers * total) if total else 0.0

        return SentimentSummary(
            instrument_id=instrument_id,
            source="social",
            timeframe=timeframe,
            bullish_pct=bullish / total * 100 if total else 0,
            bearish_pct=bearish / total * 100 if total else 0,
            neutral_pct=neutral / total * 100 if total else 0,
            avg_sentiment_score=avg_score,
            mention_count=total,
            influence_score=influence_score,
        )

    # ── Filings ───────────────────────────────────────────────────────────

    def fetch_filings(
        self,
        instruments: list[str] | None = None,
        filing_types: list[str] | None = None,
        limit: int = 50,
    ) -> list[FilingDocument]:
        """Fetch SEC filings."""
        filings: list[FilingDocument] = []
        edgar = self._sources.get("edgar")
        if edgar:
            if "edgar" not in self._adapters:
                self.connect_source("edgar")
            adapter = self._adapters.get("edgar")
            if adapter and adapter.is_connected():
                try:
                    filings = adapter.fetch(
                        instruments=instruments,
                        filing_types=filing_types,
                        limit=limit,
                    )
                except Exception:
                    pass
        return filings

    # ── Economic Events ───────────────────────────────────────────────────

    def fetch_economic_events(
        self,
        country: str | None = None,
        impact: str | None = None,
        limit: int = 50,
    ) -> list[EconomicEvent]:
        """Fetch economic calendar events."""
        econ = self._sources.get("econ_calendar")
        if econ:
            if "econ_calendar" not in self._adapters:
                self.connect_source("econ_calendar")
            adapter = self._adapters.get("econ_calendar")
            if adapter and adapter.is_connected():
                try:
                    return adapter.fetch(country=country, impact=impact, limit=limit)
                except Exception:
                    pass
        return []

    # ── Alternative Data ──────────────────────────────────────────────────

    def fetch_alternative_data(
        self,
        source: str,
        instruments: list[str] | None = None,
        sectors: list[str] | None = None,
        limit: int = 50,
    ) -> list[AlternativeDataPoint]:
        """Fetch alternative data."""
        if source not in self._adapters:
            self.connect_source(source)
        adapter = self._adapters.get(source)
        if adapter and adapter.is_connected():
            try:
                if source == "satellite":
                    return adapter.fetch(instruments=instruments, limit=limit)
                elif source == "credit_card":
                    return adapter.fetch(sectors=sectors, limit=limit)
            except Exception:
                pass
        return []

    # ── Research Reports ─────────────────────────────────────────────────

    def fetch_research_reports(
        self,
        instruments: list[str] | None = None,
        limit: int = 20,
    ) -> list[ResearchReport]:
        """Fetch research reports."""
        research = self._sources.get("research")
        if research:
            if "research" not in self._adapters:
                self.connect_source("research")
            adapter = self._adapters.get("research")
            if adapter and adapter.is_connected():
                try:
                    return adapter.fetch(instruments=instruments, limit=limit)
                except Exception:
                    pass
        return []

    # ── Combined Sentiment ────────────────────────────────────────────────

    def get_combined_sentiment(
        self,
        instrument_id: str,
        timeframe: str = "1d",
    ) -> dict[str, SentimentSummary | None]:
        """Get combined sentiment from all sources for an instrument."""
        news_sent = self.get_news_sentiment(instrument_id, timeframe)
        social_sent = self.get_social_sentiment(instrument_id, timeframe)
        return {
            "news": news_sent,
            "social": social_sent,
        }

    # ── Source Management ────────────────────────────────────────────────

    def enable_source(self, source_id: str) -> InformationSource | None:
        """Enable an information source."""
        source = self._sources.get(source_id)
        if source:
            source.is_enabled = True
        return source

    def disable_source(self, source_id: str) -> InformationSource | None:
        """Disable an information source."""
        source = self._sources.get(source_id)
        if source:
            source.is_enabled = False
        return source

    def get_all_sources(self) -> list[InformationSource]:
        """Get all registered sources."""
        return list(self._sources.values())
