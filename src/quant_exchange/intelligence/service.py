"""Market intelligence engine with sentiment scoring and event classification.

Implements the documented intelligence pipeline:
1. Ingestion → deduplication → language detection
2. Event classification (listing, regulatory, hack, earnings, etc.)
3. Lexicon-based sentiment scoring
4. Directional bias aggregation with recency weighting
"""

from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from quant_exchange.core.models import (
    Direction,
    DirectionalBias,
    EventTag,
    MarketDocument,
    SentimentLabel,
    SentimentResult,
)


class IntelligenceEngine:
    """Lexicon-based intelligence engine suitable for MVP testing."""

    POSITIVE_TERMS = {
        "上涨",
        "利好",
        "突破",
        "增长",
        "看多",
        "买入",
        "强劲",
        "创新高",
        "rally",
        "growth",
        "beat",
        "bullish",
        "upgrade",
        "inflow",
        "approval",
        "strong",
        "breakout",
    }
    NEGATIVE_TERMS = {
        "下跌",
        "利空",
        "暴跌",
        "风险",
        "看空",
        "亏损",
        "监管",
        "清算",
        "hack",
        "crash",
        "loss",
        "bearish",
        "downgrade",
        "liquidation",
        "outflow",
        "lawsuit",
        "default",
    }
    SOURCE_WEIGHT = {
        "exchange_announcement": 1.0,
        "newswire": 0.9,
        "social": 0.6,
        "research": 0.8,
    }
    # Event classification keyword maps
    EVENT_KEYWORDS: dict[str, list[str]] = {
        EventTag.LISTING.value: ["listing", "上市", "new coin", "新币"],
        EventTag.DELISTING.value: ["delisting", "delist", "退市", "下架"],
        EventTag.SECURITY_INCIDENT.value: ["hack", "exploit", "breach", "漏洞", "攻击", "被盗"],
        EventTag.REGULATORY.value: ["regulatory", "regulation", "sec", "ban", "监管", "合规", "政策"],
        EventTag.ETF_MACRO.value: ["etf", "fed", "interest rate", "gdp", "cpi", "macro", "央行", "降息", "加息"],
        EventTag.LIQUIDATION.value: ["liquidation", "bankruptcy", "清算", "破产"],
        EventTag.PARTNERSHIP.value: ["partnership", "collaboration", "合作", "战略"],
        EventTag.PRODUCT_LAUNCH.value: ["launch", "release", "upgrade", "发布", "升级"],
        EventTag.EARNINGS.value: ["earnings", "revenue", "profit", "财报", "营收", "利润"],
        EventTag.DIVIDEND.value: ["dividend", "分红", "派息"],
        EventTag.SPLIT.value: ["split", "拆股", "拆分"],
        EventTag.M_AND_A.value: ["acquisition", "merger", "takeover", "收购", "并购"],
    }

    # ── Entity recognition patterns ─────────────────────────────────────────

    # Common financial instrument / entity patterns
    _ENTITY_PATTERNS: list[tuple[str, str, str]] = [
        # (pattern_description, regex, entity_type)
        ("ticker symbol", r"\$?[A-Z]{2,5}(?:\.[A-Z]{1,2})?"),
        ("price target", r"(?:price target|pt|target)\s*[:\-]?\s*\$?[\d,]+(?:\.\d+)?"),
        ("percentage change", r"[\+\-]?\d+\.\d+\%"),
        ("stock exchange", r"(?:NYSE|NASDAQ|SEHK|SSE|SZSE|Coinbase|Binance|Bitfinex)"),
        ("company name", r"(?:company|firm|inc|ltd|corp|plc)\s+[A-Z][a-zA-Z]+(?:\s+[A-Z][a-zA-Z]+)?"),
        ("currency pair", r"(?:USD|EUR|GBP|JPY|CNY|HKD|SGD|AUD)[\/\-](?:USD|EUR|GBP|JPY|CNY|HKD|SGD|AUD)"),
        ("economic indicator", r"(?:CPI|GDP|NFP|PMI|ISM|PPI|零售销售|非农|CPI|PPI)"),
    ]

    def __init__(self) -> None:
        self.documents: dict[str, MarketDocument] = {}
        self.document_signatures: set[str] = set()
        self.sentiment_results: dict[str, SentimentResult] = {}
        self.instrument_results: dict[str, list[SentimentResult]] = defaultdict(list)
        self.event_classifications: dict[str, str] = {}  # document_id -> event_tag
        # Entity extraction results: document_id -> list of (entity_text, entity_type)
        self._entities: dict[str, list[tuple[str, str]]] = defaultdict(list)
        # Hotness tracking: instrument_id -> list of (timestamp, hotness_score)
        self._hotness_history: dict[str, list[tuple[datetime, float]]] = defaultdict(list)
        # Market confirmation cache
        self._confirmation_cache: dict[str, dict[str, Any]] = {}

    def ingest_documents(self, documents: list[MarketDocument]) -> list[SentimentResult]:
        """Normalize, deduplicate, and score incoming text documents."""

        results: list[SentimentResult] = []
        for document in documents:
            signature = self._signature(document)
            if signature in self.document_signatures:
                continue
            self.document_signatures.add(signature)
            if document.language == "unknown":
                document = MarketDocument(
                    document_id=document.document_id,
                    source=document.source,
                    instrument_id=document.instrument_id,
                    published_at=document.published_at,
                    title=document.title,
                    content=document.content,
                    language=self.detect_language(document.title + " " + document.content),
                    metadata=document.metadata,
                )
            self.documents[document.document_id] = document
            # Classify event type
            event_tag = self.classify_event(document)
            self.event_classifications[document.document_id] = event_tag
            # Auto-fill event_tag on document if empty
            if not document.event_tag and event_tag != EventTag.OTHER.value:
                document = MarketDocument(
                    document_id=document.document_id,
                    source=document.source,
                    instrument_id=document.instrument_id,
                    published_at=document.published_at,
                    title=document.title,
                    content=document.content,
                    language=document.language,
                    event_tag=event_tag,
                    metadata=document.metadata,
                )
                self.documents[document.document_id] = document
            result = self.score_document(document)
            self.sentiment_results[document.document_id] = result
            self.instrument_results[document.instrument_id].append(result)
            results.append(result)
        return results

    def detect_language(self, text: str) -> str:
        """Apply a lightweight language heuristic for Chinese and English text."""

        if any("\u4e00" <= char <= "\u9fff" for char in text):
            return "zh"
        return "en"

    def classify_event(self, document: MarketDocument) -> str:
        """Classify a document into an event category using keyword matching."""

        text = f"{document.title} {document.content}".lower()
        best_tag = EventTag.OTHER.value
        best_hits = 0
        for tag, keywords in self.EVENT_KEYWORDS.items():
            hits = sum(1 for kw in keywords if kw.lower() in text)
            if hits > best_hits:
                best_hits = hits
                best_tag = tag
        return best_tag

    def aggregate_sentiment(
        self,
        instrument_id: str,
        *,
        as_of: datetime,
        window: timedelta = timedelta(hours=1),
    ) -> dict[str, Any]:
        """Return aggregated sentiment statistics for one instrument over a window.

        Returns a dict with: avg_score, doc_count, positive_count, negative_count, neutral_count.
        """

        results: list[SentimentResult] = []
        for doc in self.documents.values():
            if doc.instrument_id != instrument_id:
                continue
            if doc.published_at > as_of or doc.published_at < as_of - window:
                continue
            sr = self.sentiment_results.get(doc.document_id)
            if sr:
                results.append(sr)
        if not results:
            return {"avg_score": 0.0, "doc_count": 0, "positive_count": 0, "negative_count": 0, "neutral_count": 0}
        pos = sum(1 for r in results if r.label == SentimentLabel.POSITIVE)
        neg = sum(1 for r in results if r.label == SentimentLabel.NEGATIVE)
        neu = sum(1 for r in results if r.label == SentimentLabel.NEUTRAL)
        avg = sum(r.score for r in results) / len(results)
        return {"avg_score": avg, "doc_count": len(results), "positive_count": pos, "negative_count": neg, "neutral_count": neu}

    def score_document(self, document: MarketDocument) -> SentimentResult:
        """Generate a lexicon-based sentiment result for one document."""

        text = f"{document.title} {document.content}".lower()
        positive_hits = sum(1 for token in self.POSITIVE_TERMS if token.lower() in text)
        negative_hits = sum(1 for token in self.NEGATIVE_TERMS if token.lower() in text)
        total_hits = positive_hits + negative_hits
        raw_score = 0.0 if total_hits == 0 else (positive_hits - negative_hits) / total_hits
        source_weight = self.SOURCE_WEIGHT.get(document.source, 0.7)
        confidence = min(1.0, 0.35 + 0.15 * total_hits + 0.35 * source_weight)
        score = raw_score * source_weight
        if score > 0.15:
            label = SentimentLabel.POSITIVE
        elif score < -0.15:
            label = SentimentLabel.NEGATIVE
        else:
            label = SentimentLabel.NEUTRAL
        return SentimentResult(
            document_id=document.document_id,
            instrument_id=document.instrument_id,
            score=score,
            label=label,
            confidence=confidence,
            positive_hits=positive_hits,
            negative_hits=negative_hits,
        )

    def directional_bias(
        self,
        instrument_id: str,
        *,
        as_of: datetime,
        window: timedelta = timedelta(days=1),
    ) -> DirectionalBias:
        """Aggregate document sentiment into a directional bias over a time window."""

        candidates = []
        for document in self.documents.values():
            if document.instrument_id != instrument_id:
                continue
            if document.published_at > as_of:
                continue
            if document.published_at < as_of - window:
                continue
            candidates.append(document)
        if not candidates:
            return DirectionalBias(
                instrument_id=instrument_id,
                as_of=as_of,
                window=window,
                score=0.0,
                direction=Direction.FLAT,
                confidence=0.1,
                supporting_documents=0,
            )
        weighted_score = 0.0
        weight_sum = 0.0
        for document in candidates:
            result = self.sentiment_results[document.document_id]
            age_hours = max((as_of - document.published_at).total_seconds() / 3600.0, 0.0)
            recency_weight = 1.0 / (1.0 + age_hours / 12.0)
            weight = result.confidence * recency_weight
            weighted_score += result.score * weight
            weight_sum += weight
        score = weighted_score / weight_sum if weight_sum else 0.0
        if score > 0.10:
            direction = Direction.LONG
        elif score < -0.10:
            direction = Direction.SHORT
        else:
            direction = Direction.FLAT
        confidence = min(1.0, 0.25 + 0.12 * len(candidates) + abs(score) * 0.5)
        return DirectionalBias(
            instrument_id=instrument_id,
            as_of=as_of,
            window=window,
            score=score,
            direction=direction,
            confidence=confidence,
            supporting_documents=len(candidates),
        )

    def recent_documents(self, limit: int = 20) -> list[MarketDocument]:
        """Return the most recent ingested documents ordered by published_at desc."""
        docs = sorted(self.documents.values(), key=lambda d: d.published_at, reverse=True)
        return docs[:limit]

    # ── Entity Recognition ───────────────────────────────────────────────────

    def extract_entities(self, document: MarketDocument) -> list[tuple[str, str]]:
        """Extract named entities from document title and content.

        Returns a list of (entity_text, entity_type) tuples.
        Entity types: ticker, price_target, percentage, exchange, currency_pair,
                      economic_indicator, person, product, organization.
        """
        import re

        text = f"{document.title} {document.content}"
        entities: list[tuple[str, str]] = []
        seen: set[str] = set()

        # Ticker symbols: $BTC, BTC, or BTC.US
        for match in re.finditer(r"\$?([A-Z]{2,5})(?:\.([A-Z]{1,2}))?", text):
            ticker = match.group(0)
            if ticker in seen:
                continue
            seen.add(ticker)
            entities.append((ticker, "ticker"))

        # Price targets: PT $150 or price target: 150
        for match in re.finditer(r"(?:price target|pt|target)[:\-]?\s*\$?([\d,]+(?:\.\d+)?)", text, re.IGNORECASE):
            val = match.group(0)
            if val not in seen:
                seen.add(val)
                entities.append((val, "price_target"))

        # Percentages
        for match in re.finditer(r"[\+\-]?\d+\.\d+\%", text):
            pct = match.group(0)
            if pct not in seen:
                seen.add(pct)
                entities.append((pct, "percentage"))

        # Exchanges
        for match in re.finditer(
            r"(NYSE|NASDAQ|SEHK|SSE|SZSE|Coinbase|Binance|Bitfinex|OKX|Bybit|Kraken|ErisX)", text
        ):
            ex = match.group(1)
            if ex not in seen:
                seen.add(ex)
                entities.append((ex, "exchange"))

        # Currency pairs
        for match in re.finditer(
            r"(USD|EUR|GBP|JPY|CNY|HKD|SGD|AUD|KRW|INR)[\/\-](USD|EUR|GBP|JPY|CNY|HKD|SGD|AUD|KRW|INR)",
            text,
        ):
            pair = match.group(0)
            if pair not in seen:
                seen.add(pair)
                entities.append((pair, "currency_pair"))

        # Economic indicators
        for match in re.finditer(
            r"(?:CPI|GDP|NFP|PMI|ISM|PPI|零售销售|非农|就业|通胀|加息|降息|缩表|扩表)"
            r"|(?:Federal Reserve|Fed|ECB|BOC|BOC Rate)",
            text,
            re.IGNORECASE,
        ):
            ind = match.group(0)
            if ind not in seen:
                seen.add(ind)
                entities.append((ind, "economic_indicator"))

        # Product names (common crypto/finance products)
        for match in re.finditer(
            r"(?:BTC|ETH|SOL|XRP|BNB|ADA|MATIC|LINK|AVAX|UNI|DOT|ATOM|LTC|BCH)"
            r"|(?:Apple|Microsoft|Google|Amazon|Tesla|Meta|Nvidia|Netflix|JPMorgan|Goldman)",
            text,
        ):
            prod = match.group(0)
            if prod not in seen:
                seen.add(prod)
                entities.append((prod, "product_or_organization"))

        self._entities[document.document_id] = entities
        return entities

    def get_document_entities(self, document_id: str) -> list[tuple[str, str]]:
        """Return cached entities for a previously ingested document."""
        return self._entities.get(document_id, [])

    def get_instrument_entities(self, instrument_id: str) -> dict[str, int]:
        """Aggregate all entities across documents for one instrument.

        Returns a dict mapping entity_text -> frequency count.
        """
        freq: dict[str, int] = defaultdict(int)
        for doc in self.documents.values():
            if doc.instrument_id != instrument_id:
                continue
            for entity_text, _ in self._entities.get(doc.document_id, []):
                freq[entity_text] += 1
        return dict(freq)

    # ── Hotness / Diffusion Engine ───────────────────────────────────────────

    def compute_hotness(
        self,
        instrument_id: str,
        *,
        as_of: datetime,
        window: timedelta = timedelta(hours=24),
    ) -> dict[str, Any]:
        """Compute how fast and wide information is spreading for an instrument.

        Returns:
            hotness_score: 0-1 score (1 = maximum spread/velocity)
            velocity: documents per hour over the window
            spread: number of unique sources
            peak_time: datetime of maximum document density
        """
        window_docs = []
        for doc in self.documents.values():
            if doc.instrument_id != instrument_id:
                continue
            if as_of - window <= doc.published_at <= as_of:
                window_docs.append(doc)

        if not window_docs:
            return {"hotness_score": 0.0, "velocity": 0.0, "spread": 0, "peak_time": None, "doc_count": 0}

        # Velocity: docs per hour
        hours_span = max((as_of - min(d.published_at for d in window_docs)).total_seconds() / 3600.0, 0.5)
        velocity = len(window_docs) / hours_span

        # Spread: unique sources
        sources = set(doc.source for doc in window_docs)
        spread = len(sources)

        # Find peak 1-hour bucket
        buckets: dict[int, int] = defaultdict(int)
        for doc in window_docs:
            bucket = int(doc.published_at.timestamp() / 3600)
            buckets[bucket] += 1
        peak_bucket = max(buckets, key=buckets.get) if buckets else None
        peak_time = datetime.fromtimestamp(peak_bucket * 3600, tz=as_of.tzinfo) if peak_bucket else as_of

        # Combine into hotness score (0-1)
        # Normalize velocity: >10 docs/hr is max hotness
        vel_norm = min(1.0, velocity / 10.0)
        # Normalize spread: >5 sources is max
        spr_norm = min(1.0, spread / 5.0)
        # Recency bonus: recent docs within last 2 hrs
        recent_count = sum(1 for d in window_docs if (as_of - d.published_at).total_seconds() / 3600 < 2.0)
        rec_norm = min(1.0, recent_count / 5.0)
        hotness_score = 0.5 * vel_norm + 0.3 * spr_norm + 0.2 * rec_norm

        # Record in history
        self._hotness_history[instrument_id].append((as_of, hotness_score))
        # Trim history beyond window * 2
        cutoff = as_of - window * 2
        self._hotness_history[instrument_id] = [
            (ts, h) for ts, h in self._hotness_history[instrument_id] if ts > cutoff
        ]

        return {
            "hotness_score": hotness_score,
            "velocity": velocity,
            "spread": spread,
            "peak_time": peak_time,
            "doc_count": len(window_docs),
        }

    def hotness_trend(self, instrument_id: str, limit: int = 10) -> list[tuple[datetime, float]]:
        """Return recent hotness history for an instrument."""
        history = sorted(self._hotness_history.get(instrument_id, []), key=lambda x: x[0])
        return history[-limit:]

    # ── Market Confirmation Engine ────────────────────────────────────────────

    def market_confirmation(
        self,
        instrument_id: str,
        price_change_pct: float,
        *,
        as_of: datetime,
        sentiment_window: timedelta = timedelta(hours=6),
        price_window: timedelta = timedelta(hours=6),
    ) -> dict[str, Any]:
        """Correlate sentiment signals with price action to confirm or contradict.

        Returns a confirmation dict:
            confirmed: bool — sentiment direction aligns with price direction
            alignment: "confirmed" | "contradicted" | "inconclusive"
            sentiment_direction: Direction
            price_direction: Direction
            correlation_strength: 0.0-1.0
            confirmation_score: 0.0-1.0
        """
        if price_change_pct > 0.0:
            price_direction = Direction.LONG
        elif price_change_pct < 0.0:
            price_direction = Direction.SHORT
        else:
            price_direction = Direction.FLAT

        # Get recent sentiment
        agg = self.aggregate_sentiment(instrument_id, as_of=as_of, window=sentiment_window)
        sentiment_score = agg["avg_score"]
        if sentiment_score > 0.1:
            sentiment_direction = Direction.LONG
        elif sentiment_score < -0.1:
            sentiment_direction = Direction.SHORT
        else:
            sentiment_direction = Direction.FLAT

        # Compute correlation strength based on sentiment confidence and doc count
        doc_count = agg["doc_count"]
        avg_confidence = 0.5  # default
        if doc_count > 0:
            confs = [self.sentiment_results[d.document_id].confidence for d in self.documents.values()
                     if d.instrument_id == instrument_id
                     and as_of - sentiment_window <= d.published_at <= as_of
                     and d.document_id in self.sentiment_results]
            avg_confidence = sum(confs) / len(confs) if confs else 0.5

        correlation_strength = min(1.0, 0.3 * doc_count + 0.5 * avg_confidence)

        # Determine alignment
        if sentiment_direction == Direction.FLAT or price_direction == Direction.FLAT:
            alignment = "inconclusive"
            confirmed = False
        elif sentiment_direction == price_direction:
            alignment = "confirmed"
            confirmed = True
        else:
            alignment = "contradicted"
            confirmed = False

        confirmation_score = correlation_strength if confirmed else (1.0 - correlation_strength) * 0.5

        cache_key = f"{instrument_id}:{as_of.isoformat()}"
        result = {
            "confirmed": confirmed,
            "alignment": alignment,
            "sentiment_direction": sentiment_direction.value,
            "price_direction": price_direction.value,
            "sentiment_score": sentiment_score,
            "price_change_pct": price_change_pct,
            "correlation_strength": correlation_strength,
            "confirmation_score": confirmation_score,
            "doc_count": doc_count,
        }
        self._confirmation_cache[cache_key] = result
        return result

    def _signature(self, document: MarketDocument) -> str:
        """Build a stable content signature used for document deduplication."""

        payload = "|".join(
            [
                document.instrument_id,
                document.source,
                document.title.strip().lower(),
                document.content.strip().lower(),
                document.published_at.isoformat(),
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
