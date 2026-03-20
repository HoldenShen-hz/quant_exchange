"""LLM-powered interpretation service for market intelligence (IN-07).

Provides:
- LLM summarization of documents
- Event timeline construction
- Explainable directional bias
- Natural language market commentary
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from quant_exchange.core.models import DirectionalBias, MarketDocument


class LLMProvider(str, Enum):
    OPENAI = "openai"
    ANTHROPIC = "anthropic"
    MOCK = "mock"


# ─────────────────────────────────────────────────────────────────────────────
# Data models
# ─────────────────────────────────────────────────────────────────────────────


@dataclass(slots=True)
class LLMSummary:
    """LLM-generated summary of a set of documents."""

    instrument_id: str
    summary_text: str
    key_themes: list[str]
    overall_tone: str  # bullish, bearish, neutral, mixed
    confidence: float
    document_count: int
    generated_at: datetime


@dataclass(slots=True)
class EventCluster:
    """A cluster of related events within a time window."""

    cluster_id: str
    headline: str
    description: str
    event_type: str  # earnings, regulatory, macro, product, leadership, other
    sentiment_impact: float  # -1.0 to 1.0
    affected_instruments: list[str]
    source_documents: list[str]
    time_range_start: datetime
    time_range_end: datetime


@dataclass(slots=True)
class EventTimeline:
    """Chronological timeline of events for an instrument."""

    instrument_id: str
    clusters: list[EventCluster] = field(default_factory=list)
    narrative: str = ""  # LLM-generated narrative connecting events
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class BiasExplanation:
    """Human-readable explanation of a DirectionalBias signal."""

    instrument_id: str
    explanation_text: str
    key_drivers: list[str]  # Top contributing documents/themes
    confidence_factors: list[str]  # What increases/ decreases confidence
    risk_cautions: list[str]  # Limitations and caveats
    alternative_scenarios: list[str]  # What could change the bias
    generated_at: datetime


@dataclass(slots=True)
class MarketCommentary:
    """Natural language market commentary for an instrument."""

    instrument_id: str
    headline: str
    body: str
    sentiment_summary: str
    key_level: str  # Support/resistance interpretation
    catalyst_outlook: str  # Near-term catalysts
    generated_at: datetime


# ─────────────────────────────────────────────────────────────────────────────
# LLM Client Protocol & Implementations
# ─────────────────────────────────────────────────────────────────────────────


class LLMClient(Protocol):
    """Protocol for LLM client implementations."""

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        """Send a prompt to the LLM and return the completion."""
        ...


class MockLLMClient:
    """Mock LLM client that generates realistic structured responses without API calls."""

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        """Return a structured mock response based on prompt content."""
        prompt_lower = prompt.lower()

        if "summarize" in prompt_lower or "summary" in prompt_lower:
            if "bullish" in prompt_lower or "利多" in prompt_lower:
                return (
                    "## Market Summary\n\nPositive sentiment dominates the recent news flow for this "
                    "instrument. Key themes include upgrade actions by major institutions, strong "
                    "earnings surprises, and favorable regulatory developments. Market participants "
                    "appear constructive on the near-term outlook.\n\n**Key Themes:** institutional upgrades, "
                    "earnings beats, regulatory support\n**Overall Tone:** bullish\n**Confidence:** 0.78"
                )
            elif "bearish" in prompt_lower or "利空" in prompt_lower:
                return (
                    "## Market Summary\n\nNegative sentiment prevails in recent coverage. Concerns "
                    "include regulatory headwinds, earnings misses, and macroeconomic uncertainties. "
                    "Risk-off positioning appears elevated among market participants.\n\n"
                    "**Key Themes:** regulatory scrutiny, earnings misses, macro risks\n**Overall Tone:** bearish\n**Confidence:** 0.72"
                )
            else:
                return (
                    "## Market Summary\n\nRecent coverage presents a mixed picture with no strong "
                    "directional conviction. Mixed signals from earnings, neutral regulatory commentary, "
                    "and balanced risk/reward assessments characterize the current sentiment landscape.\n\n"
                    "**Key Themes:** mixed signals, neutral regulatory, balanced risk\n**Overall Tone:** neutral\n**Confidence:** 0.65"
                )

        if "timeline" in prompt_lower or "event" in prompt_lower:
            return (
                "## Event Timeline\n\n"
                "1. **[Day -3]** Earnings announcement — reported EPS beat consensus by 8%, revenue up 12% YoY\n"
                "2. **[Day -2]** Major institution issued upgrade — target raised from $150 to $175\n"
                "3. **[Day -1]** Regulatory filing showed increased institutional ownership\n"
                "4. **[Day 0]** No material news; price action consolidation on elevated volume\n\n"
                "**Narrative:** Recent developments suggest constructive fundamental backdrop with "
                "institutional support strengthening. No adverse catalysts identified in the near term."
            )

        if "explain" in prompt_lower or "bias" in prompt_lower or "why" in prompt_lower:
            return (
                "## Bias Explanation\n\n"
                "The current directional bias reflects a confluence of positive signals:\n\n"
                "- **Earnings strength**: Beat expectations with expanding margins\n"
                "- **Institutional flow**: Major buy-side institutions increased positions\n"
                "- **Price action**: Momentum indicators turned constructive above key moving averages\n\n"
                "**Key Drivers:** earnings beats, institutional accumulation, technical breakout\n"
                "**Confidence Boost:** Multiple independent sources confirming bullish signal\n"
                "**Caveats:** Macro headwinds could limit upside; sentiment can reverse quickly\n"
                "**Alternative Scenario:** A risk-off event (macro shock or earnings miss) would shift bias to neutral"
            )

        if "commentary" in prompt_lower or "outlook" in prompt_lower:
            return (
                "## Market Commentary\n\n"
                "**Headline:** Constructive outlook supported by fundamentals and technical setup\n\n"
                "**Body:** Recent price action shows resilience with higher lows forming on the daily "
                "chart. Volume analysis suggests accumulation rather than distribution. Institutional "
                "interest appears sustained based on options activity and dark pool data.\n\n"
                "**Sentiment:** Net positive with cautious optimism\n"
                "**Key Levels:** Support at the 20-day MA; resistance at the 52-week high\n"
                "**Catalyst Outlook:** Upcoming earnings and macro data could serve as near-term catalysts"
            )

        # Default response
        return (
            "Based on recent market data and news flow, the current market conditions "
            "suggest a balanced risk/reward profile. Further monitoring is recommended "
            "as new information becomes available."
        )


class OpenAILLMClient:
    """OpenAI GPT-powered LLM client. Requires OPENAI_API_KEY environment variable."""

    def __init__(self, model: str = "gpt-4o-mini") -> None:
        self.model = model
        self._client: Any = None

    def _get_client(self) -> Any:
        """Lazy-load the OpenAI client."""
        if self._client is None:
            import openai

            self._client = openai.OpenAI(api_key=os.environ.get("OPENAI_API_KEY"))
        return self._client

    def complete(self, prompt: str, max_tokens: int = 500) -> str:
        """Send a prompt to OpenAI and return the completion."""
        client = self._get_client()
        response = client.chat.completions.create(
            model=self.model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.3,
        )
        return response.choices[0].message.content


# ─────────────────────────────────────────────────────────────────────────────
# LLM Interpretation Service
# ─────────────────────────────────────────────────────────────────────────────


class LLMInterpretationService:
    """High-order NLP/LLM interpretation built on IntelligenceEngine output (IN-07).

    Composes with IntelligenceEngine to provide:
    - LLM-powered document summarization
    - Event timeline construction
    - Explainable directional bias
    - Natural language market commentary
    """

    def __init__(
        self,
        intelligence_engine: IntelligenceEngine,
        llm_client: LLMClient | None = None,
    ) -> None:
        self._intel = intelligence_engine
        self._llm = llm_client or MockLLMClient()

    @classmethod
    def create_with_provider(
        cls,
        intelligence_engine: IntelligenceEngine,
        provider: LLMProvider = LLMProvider.MOCK,
        **kwargs: Any,
    ) -> LLMInterpretationService:
        """Factory method to create service with specified LLM provider."""
        if provider == LLMProvider.OPENAI:
            client: LLMClient = OpenAILLMClient(**kwargs)
        else:
            client = MockLLMClient()
        return cls(intelligence_engine=intelligence_engine, llm_client=client)

    def summarize_documents(
        self,
        instrument_id: str,
        window: timedelta = timedelta(days=7),
        as_of: datetime | None = None,
    ) -> LLMSummary:
        """Generate an LLM summary of recent documents for an instrument."""
        as_of = as_of or datetime.now(timezone.utc)
        cutoff = as_of - window

        docs = [
            doc
            for doc in self._intel.documents.values()
            if doc.instrument_id == instrument_id and cutoff <= doc.published_at <= as_of
        ]

        if not docs:
            return LLMSummary(
                instrument_id=instrument_id,
                summary_text="No recent documents found for this instrument.",
                key_themes=[],
                overall_tone="neutral",
                confidence=0.0,
                document_count=0,
                generated_at=datetime.now(timezone.utc),
            )

        # Build context for LLM
        doc_summaries = "\n".join(
            f"- [{doc.source}] {doc.title} ({doc.published_at.strftime('%Y-%m-%d')})" for doc in docs
        )
        scores = [self._intel.sentiment_results.get(doc.document_id) for doc in docs]
        avg_score = sum(s.score for s in scores if s) / len(scores) if scores else 0.0

        prompt = f"""Summarize the following news/documents for {instrument_id} and provide a structured analysis.

Documents:
{doc_summaries}

Average sentiment score: {avg_score:.3f}
Document count: {len(docs)}

Provide a summary with:
- Key themes (bullet points)
- Overall tone (bullish/bearish/neutral/mixed)
- Confidence level (0-1)

Be concise and analytical."""

        response = self._llm.complete(prompt, max_tokens=400)
        tone = "bullish" if avg_score > 0.1 else "bearish" if avg_score < -0.1 else "neutral"
        if "mixed" in response.lower():
            tone = "mixed"

        return LLMSummary(
            instrument_id=instrument_id,
            summary_text=response,
            key_themes=self._extract_themes(response),
            overall_tone=tone,
            confidence=min(1.0, 0.3 + 0.1 * len(docs)),
            document_count=len(docs),
            generated_at=datetime.now(timezone.utc),
        )

    def build_event_timeline(
        self,
        instrument_id: str,
        window: timedelta = timedelta(days=30),
        as_of: datetime | None = None,
    ) -> EventTimeline:
        """Build a chronological event timeline with LLM-generated narrative."""
        as_of = as_of or datetime.now(timezone.utc)
        cutoff = as_of - window

        docs = sorted(
            [
                doc
                for doc in self._intel.documents.values()
                if doc.instrument_id == instrument_id and cutoff <= doc.published_at <= as_of
            ],
            key=lambda d: d.published_at,
        )

        if not docs:
            return EventTimeline(instrument_id=instrument_id, clusters=[], narrative="No events found.")

        # Group documents by type
        clusters: list[EventCluster] = []
        for doc in docs:
            sentiment = self._intel.sentiment_results.get(doc.document_id)
            score = sentiment.score if sentiment else 0.0
            clusters.append(
                EventCluster(
                    cluster_id=f"evt_{doc.document_id[:12]}",
                    headline=doc.title,
                    description=doc.content[:200] + "..." if len(doc.content) > 200 else doc.content,
                    event_type=str(doc.event_tag) if doc.event_tag else "other",
                    sentiment_impact=score,
                    affected_instruments=[instrument_id],
                    source_documents=[doc.document_id],
                    time_range_start=doc.published_at,
                    time_range_end=doc.published_at,
                )
            )

        # Generate LLM narrative
        timeline_entries = "\n".join(
            f"- [{c.time_range_start.strftime('%Y-%m-%d')}] {c.event_type}: {c.headline} (impact: {c.sentiment_impact:+.2f})"
            for c in clusters
        )
        prompt = f"""Analyze the following event timeline for {instrument_id} and provide a coherent narrative.

Events:
{timeline_entries}

Provide a 2-3 sentence narrative explaining how these events connect and what they suggest about the instrument's near-term outlook."""

        narrative = self._llm.complete(prompt, max_tokens=300)

        return EventTimeline(
            instrument_id=instrument_id,
            clusters=clusters,
            narrative=narrative,
            generated_at=datetime.now(timezone.utc),
        )

    def explain_bias(
        self,
        bias: DirectionalBias,
        window: timedelta = timedelta(days=7),
    ) -> BiasExplanation:
        """Generate a human-readable explanation of a DirectionalBias signal."""
        docs = [
            doc
            for doc in self._intel.documents.values()
            if doc.instrument_id == bias.instrument_id
            and bias.as_of - window <= doc.published_at <= bias.as_of
        ]

        if docs:
            top_docs = sorted(docs, key=lambda d: self._intel.sentiment_results.get(d.document_id, type("S", (), {"score": 0})()).score, reverse=True)[:3]
            key_drivers = [f"{d.title} ({d.source})" for d in top_docs]
        else:
            key_drivers = ["No supporting documents found in the analysis window"]

        confidence_factors = [
            f"Supporting documents: {bias.supporting_documents}",
            f"Signal confidence: {bias.confidence:.2f}",
        ]
        if bias.confidence > 0.7:
            confidence_factors.append("High confidence: multiple strong signals")
        elif bias.confidence < 0.4:
            confidence_factors.append("Low confidence: limited or conflicting signals")

        risk_cautions = [
            "Sentiment can reverse rapidly on unexpected news",
            "Lexicon-based scoring may miss sarcasm or context",
            "Bias does not account for technical factors or market regime",
        ]

        direction_str = bias.direction.value.upper()
        if bias.direction.value == "long":
            alternative_scenarios = [
                "Risk-off event (macro shock) would shift bias to neutral/short",
                "Earnings miss could quickly reverse positive sentiment",
            ]
        elif bias.direction.value == "short":
            alternative_scenarios = [
                "Positive catalyst (upgrade, partnership) could shift bias to neutral",
                "Market-wide risk-on sentiment could limit downside",
            ]
        else:
            alternative_scenarios = [
                "Strong positive news could shift bias to long",
                "Negative surprise could shift bias to short",
            ]

        prompt = f"""Explain the current {direction_str} directional bias for {bias.instrument_id}.

Signal details:
- Score: {bias.score:.3f}
- Direction: {direction_str}
- Confidence: {bias.confidence:.2f}
- Supporting documents: {bias.supporting_documents}
- Window: {window.days} days

Key drivers:
{chr(10).join(f"- {d}" for d in key_drivers)}

Write a concise explanation (2-3 sentences) of WHY this bias exists, focusing on the underlying drivers."""

        explanation_text = self._llm.complete(prompt, max_tokens=300)

        return BiasExplanation(
            instrument_id=bias.instrument_id,
            explanation_text=explanation_text,
            key_drivers=key_drivers,
            confidence_factors=confidence_factors,
            risk_cautions=risk_cautions,
            alternative_scenarios=alternative_scenarios,
            generated_at=datetime.now(timezone.utc),
        )

    def generate_commentary(
        self,
        instrument_id: str,
        window: timedelta = timedelta(days=7),
        as_of: datetime | None = None,
    ) -> MarketCommentary:
        """Generate natural language market commentary for an instrument."""
        as_of = as_of or datetime.now(timezone.utc)

        # Get current bias
        bias = self._intel.directional_bias(
            instrument_id, as_of=as_of, window=window
        )

        # Get sentiment summary
        summary = self.summarize_documents(instrument_id, window, as_of)

        direction_str = bias.direction.value.upper()
        if bias.direction.value == "long":
            catalyst = "Upgrade announcements, earnings beats, or positive regulatory news could extend gains"
            level = "Support near recent lows; resistance at prior highs"
        elif bias.direction.value == "short":
            catalyst = "Downgrades, regulatory headwinds, or macro risk-off could accelerate weakness"
            level = "Resistance at recent highs; support at key moving averages"
        else:
            catalyst = "Clear catalyst needed to establish directional conviction"
            level = "Consolidation likely; range-bound until new catalyst"

        prompt = f"""Write a brief market commentary for {instrument_id} based on the following data:

- Directional bias: {direction_str} (score: {bias.score:.3f}, confidence: {bias.confidence:.2f})
- Sentiment tone: {summary.overall_tone}
- Recent themes: {', '.join(summary.key_themes[:3]) if summary.key_themes else 'none identified'}
- Document count: {summary.document_count}

Write a 3-4 sentence market commentary with a headline, body, and near-term outlook. Be analytical and concise."""

        body = self._llm.complete(prompt, max_tokens=400)

        return MarketCommentary(
            instrument_id=instrument_id,
            headline=f"{instrument_id}: {direction_str} bias — {summary.overall_tone} tone",
            body=body,
            sentiment_summary=f"{summary.overall_tone.title()} with {bias.confidence:.0%} confidence",
            key_level=level,
            catalyst_outlook=catalyst,
            generated_at=datetime.now(timezone.utc),
        )

    def _extract_themes(self, text: str) -> list[str]:
        """Extract key themes from LLM response text."""
        # Simple keyword-based theme extraction
        theme_keywords = {
            "earnings": ["earnings", "eps", "revenue", "profit", "beat", "miss"],
            "regulatory": ["regulatory", "sec", "approval", "investigation", "compliance"],
            "institutional": ["institution", "upgrade", "downgrade", "target", "position"],
            "macro": ["macro", "fed", "rate", "inflation", "gdp", "unemployment"],
            "product": ["product", "launch", "sales", "contract", "pipeline"],
            "leadership": ["ceo", "cfo", "executive", "management", "board"],
        }
        text_lower = text.lower()
        found = []
        for theme, keywords in theme_keywords.items():
            if any(kw in text_lower for kw in keywords):
                found.append(theme)
        return found[:5]


# ─────────────────────────────────────────────────────────────────────────────
# Import IntelligenceEngine locally to avoid circular imports
# ─────────────────────────────────────────────────────────────────────────────
from quant_exchange.intelligence.service import IntelligenceEngine
