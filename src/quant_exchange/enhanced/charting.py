"""Advanced charting system service (CHART-01 ~ CHART-07).

Covers:
- Multi-chart types: K-line, line, area, Heikin-Ashi
- Technical indicator overlays (MA, EMA, MACD, RSI, Bollinger Bands, etc.)
- Annotation system: trendlines, Fibonacci, price annotations
- Multi-chart layouts with cross-chart linking
- Chart state persistence per user
- Chart snapshot/export
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class ChartType(str, Enum):
    KLINE = "kline"
    LINE = "line"
    AREA = "area"
    HEIKIN_ASHI = "heikin_ashi"
    CANDLESTICK = "candlestick"
    BAR = "bar"


class IndicatorType(str, Enum):
    MA = "ma"           # Moving Average
    EMA = "ema"         # Exponential Moving Average
    MACD = "macd"       # MACD
    RSI = "rsi"         # Relative Strength Index
    BOLLINGER = "bollinger"  # Bollinger Bands
    ATR = "atr"         # Average True Range
    VOLUME = "volume"
    OBV = "obv"         # On-Balance Volume
    STOCH = "stoch"     # Stochastic
    VWAP = "vwap"       # Volume Weighted Average Price


class AnnotationType(str, Enum):
    TRENDLINE = "trendline"
    FIBONACCI = "fibonacci"
    HORIZONTAL = "horizontal"
    VERTICAL = "vertical"
    TEXT = "text"
    PRICE_RANGE = "price_range"
    ARROW = "arrow"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class IndicatorConfig:
    """Configuration for a technical indicator on a chart."""

    indicator_type: IndicatorType
    params: dict[str, float]  # e.g., {"period": 20} for MA
    color: str = "#2196F3"
    panel: int = 0  # 0 = main panel, 1+ = sub-panels
    visible: bool = True


@dataclass(slots=True)
class ChartAnnotation:
    """An annotation on a chart."""

    annotation_id: str
    chart_id: str
    annotation_type: AnnotationType
    # For trendlines
    start_time: datetime | None = None
    end_time: datetime | None = None
    start_price: float | None = None
    end_price: float | None = None
    # For horizontal/vertical
    price: float | None = None
    time: datetime | None = None
    # For text
    text: str = ""
    # For fibonacci
    levels: tuple[float, ...] = field(default_factory=lambda: (0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0))
    color: str = "#FF9800"
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class ChartPanel:
    """A panel within a chart layout."""

    panel_id: str
    chart_id: str
    indicators: tuple[IndicatorConfig, ...] = field(default_factory=tuple)
    height_ratio: float = 1.0  # relative height within chart


@dataclass(slots=True)
class Chart:
    """A complete chart definition."""

    chart_id: str
    user_id: str
    instrument_id: str
    chart_type: ChartType
    time_range_start: datetime | None = None
    time_range_end: datetime | None = None
    period: str = "1d"  # 1m, 5m, 15m, 1h, 4h, 1d, 1w
    panels: tuple[ChartPanel, ...] = field(default_factory=tuple)
    is_default: bool = False  # default chart for this instrument
    name: str = ""
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class ChartSnapshot:
    """A saved snapshot of a chart state."""

    snapshot_id: str
    chart_id: str
    user_id: str
    image_data: str = ""  # base64 encoded PNG
    data_json: str = ""   # JSON serialized chart data
    description: str = ""
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class ChartComparison:
    """Multi-instrument comparison chart."""

    comparison_id: str
    user_id: str
    instrument_ids: tuple[str, ...]
    chart_type: ChartType
    normalized: bool = True  # normalize to % change from start
    name: str = ""
    created_at: str = field(default_factory=_now)


# ─────────────────────────────────────────────────────────────────────────────
# Charting Service
# ─────────────────────────────────────────────────────────────────────────────

class ChartingService:
    """Advanced charting system service (CHART-01 ~ CHART-07).

    Provides:
    - Multi-chart types: K-line, line, area, Heikin-Ashi
    - Technical indicator overlays (MA, EMA, MACD, RSI, Bollinger, etc.)
    - Annotation system: trendlines, Fibonacci, price annotations
    - Multi-chart layouts with cross-chart linking
    - Chart state persistence per user
    - Chart snapshot/export
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._charts: dict[str, Chart] = {}
        self._annotations: dict[str, list[ChartAnnotation]] = defaultdict(list)
        self._snapshots: dict[str, ChartSnapshot] = {}
        self._comparisons: dict[str, ChartComparison] = {}
        self._user_charts: dict[str, list[str]] = defaultdict(list)  # user_id -> chart_ids

    # ── Chart Management ─────────────────────────────────────────────────

    def create_chart(
        self,
        user_id: str,
        instrument_id: str,
        chart_type: ChartType,
        period: str = "1d",
        panels: list[ChartPanel] | None = None,
        is_default: bool = False,
        name: str = "",
    ) -> Chart:
        """Create a new chart."""
        chart_id = f"chart:{uuid.uuid4().hex[:12]}"
        chart = Chart(
            chart_id=chart_id,
            user_id=user_id,
            instrument_id=instrument_id,
            chart_type=chart_type,
            period=period,
            panels=tuple(panels) if panels else (),
            is_default=is_default,
            name=name or f"{instrument_id} {chart_type.value} {period}",
        )
        self._charts[chart_id] = chart
        self._user_charts[user_id].append(chart_id)
        return chart

    def add_panel(
        self,
        chart_id: str,
        indicators: list[IndicatorConfig],
        height_ratio: float = 1.0,
    ) -> ChartPanel | None:
        """Add a panel to an existing chart."""
        chart = self._charts.get(chart_id)
        if not chart:
            return None
        panel_id = f"panel:{uuid.uuid4().hex[:8]}"
        panel = ChartPanel(
            panel_id=panel_id,
            chart_id=chart_id,
            indicators=tuple(indicators),
            height_ratio=height_ratio,
        )
        chart.panels = chart.panels + (panel,)
        chart.updated_at = _now()
        return panel

    def get_chart(self, chart_id: str) -> Chart | None:
        """Get a chart by ID."""
        return self._charts.get(chart_id)

    def get_user_charts(
        self,
        user_id: str,
        instrument_id: str | None = None,
    ) -> list[Chart]:
        """Get all charts for a user."""
        chart_ids = self._user_charts.get(user_id, [])
        charts = [self._charts[cid] for cid in chart_ids if cid in self._charts]
        if instrument_id:
            charts = [c for c in charts if c.instrument_id == instrument_id]
        return charts

    def update_chart(
        self,
        chart_id: str,
        time_range_start: datetime | None = None,
        time_range_end: datetime | None = None,
        period: str | None = None,
    ) -> Chart | None:
        """Update chart time range or period."""
        chart = self._charts.get(chart_id)
        if not chart:
            return None
        if time_range_start is not None:
            chart.time_range_start = time_range_start
        if time_range_end is not None:
            chart.time_range_end = time_range_end
        if period is not None:
            chart.period = period
        chart.updated_at = _now()
        return chart

    def delete_chart(self, chart_id: str, user_id: str) -> bool:
        """Delete a chart (only owner)."""
        chart = self._charts.get(chart_id)
        if not chart or chart.user_id != user_id:
            return False
        del self._charts[chart_id]
        self._user_charts[user_id] = [c for c in self._user_charts[user_id] if c != chart_id]
        return True

    # ── Annotations ──────────────────────────────────────────────────────

    def add_annotation(
        self,
        chart_id: str,
        annotation_type: AnnotationType,
        **kwargs,
    ) -> ChartAnnotation | None:
        """Add an annotation to a chart."""
        chart = self._charts.get(chart_id)
        if not chart:
            return None
        annotation = ChartAnnotation(
            annotation_id=f"ann:{uuid.uuid4().hex[:12]}",
            chart_id=chart_id,
            annotation_type=annotation_type,
            **kwargs,
        )
        self._annotations[chart_id].append(annotation)
        return annotation

    def add_trendline(
        self,
        chart_id: str,
        start_time: datetime,
        end_time: datetime,
        start_price: float,
        end_price: float,
        color: str = "#FF9800",
    ) -> ChartAnnotation | None:
        """Add a trendline annotation."""
        return self.add_annotation(
            chart_id=chart_id,
            annotation_type=AnnotationType.TRENDLINE,
            start_time=start_time,
            end_time=end_time,
            start_price=start_price,
            end_price=end_price,
            color=color,
        )

    def add_fibonacci(
        self,
        chart_id: str,
        start_time: datetime,
        end_time: datetime,
        start_price: float,
        end_price: float,
        levels: tuple[float, ...] | None = None,
        color: str = "#FF9800",
    ) -> ChartAnnotation | None:
        """Add Fibonacci retracement annotation."""
        return self.add_annotation(
            chart_id=chart_id,
            annotation_type=AnnotationType.FIBONACCI,
            start_time=start_time,
            end_time=end_time,
            start_price=start_price,
            end_price=end_price,
            levels=levels or (0, 0.236, 0.382, 0.5, 0.618, 0.786, 1.0),
            color=color,
        )

    def add_horizontal_line(
        self,
        chart_id: str,
        price: float,
        time: datetime | None = None,
        color: str = "#4CAF50",
    ) -> ChartAnnotation | None:
        """Add a horizontal price line annotation."""
        return self.add_annotation(
            chart_id=chart_id,
            annotation_type=AnnotationType.HORIZONTAL,
            price=price,
            time=time,
            color=color,
        )

    def add_text_annotation(
        self,
        chart_id: str,
        text: str,
        time: datetime,
        price: float,
        color: str = "#9C27B0",
    ) -> ChartAnnotation | None:
        """Add a text annotation."""
        return self.add_annotation(
            chart_id=chart_id,
            annotation_type=AnnotationType.TEXT,
            text=text,
            time=time,
            price=price,
            color=color,
        )

    def get_annotations(self, chart_id: str) -> list[ChartAnnotation]:
        """Get all annotations for a chart."""
        return self._annotations.get(chart_id, [])

    def delete_annotation(self, chart_id: str, annotation_id: str) -> bool:
        """Delete an annotation."""
        annotations = self._annotations.get(chart_id, [])
        for i, ann in enumerate(annotations):
            if ann.annotation_id == annotation_id:
                annotations.pop(i)
                return True
        return False

    # ── Indicators ────────────────────────────────────────────────────────

    def compute_ma(self, prices: list[float], period: int) -> list[float | None]:
        """Compute simple moving average."""
        if len(prices) < period:
            return [None] * len(prices)
        result = [None] * (period - 1)
        window_sum = sum(prices[:period])
        result.append(window_sum / period)
        for i in range(period, len(prices)):
            window_sum = window_sum - prices[i - period] + prices[i]
            result.append(window_sum / period)
        return result

    def compute_ema(self, prices: list[float], period: int) -> list[float | None]:
        """Compute exponential moving average."""
        if len(prices) < period:
            return [None] * len(prices)
        multiplier = 2.0 / (period + 1)
        result: list[float | None] = [None] * (period - 1)
        ema = sum(prices[:period]) / period
        result.append(ema)
        for i in range(period, len(prices)):
            ema = (prices[i] - ema) * multiplier + ema
            result.append(ema)
        return result

    def compute_rsi(self, prices: list[float], period: int = 14) -> list[float | None]:
        """Compute Relative Strength Index."""
        if len(prices) < period + 1:
            return [None] * len(prices)

        changes = [prices[i] - prices[i - 1] for i in range(1, len(prices))]
        gains = [max(c, 0) for c in changes]
        losses = [max(-c, 0) for c in changes]

        result: list[float | None] = [None] * period
        avg_gain = sum(gains[:period]) / period
        avg_loss = sum(losses[:period]) / period

        if avg_loss == 0:
            result.append(100.0)
        else:
            rs = avg_gain / avg_loss
            result.append(100.0 - 100.0 / (1.0 + rs))

        for i in range(period, len(changes)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                result.append(100.0)
            else:
                rs = avg_gain / avg_loss
                result.append(100.0 - 100.0 / (1.0 + rs))

        return result

    def compute_macd(
        self,
        prices: list[float],
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> tuple[list[float | None], list[float | None], list[float | None]]:
        """Compute MACD, signal line, and histogram."""
        ema_fast = self.compute_ema(prices, fast)
        ema_slow = self.compute_ema(prices, slow)

        macd_line: list[float | None] = []
        for f, s in zip(ema_fast, ema_slow):
            if f is None or s is None:
                macd_line.append(None)
            else:
                macd_line.append(f - s)

        # Signal line = EMA of MACD
        valid_macd = [v for v in macd_line if v is not None]
        if len(valid_macd) < signal:
            signal_line = [None] * len(macd_line)
            histogram = [None] * len(macd_line)
            return macd_line, signal_line, histogram

        signal_ema_start = valid_macd[0]
        signal_line: list[float | None] = [None] * (len(valid_macd) - 1)
        signal_line.append(signal_ema_start)
        multiplier = 2.0 / (signal + 1)
        for i in range(signal, len(valid_macd)):
            ema = (valid_macd[i] - signal_line[-1]) * multiplier + signal_line[-1]
            signal_line.append(ema)

        # Align signal_line back to original length
        offset = len(macd_line) - len(signal_line)
        aligned_signal: list[float | None] = [None] * offset + signal_line

        histogram = [None] * len(macd_line)
        for i in range(len(macd_line)):
            if macd_line[i] is not None and aligned_signal[i] is not None:
                histogram[i] = macd_line[i] - aligned_signal[i]

        return macd_line, aligned_signal, histogram

    def compute_bollinger_bands(
        self,
        prices: list[float],
        period: int = 20,
        num_std: float = 2.0,
    ) -> tuple[list[float | None], list[float | None], list[float | None]]:
        """Compute Bollinger Bands (upper, middle, lower)."""
        import math
        if len(prices) < period:
            return [None] * len(prices), [None] * len(prices), [None] * len(prices)

        middle = self.compute_ma(prices, period)
        upper = [None] * (period - 1)
        lower = [None] * (period - 1)

        for i in range(period - 1, len(prices)):
            window = prices[i - period + 1:i + 1]
            mean = middle[i]
            if mean is not None:
                std = math.sqrt(sum((p - mean) ** 2 for p in window) / period)
                upper.append(mean + num_std * std)
                lower.append(mean - num_std * std)
            else:
                upper.append(None)
                lower.append(None)

        return upper, middle, lower

    def compute_atr(
        self,
        highs: list[float],
        lows: list[float],
        closes: list[float],
        period: int = 14,
    ) -> list[float | None]:
        """Compute Average True Range."""
        if len(highs) < 2:
            return [None] * len(highs)

        true_ranges = [highs[0] - lows[0]]
        for i in range(1, len(highs)):
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1]),
            )
            true_ranges.append(tr)

        if len(true_ranges) < period:
            return [None] * len(true_ranges)

        result: list[float | None] = [None] * (period - 1)
        atr = sum(true_ranges[:period]) / period
        result.append(atr)
        for i in range(period, len(true_ranges)):
            atr = (atr * (period - 1) + true_ranges[i]) / period
            result.append(atr)
        return result

    # ── Chart Snapshots ──────────────────────────────────────────────────

    def save_snapshot(
        self,
        chart_id: str,
        user_id: str,
        image_data: str = "",
        data_json: str = "",
        description: str = "",
    ) -> ChartSnapshot:
        """Save a chart snapshot."""
        snapshot_id = f"snap:{uuid.uuid4().hex[:12]}"
        snapshot = ChartSnapshot(
            snapshot_id=snapshot_id,
            chart_id=chart_id,
            user_id=user_id,
            image_data=image_data,
            data_json=data_json,
            description=description,
        )
        self._snapshots[snapshot_id] = snapshot
        return snapshot

    def get_snapshots(self, chart_id: str) -> list[ChartSnapshot]:
        """Get all snapshots for a chart."""
        return [s for s in self._snapshots.values() if s.chart_id == chart_id]

    # ── Comparison Charts ────────────────────────────────────────────────

    def create_comparison(
        self,
        user_id: str,
        instrument_ids: list[str],
        chart_type: ChartType = ChartType.LINE,
        normalized: bool = True,
        name: str = "",
    ) -> ChartComparison:
        """Create a multi-instrument comparison chart."""
        comparison_id = f"comp:{uuid.uuid4().hex[:12]}"
        comparison = ChartComparison(
            comparison_id=comparison_id,
            user_id=user_id,
            instrument_ids=tuple(instrument_ids),
            chart_type=chart_type,
            normalized=normalized,
            name=name or f"Comparison {', '.join(instrument_ids)}",
        )
        self._comparisons[comparison_id] = comparison
        return comparison

    def get_comparison(self, comparison_id: str) -> ChartComparison | None:
        """Get a comparison chart."""
        return self._comparisons.get(comparison_id)

    def get_user_comparisons(self, user_id: str) -> list[ChartComparison]:
        """Get all comparison charts for a user."""
        return [c for c in self._comparisons.values() if c.user_id == user_id]

    # ── Chart Layouts ──────────────────────────────────────────────────

    def create_layout(
        self,
        user_id: str,
        name: str,
        chart_ids: list[str],
    ) -> dict[str, Any]:
        """Create a multi-chart layout (grid/rows configuration)."""
        layout_id = f"layout:{uuid.uuid4().hex[:12]}"
        return {
            "layout_id": layout_id,
            "user_id": user_id,
            "name": name,
            "chart_ids": chart_ids,
            "grid_config": {
                "rows": 1,
                "cols": len(chart_ids),
                "linked_crosshair": True,
                "sync_time_range": True,
            },
            "created_at": _now(),
        }
