"""Stock universe, F10 screening, sector filtering, and comparison services."""

from __future__ import annotations

import csv
import gzip
import hashlib
import random
from dataclasses import asdict, dataclass, replace
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

from quant_exchange.core.models import Instrument, MarketType


@dataclass(slots=True, frozen=True)
class StockProfile:
    """Normalized stock profile used by the stock screener and F10 workbench."""

    instrument_id: str
    symbol: str
    company_name: str
    market_region: str
    exchange_code: str
    board: str
    sector: str
    industry: str
    concepts: tuple[str, ...] = ()
    f10_summary: str = ""
    main_business: str = ""
    products_services: str = ""
    competitive_advantages: str = ""
    risks: str = ""
    pe_ttm: float | None = None
    pb: float | None = None
    roe: float | None = None
    revenue_growth: float | None = None
    net_profit_growth: float | None = None
    gross_margin: float | None = None
    net_margin: float | None = None
    debt_to_asset: float | None = None
    dividend_yield: float | None = None
    operating_cashflow_growth: float | None = None
    free_cashflow_margin: float | None = None
    current_ratio: float | None = None
    quick_ratio: float | None = None
    interest_coverage: float | None = None
    asset_turnover: float | None = None
    market_cap: float | None = None
    float_market_cap: float | None = None
    last_price: float | None = None
    listing_date: date | None = None
    currency: str = "USD"

    def searchable_text(self) -> str:
        """Return the combined text blob used by free-text and F10 filtering."""

        parts = [
            self.symbol,
            self.company_name,
            self.board,
            self.sector,
            self.industry,
            " ".join(self.concepts),
            self.f10_summary,
            self.main_business,
            self.products_services,
            self.competitive_advantages,
            self.risks,
        ]
        return " ".join(part for part in parts if part).lower()


class StockDirectoryService:
    """Maintain stock master data and provide flexible screening operations."""

    MARKET_TIMEZONES = {
        "CN": "Asia/Shanghai",
        "HK": "Asia/Hong_Kong",
        "US": "America/New_York",
    }

    def __init__(
        self,
        persistence=None,
        registrar: Callable[[Instrument], None] | None = None,
        history_root: str | Path | None = None,
    ) -> None:
        self.persistence = persistence
        self.registrar = registrar
        self.instruments: dict[str, Instrument] = {}
        self.profiles: dict[str, StockProfile] = {}
        self.history_root = Path(history_root) if history_root is not None else Path(__file__).resolve().parents[3] / "data"
        self._history_cache: dict[str, tuple[str, list[dict[str, Any]]]] = {}

    def bootstrap_demo_directory(self) -> list[dict[str, Any]]:
        """Load a representative A-share, Hong Kong, and US stock directory."""

        if self.profiles:
            return self.list_stocks()
        for instrument, profile in self._demo_stocks():
            self.upsert_stock(instrument, profile)
        for instrument_id in self.profiles:
            self._ensure_financial_history(instrument_id)
        return self.list_stocks()

    def bootstrap_persisted_or_demo_directory(self) -> list[dict[str, Any]]:
        """Prefer persisted stock master data and only fall back to the demo universe when needed."""

        if self.profiles:
            return self.list_stocks({"limit": 50})
        loaded_count = self.load_from_persistence()
        if loaded_count > 0:
            # Keep the rich demo profiles as curated overlays for the best-known reference names.
            for instrument, profile in self._demo_stocks():
                self.upsert_stock(instrument, profile)
            return self.list_stocks({"limit": 50})
        return self.bootstrap_demo_directory()

    def upsert_stock(self, instrument: Instrument, profile: StockProfile) -> dict[str, Any]:
        """Insert or update one stock instrument together with its F10 profile."""

        if instrument.market != MarketType.STOCK:
            raise ValueError("StockDirectoryService only accepts stock instruments.")
        instrument, profile = self._canonicalize_models(instrument, profile)
        self._store_stock(instrument, profile, persist=True, register=True)
        return self.get_stock(profile.instrument_id)

    def load_from_persistence(self) -> int:
        """Load persisted stock master data into the in-memory directory."""

        if self.persistence is None:
            return 0
        instrument_rows = self.persistence.fetch_all(
            "ref_instruments",
            where="market_type = :market_type",
            params={"market_type": MarketType.STOCK.value},
        )
        if not instrument_rows:
            return 0
        profile_rows = self.persistence.fetch_all("ref_stock_profiles")
        profiles_by_instrument = {row["instrument_id"]: row["payload"] for row in profile_rows}
        loaded = 0
        for row in instrument_rows:
            instrument_id = row["instrument_id"]
            profile_payload = profiles_by_instrument.get(instrument_id)
            if profile_payload is None:
                continue
            instrument, profile = self._canonicalize_models(
                self._instrument_from_payload(row["payload"]),
                self._profile_from_payload(profile_payload),
            )
            self._store_stock(instrument, profile, persist=False, register=True)
            loaded += 1
        return loaded

    def _store_stock(
        self,
        instrument: Instrument,
        profile: StockProfile,
        *,
        persist: bool,
        register: bool,
    ) -> None:
        """Write one stock into memory and optionally into persistence."""

        self.instruments[instrument.instrument_id] = instrument
        self.profiles[profile.instrument_id] = profile
        if register and self.registrar is not None:
            self.registrar(instrument)
        if persist and self.persistence is not None:
            instrument_payload = {
                "instrument_id": instrument.instrument_id,
                "symbol": instrument.symbol,
                "market": instrument.market.value,
                "market_region": instrument.market_region,
                "instrument_type": instrument.instrument_type,
                "settlement_cycle": instrument.settlement_cycle,
                "short_sellable": instrument.short_sellable,
                "trading_rules": instrument.trading_rules,
            }
            self.persistence.upsert_record(
                "ref_instruments",
                "instrument_id",
                instrument.instrument_id,
                instrument_payload,
                extra_columns={
                    "symbol": instrument.symbol,
                    "market_type": instrument.market.value,
                    "market_region": instrument.market_region,
                    "instrument_type": instrument.instrument_type,
                    "status": "ACTIVE",
                },
            )
            payload = self._serialize_profile(profile)
            self.persistence.upsert_record(
                "ref_stock_profiles",
                "instrument_id",
                profile.instrument_id,
                payload,
                extra_columns={
                    "symbol": profile.symbol,
                    "company_name": profile.company_name,
                    "market_region": profile.market_region,
                    "exchange_code": profile.exchange_code,
                    "board": profile.board,
                    "sector": profile.sector,
                    "industry": profile.industry,
                    "pe_ttm": profile.pe_ttm,
                    "roe": profile.roe,
                    "market_cap": profile.market_cap,
                },
            )

    def get_stock(self, instrument_id: str) -> dict[str, Any]:
        """Return one stock record enriched with instrument and profile data."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        return self.get_stock_core(instrument_id) | {"financial_analysis": self.analyze_financials(instrument_id)}

    def get_stock_core(self, instrument_id: str) -> dict[str, Any]:
        """Return one stock record without computed analysis fields."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        instrument = self.instruments[instrument_id]
        profile = self.profiles[instrument_id]
        return {
            "instrument_id": instrument.instrument_id,
            "symbol": instrument.symbol,
            "company_name": profile.company_name,
            "market_region": profile.market_region,
            "exchange_code": profile.exchange_code,
            "board": profile.board,
            "sector": profile.sector,
            "industry": profile.industry,
            "concepts": list(profile.concepts),
            "f10_summary": profile.f10_summary,
            "main_business": profile.main_business,
            "products_services": profile.products_services,
            "competitive_advantages": profile.competitive_advantages,
            "risks": profile.risks,
            "pe_ttm": profile.pe_ttm,
            "pb": profile.pb,
            "roe": profile.roe,
            "revenue_growth": profile.revenue_growth,
            "net_profit_growth": profile.net_profit_growth,
            "gross_margin": profile.gross_margin,
            "net_margin": profile.net_margin,
            "debt_to_asset": profile.debt_to_asset,
            "dividend_yield": profile.dividend_yield,
            "operating_cashflow_growth": profile.operating_cashflow_growth,
            "free_cashflow_margin": profile.free_cashflow_margin,
            "current_ratio": profile.current_ratio,
            "quick_ratio": profile.quick_ratio,
            "interest_coverage": profile.interest_coverage,
            "asset_turnover": profile.asset_turnover,
            "market_cap": profile.market_cap,
            "float_market_cap": profile.float_market_cap,
            "last_price": profile.last_price,
            "listing_date": profile.listing_date.isoformat() if profile.listing_date else None,
            "currency": profile.currency,
            "instrument": {
                "tick_size": instrument.tick_size,
                "lot_size": instrument.lot_size,
                "settlement_cycle": instrument.settlement_cycle,
                "short_sellable": instrument.short_sellable,
                "trading_sessions": instrument.trading_sessions,
                "trading_rules": instrument.trading_rules,
            },
            **self._live_quote_fields(instrument_id),
        }

    def list_stocks(self, filters: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        """Return all stocks that match the provided filters."""

        filters = filters or {}
        matched = self._filtered_records(filters)
        sort_key = str(filters.get("sort_by") or "symbol")
        reverse = self._parse_bool(filters.get("sort_desc"), default=sort_key != "symbol")
        matched = self._sort_records(matched, sort_key=sort_key, reverse=reverse)
        offset = int(filters.get("offset") or 0)
        limit = int(filters["limit"]) if filters.get("limit") else None
        if offset:
            matched = matched[offset:]
        return matched[:limit] if limit is not None else matched

    def count_stocks(self, filters: dict[str, Any] | None = None) -> int:
        """Return the number of stocks matching the provided filters."""

        return len(self._filtered_records(filters or {}))

    def universe_summary(self, *, featured_limit: int = 24) -> dict[str, Any]:
        """Return a summary view over the full stock universe for the web home page."""

        market_counts: dict[str, int] = {}
        exchange_counts: dict[str, int] = {}
        board_counts: dict[str, int] = {}
        for profile in self.profiles.values():
            market_counts[profile.market_region] = market_counts.get(profile.market_region, 0) + 1
            exchange_counts[profile.exchange_code] = exchange_counts.get(profile.exchange_code, 0) + 1
            board_counts[profile.board] = board_counts.get(profile.board, 0) + 1
        return {
            "total_count": len(self.profiles),
            "market_counts": market_counts,
            "exchange_counts": exchange_counts,
            "board_counts": board_counts,
            "featured_stocks": self.list_stocks({"sort_by": "symbol", "sort_desc": False, "limit": featured_limit}),
        }

    def available_filters(self) -> dict[str, list[str]]:
        """Return filter options for the stock workbench UI."""

        records = [self.get_stock_core(instrument_id) for instrument_id in self.profiles]
        return {
            "market_regions": sorted({record["market_region"] for record in records}),
            "exchange_codes": sorted({record["exchange_code"] for record in records}),
            "boards": sorted({record["board"] for record in records}),
            "sectors": sorted({record["sector"] for record in records}),
            "industries": sorted({record["industry"] for record in records}),
            "concepts": sorted({concept for record in records for concept in record["concepts"]}),
        }

    def compare_stocks(self, left_instrument_id: str, right_instrument_id: str) -> dict[str, Any]:
        """Return side-by-side comparison payload for two stocks."""

        left_instrument_id = self._canonical_instrument_id(left_instrument_id)
        right_instrument_id = self._canonical_instrument_id(right_instrument_id)
        left = self.get_stock(left_instrument_id)
        right = self.get_stock(right_instrument_id)
        metrics = [
            ("市值 / Market Cap", left["market_cap"], right["market_cap"]),
            ("PE(TTM)", left["pe_ttm"], right["pe_ttm"]),
            ("PB", left["pb"], right["pb"]),
            ("ROE", left["roe"], right["roe"]),
            ("营收增速 / Revenue Growth", left["revenue_growth"], right["revenue_growth"]),
            ("净利润增速 / Net Profit Growth", left["net_profit_growth"], right["net_profit_growth"]),
            ("股息率 / Dividend Yield", left["dividend_yield"], right["dividend_yield"]),
            ("资产负债率 / Debt Ratio", left["debt_to_asset"], right["debt_to_asset"]),
            ("毛利率 / Gross Margin", left["gross_margin"], right["gross_margin"]),
            ("净利率 / Net Margin", left["net_margin"], right["net_margin"]),
            ("经营现金流增速 / OCF Growth", left["operating_cashflow_growth"], right["operating_cashflow_growth"]),
            ("自由现金流率 / FCF Margin", left["free_cashflow_margin"], right["free_cashflow_margin"]),
            ("流动比率 / Current Ratio", left["current_ratio"], right["current_ratio"]),
        ]
        financial_scores = [
            ("综合评分 / Overall", left["financial_analysis"]["overall_score"], right["financial_analysis"]["overall_score"]),
            ("估值 / Valuation", left["financial_analysis"]["valuation_score"], right["financial_analysis"]["valuation_score"]),
            ("盈利 / Profitability", left["financial_analysis"]["profitability_score"], right["financial_analysis"]["profitability_score"]),
            ("成长 / Growth", left["financial_analysis"]["growth_score"], right["financial_analysis"]["growth_score"]),
            ("现金流 / Cash Flow", left["financial_analysis"]["cashflow_score"], right["financial_analysis"]["cashflow_score"]),
            ("偿债 / Solvency", left["financial_analysis"]["solvency_score"], right["financial_analysis"]["solvency_score"]),
        ]
        return {"left": left, "right": right, "metrics": metrics, "financial_scores": financial_scores}

    def get_stock_history(self, instrument_id: str, *, limit: int = 120) -> dict[str, Any]:
        """Return historical daily bars for one stock to power the web K-line chart."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        if instrument_id not in self.profiles:
            raise KeyError(instrument_id)
        source, bars = self._history_payload(instrument_id)
        sliced = bars[-max(1, limit) :]
        latest_close = sliced[-1]["close"] if sliced else None
        previous_close = sliced[-2]["close"] if len(sliced) >= 2 else latest_close
        change_pct = 0.0
        if latest_close is not None and previous_close not in (None, 0):
            change_pct = round((latest_close - previous_close) / previous_close * 100, 2)
        return {
            "instrument_id": instrument_id,
            "symbol": self.profiles[instrument_id].symbol,
            "source": source,
            "timeframe": "1d",
            "bars": sliced,
            "summary": {
                "bar_count": len(sliced),
                "latest_close": latest_close,
                "change_pct": change_pct,
                "period_high": max((bar["high"] for bar in sliced), default=None),
                "period_low": min((bar["low"] for bar in sliced), default=None),
            },
        }

    def get_financial_history(self, instrument_id: str, *, limit: int = 8) -> dict[str, Any]:
        """Return persisted financial history snapshots for one stock."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        if instrument_id not in self.profiles:
            raise KeyError(instrument_id)
        self._ensure_financial_history(instrument_id)
        if self.persistence is None:
            snapshots = self._generate_demo_financial_history(instrument_id, periods=max(limit, 5))
        else:
            rows = self.persistence.fetch_all(
                "ref_stock_financial_history",
                where="instrument_id = :instrument_id",
                params={"instrument_id": instrument_id},
                order_by="report_date DESC",
                limit=limit,
            )
            snapshots = [row["payload"] for row in rows]
        return {
            "instrument_id": instrument_id,
            "symbol": self.profiles[instrument_id].symbol,
            "period_type": "FY",
            "snapshots": snapshots,
        }

    def get_minute_bars(self, instrument_id: str, *, limit: int = 240) -> dict[str, Any]:
        """Return persisted one-minute bars for one stock."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        if instrument_id not in self.profiles:
            raise KeyError(instrument_id)
        self._ensure_minute_history(instrument_id, bars=max(limit, 240))
        if self.persistence is None:
            bars = self._generate_demo_minute_bars(instrument_id, bars=limit)
        else:
            rows = self.persistence.fetch_all(
                "mkt_stock_minute_bars",
                where="instrument_id = :instrument_id",
                params={"instrument_id": instrument_id},
                order_by="bar_time DESC",
                limit=limit,
            )
            bars = [row["payload"] for row in reversed(rows) if row.get("payload")]
        return {
            "instrument_id": instrument_id,
            "symbol": self.profiles[instrument_id].symbol,
            "interval": "1m",
            "bars": bars,
        }

    def analyze_financials(self, instrument_id: str) -> dict[str, Any]:
        """Generate a compact financial scorecard and narrative for one stock."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        record = self.get_stock_core(instrument_id)
        valuation_score = self._average_scores(
            [
                self._score_lower_better(record.get("pe_ttm"), [(15, 95), (25, 80), (35, 62), (50, 40)], default=25),
                self._score_lower_better(record.get("pb"), [(2, 92), (4, 78), (8, 60), (15, 40)], default=28),
                self._score_higher_better(record.get("dividend_yield"), [(4, 90), (2, 78), (1, 62), (0, 40)], default=35),
            ]
        )
        profitability_score = self._average_scores(
            [
                self._score_higher_better(record.get("roe"), [(20, 95), (15, 82), (10, 68), (5, 50)], default=30),
                self._score_higher_better(record.get("gross_margin"), [(60, 92), (40, 80), (20, 62), (10, 45)], default=28),
                self._score_higher_better(record.get("net_margin"), [(25, 95), (15, 82), (8, 65), (3, 45)], default=28),
            ]
        )
        growth_score = self._average_scores(
            [
                self._score_higher_better(record.get("revenue_growth"), [(20, 92), (10, 78), (0, 60), (-10, 35)], default=20),
                self._score_higher_better(record.get("net_profit_growth"), [(20, 95), (10, 80), (0, 60), (-10, 35)], default=20),
                self._score_higher_better(record.get("operating_cashflow_growth"), [(20, 90), (10, 76), (0, 58), (-10, 32)], default=20),
            ]
        )
        cashflow_score = self._average_scores(
            [
                self._score_higher_better(record.get("operating_cashflow_growth"), [(20, 92), (10, 78), (0, 60), (-10, 35)], default=22),
                self._score_higher_better(record.get("free_cashflow_margin"), [(15, 92), (8, 76), (3, 58), (0, 42)], default=24),
                self._score_higher_better(record.get("quick_ratio"), [(1.5, 88), (1.0, 74), (0.8, 58), (0.5, 38)], default=28),
            ]
        )
        solvency_score = self._average_scores(
            [
                self._score_lower_better(record.get("debt_to_asset"), [(30, 92), (50, 76), (70, 56), (85, 36)], default=22),
                self._score_higher_better(record.get("current_ratio"), [(2.0, 92), (1.5, 78), (1.0, 60), (0.7, 38)], default=25),
                self._score_higher_better(record.get("interest_coverage"), [(8, 92), (4, 75), (2, 55), (1, 35)], default=28),
            ]
        )
        overall_score = round(
            valuation_score * 0.18
            + profitability_score * 0.24
            + growth_score * 0.22
            + cashflow_score * 0.18
            + solvency_score * 0.18,
            1,
        )
        return {
            "overall_score": overall_score,
            "rating": self._rating_label(overall_score),
            "valuation_score": valuation_score,
            "profitability_score": profitability_score,
            "growth_score": growth_score,
            "cashflow_score": cashflow_score,
            "solvency_score": solvency_score,
            "summary": self._build_summary(
                record,
                overall_score,
                valuation_score,
                profitability_score,
                growth_score,
                cashflow_score,
                solvency_score,
            ),
            "strengths": self._collect_strengths(
                record,
                valuation_score,
                profitability_score,
                growth_score,
                cashflow_score,
                solvency_score,
            ),
            "concerns": self._collect_concerns(
                record,
                valuation_score,
                profitability_score,
                growth_score,
                cashflow_score,
                solvency_score,
            ),
        }

    def _matches(self, record: dict[str, Any], filters: dict[str, Any]) -> bool:
        text_query = str(filters.get("query", "")).strip().lower()
        f10_query = str(filters.get("f10_query", "")).strip().lower()
        concept_query = str(filters.get("concept", "")).strip().lower()
        if filters.get("market_region") and record["market_region"] != filters["market_region"]:
            return False
        if filters.get("exchange_code") and record["exchange_code"] != filters["exchange_code"]:
            return False
        if filters.get("board") and record["board"] != filters["board"]:
            return False
        if filters.get("sector") and record["sector"] != filters["sector"]:
            return False
        if filters.get("industry") and record["industry"] != filters["industry"]:
            return False
        searchable = " ".join(
            [
                record["symbol"],
                record["company_name"],
                record["sector"],
                record["industry"],
                " ".join(record["concepts"]),
            ]
        ).lower()
        f10_blob = " ".join(
            [
                " ".join(record["concepts"]),
                record["f10_summary"],
                record["main_business"],
                record["products_services"],
                record["competitive_advantages"],
                record["risks"],
            ]
        ).lower()
        if text_query and text_query not in searchable and text_query not in f10_blob:
            return False
        if f10_query and f10_query not in f10_blob:
            return False
        if concept_query and concept_query not in " ".join(record["concepts"]).lower():
            return False
        numeric_guards = [
            ("min_change_pct", "change_pct", lambda actual, limit: actual >= limit),
            ("max_change_pct", "change_pct", lambda actual, limit: actual <= limit),
            ("min_pe_ttm", "pe_ttm", lambda actual, limit: actual >= limit),
            ("max_pe_ttm", "pe_ttm", lambda actual, limit: actual <= limit),
            ("min_pb", "pb", lambda actual, limit: actual >= limit),
            ("max_pb", "pb", lambda actual, limit: actual <= limit),
            ("min_roe", "roe", lambda actual, limit: actual >= limit),
            ("min_revenue_growth", "revenue_growth", lambda actual, limit: actual >= limit),
            ("min_net_profit_growth", "net_profit_growth", lambda actual, limit: actual >= limit),
            ("min_gross_margin", "gross_margin", lambda actual, limit: actual >= limit),
            ("min_net_margin", "net_margin", lambda actual, limit: actual >= limit),
            ("min_operating_cashflow_growth", "operating_cashflow_growth", lambda actual, limit: actual >= limit),
            ("min_free_cashflow_margin", "free_cashflow_margin", lambda actual, limit: actual >= limit),
            ("min_current_ratio", "current_ratio", lambda actual, limit: actual >= limit),
            ("max_debt_to_asset", "debt_to_asset", lambda actual, limit: actual <= limit),
            ("min_dividend_yield", "dividend_yield", lambda actual, limit: actual >= limit),
            ("min_market_cap", "market_cap", lambda actual, limit: actual >= limit),
            ("max_market_cap", "market_cap", lambda actual, limit: actual <= limit),
        ]
        for filter_key, field_name, predicate in numeric_guards:
            if filters.get(filter_key) in ("", None):
                continue
            actual = record.get(field_name)
            if actual is None or not predicate(actual, float(filters[filter_key])):
                return False
        return True

    def _filtered_records(self, filters: dict[str, Any]) -> list[dict[str, Any]]:
        """Return matched stock records without pagination."""

        records = [self.get_stock_core(instrument_id) for instrument_id in self.profiles]
        return [record for record in records if self._matches(record, filters)]

    def _sort_records(self, records: list[dict[str, Any]], *, sort_key: str, reverse: bool) -> list[dict[str, Any]]:
        """Sort stock records while always pushing missing values to the end."""

        with_value = [record for record in records if record.get(sort_key) is not None]
        without_value = [record for record in records if record.get(sort_key) is None]
        with_value.sort(key=lambda record: self._sortable_value(record.get(sort_key)), reverse=reverse)
        return with_value + without_value

    def _sortable_value(self, value: Any) -> Any:
        """Normalize mixed stock fields into values that can be sorted safely."""

        if isinstance(value, (int, float)):
            return value
        return str(value or "").lower()

    def _safe_float(self, value: Any, *, default: float | None = None) -> float | None:
        """Convert raw text into float while tolerating blanks and malformed fields."""

        if value in ("", None):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _parse_bool(self, value: Any, *, default: bool) -> bool:
        """Parse a truthy/falsey query value."""

        if value in ("", None):
            return default
        if isinstance(value, bool):
            return value
        return str(value).strip().lower() in {"1", "true", "yes", "y"}

    def save_financial_snapshot(self, instrument_id: str, snapshot: dict[str, Any], *, commit: bool = True) -> None:
        """Persist one historical financial snapshot for a stock."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        if self.persistence is None:
            return
        snapshot_key = f"{instrument_id}:{snapshot['period_type']}:{snapshot['report_date']}"
        self.persistence.upsert_record(
            "ref_stock_financial_history",
            "snapshot_key",
            snapshot_key,
            snapshot,
            extra_columns={
                "instrument_id": instrument_id,
                "report_date": snapshot["report_date"],
                "period_type": snapshot["period_type"],
                "fiscal_year": snapshot["fiscal_year"],
                "revenue": snapshot.get("revenue"),
                "net_income": snapshot.get("net_income"),
                "operating_cashflow": snapshot.get("operating_cashflow"),
                "free_cashflow": snapshot.get("free_cashflow"),
            },
            commit=commit,
        )

    def save_minute_bar(self, instrument_id: str, bar: dict[str, Any], *, commit: bool = True) -> None:
        """Persist one one-minute bar for a stock."""

        instrument_id = self._canonical_instrument_id(instrument_id)
        if self.persistence is None:
            return
        profile = self.profiles[instrument_id]
        bar_key = f"{instrument_id}:{bar['bar_time']}"
        self.persistence.upsert_record(
            "mkt_stock_minute_bars",
            "bar_key",
            bar_key,
            bar,
            extra_columns={
                "instrument_id": instrument_id,
                "bar_time": bar["bar_time"],
                "market_region": profile.market_region,
                "exchange_code": profile.exchange_code,
                "close_price": bar["close"],
                "volume": bar["volume"],
            },
            commit=commit,
        )

    def _ensure_financial_history(self, instrument_id: str, periods: int = 5) -> None:
        """Seed persisted financial history when it is missing."""

        if self.persistence is None:
            return
        existing_rows = self.persistence.fetch_all(
            "ref_stock_financial_history",
            where="instrument_id = :instrument_id",
            params={"instrument_id": instrument_id},
            limit=periods,
        )
        if len(existing_rows) >= periods:
            return
        for snapshot in self._generate_demo_financial_history(instrument_id, periods=periods):
            self.save_financial_snapshot(instrument_id, snapshot, commit=False)
        self.persistence.commit()

    def _ensure_minute_history(self, instrument_id: str, bars: int = 240) -> None:
        """Seed persisted one-minute bars when they are missing."""

        if self.persistence is None:
            return
        existing_rows = self.persistence.fetch_all(
            "mkt_stock_minute_bars",
            where="instrument_id = :instrument_id",
            params={"instrument_id": instrument_id},
            limit=bars,
        )
        if len(existing_rows) >= bars:
            return
        for bar in self._generate_demo_minute_bars(instrument_id, bars=bars):
            self.save_minute_bar(instrument_id, bar, commit=False)
        self.persistence.commit()

    def _live_quote_fields(self, instrument_id: str) -> dict[str, Any]:
        """Compute change_pct, volume, turnover, open, high, low from latest bars.

        Uses _history_payload_readonly to avoid populating the cache with generated
        demo data, which would shadow real local files in later get_stock_history calls.
        """

        empty = {"change_pct": None, "volume": None, "turnover": None, "open": None, "high": None, "low": None, "amplitude": None}
        try:
            result = self._history_payload_readonly(instrument_id)
        except Exception:
            return empty
        if result is None:
            return empty
        _, bars = result
        if not bars:
            return empty
        latest = bars[-1]
        previous = bars[-2] if len(bars) >= 2 else None
        latest_close = latest.get("close")
        prev_close = previous.get("close") if previous else None
        change_pct = None
        amplitude = None
        if latest_close is not None and prev_close not in (None, 0):
            change_pct = round((latest_close - prev_close) / prev_close * 100, 2)
        if prev_close not in (None, 0):
            high = latest.get("high", latest_close)
            low = latest.get("low", latest_close)
            if high is not None and low is not None:
                amplitude = round((high - low) / prev_close * 100, 2)
        vol = latest.get("volume")
        turnover = round(latest_close * vol, 2) if latest_close and vol else None
        return {
            "change_pct": change_pct,
            "volume": vol,
            "turnover": turnover,
            "open": latest.get("open"),
            "high": latest.get("high"),
            "low": latest.get("low"),
            "amplitude": amplitude,
        }

    def _history_payload_readonly(self, instrument_id: str) -> tuple[str, list[dict[str, Any]]] | None:
        """Return cached or local history WITHOUT generating demo data.

        Returns None when there is no cached or local data available, so
        the caller can decide whether to trigger the full (generating) path.
        """

        cached = self._history_cache.get(instrument_id)
        if cached is not None:
            return cached
        local = self._load_local_daily_history(instrument_id)
        if local:
            self._history_cache[instrument_id] = ("local_a_share_raw", local)
            return self._history_cache[instrument_id]
        return None

    def _history_payload(self, instrument_id: str) -> tuple[str, list[dict[str, Any]]]:
        """Load cached history, then local files, otherwise fall back to demo history generation."""

        cached = self._history_cache.get(instrument_id)
        if cached is not None:
            return cached
        local = self._load_local_daily_history(instrument_id)
        if local:
            self._history_cache[instrument_id] = ("local_a_share_raw", local)
            return self._history_cache[instrument_id]
        generated = self._generate_demo_history(instrument_id)
        self._history_cache[instrument_id] = ("generated_demo", generated)
        return self._history_cache[instrument_id]

    def _load_local_daily_history(self, instrument_id: str) -> list[dict[str, Any]]:
        """Load locally downloaded A-share raw daily bars if they exist on disk."""

        instrument = self.instruments[instrument_id]
        if instrument.market != MarketType.STOCK or instrument.market_region != "CN":
            return []
        path = self._local_history_file(instrument_id)
        if not path.exists():
            return []
        bars: list[dict[str, Any]] = []
        with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                close_price = self._safe_float(row.get("close"))
                if close_price is None:
                    continue
                bars.append(
                    {
                        "trade_date": row["date"],
                        "open": self._safe_float(row.get("open"), default=close_price) or close_price,
                        "high": self._safe_float(row.get("high"), default=close_price) or close_price,
                        "low": self._safe_float(row.get("low"), default=close_price) or close_price,
                        "close": close_price,
                        "volume": self._safe_float(row.get("volume"), default=0.0) or 0.0,
                    }
                )
        return bars

    def _local_history_file(self, instrument_id: str) -> Path:
        """Resolve the expected raw-history file path for one A-share stock."""

        profile = self.profiles[instrument_id]
        symbol = profile.symbol.split(".", 1)[0]
        exchange_dir = profile.exchange_code.lower()
        return self.history_root / "cn_equities" / "a_share" / "daily_raw" / exchange_dir / f"{symbol}.csv.gz"

    def _generate_demo_history(self, instrument_id: str, bars: int = 180) -> list[dict[str, Any]]:
        """Generate deterministic fallback daily bars for stocks without local history files."""

        profile = self.profiles[instrument_id]
        seed = int(hashlib.sha256(instrument_id.encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed)
        end_date = date.today()
        trading_days: list[date] = []
        current = end_date
        while len(trading_days) < bars:
            if current.weekday() < 5:
                trading_days.append(current)
            current -= timedelta(days=1)
        trading_days.reverse()

        base_close = max(profile.last_price or 30.0, 1.0)
        current_price = base_close * (0.82 + rng.random() * 0.18)
        volatility = 0.012 + rng.random() * 0.015
        trend_bias = (rng.random() - 0.48) * 0.003
        generated: list[dict[str, Any]] = []
        for trading_day in trading_days:
            overnight_gap = (rng.random() - 0.5) * volatility * 0.6
            open_price = max(current_price * (1 + overnight_gap), 0.5)
            intraday_move = trend_bias + (rng.random() - 0.5) * volatility
            close_price = max(open_price * (1 + intraday_move), 0.5)
            wick_scale = volatility * (0.55 + rng.random() * 0.7)
            high_price = max(open_price, close_price) * (1 + wick_scale)
            low_price = min(open_price, close_price) * max(1 - wick_scale, 0.2)
            generated.append(
                {
                    "trade_date": trading_day.isoformat(),
                    "open": round(open_price, 2),
                    "high": round(high_price, 2),
                    "low": round(low_price, 2),
                    "close": round(close_price, 2),
                    "volume": round((profile.market_cap or 5_000.0) * (9_000 + rng.random() * 7_000), 0),
                }
            )
            current_price = close_price

        scale = base_close / generated[-1]["close"] if generated and generated[-1]["close"] else 1.0
        for bar in generated:
            bar["open"] = round(bar["open"] * scale, 2)
            bar["high"] = round(bar["high"] * scale, 2)
            bar["low"] = round(bar["low"] * scale, 2)
            bar["close"] = round(bar["close"] * scale, 2)
        return generated

    def _generate_demo_financial_history(self, instrument_id: str, periods: int = 5) -> list[dict[str, Any]]:
        """Generate deterministic historical financial snapshots for one stock."""

        profile = self.profiles[instrument_id]
        seed = int(hashlib.sha256(f"financial:{instrument_id}".encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed)
        current_year = date.today().year
        base_revenue = max((profile.market_cap or 5_000.0) * (0.15 + rng.random() * 0.18), 120.0)
        base_margin = (profile.net_margin or 12.0) / 100.0
        base_cashflow_margin = max((profile.free_cashflow_margin or 8.0) / 100.0, 0.01)
        snapshots: list[dict[str, Any]] = []
        for offset, fiscal_year in enumerate(range(current_year - periods + 1, current_year + 1)):
            progress = (offset + 1) / max(periods, 1)
            revenue = base_revenue * (0.78 + progress * 0.34 + (rng.random() - 0.5) * 0.08)
            net_margin = max(0.02, base_margin * (0.82 + progress * 0.24 + (rng.random() - 0.5) * 0.05))
            net_income = revenue * net_margin
            operating_cashflow = net_income * (1.08 + rng.random() * 0.24)
            free_cashflow = operating_cashflow * (0.72 + rng.random() * 0.18)
            total_assets = max((profile.market_cap or 5_000.0) * (0.38 + rng.random() * 0.24), revenue * 0.9)
            total_liabilities = total_assets * max((profile.debt_to_asset or 40.0) / 100.0, 0.05)
            snapshot = {
                "instrument_id": instrument_id,
                "report_date": date(fiscal_year, 12, 31).isoformat(),
                "period_type": "FY",
                "fiscal_year": fiscal_year,
                "currency": profile.currency,
                "revenue": round(revenue, 2),
                "net_income": round(net_income, 2),
                "operating_cashflow": round(operating_cashflow, 2),
                "free_cashflow": round(free_cashflow, 2),
                "total_assets": round(total_assets, 2),
                "total_liabilities": round(total_liabilities, 2),
                "gross_margin": round(max((profile.gross_margin or 30.0) * (0.84 + progress * 0.18 + (rng.random() - 0.5) * 0.04), 1.0), 2),
                "net_margin": round(net_margin * 100, 2),
                "roe": round(max((profile.roe or 10.0) * (0.78 + progress * 0.3 + (rng.random() - 0.5) * 0.06), 1.0), 2),
                "revenue_growth": round((profile.revenue_growth or 6.0) * (0.8 + progress * 0.22 + (rng.random() - 0.5) * 0.08), 2),
                "net_profit_growth": round((profile.net_profit_growth or 7.0) * (0.78 + progress * 0.26 + (rng.random() - 0.5) * 0.1), 2),
                "operating_cashflow_growth": round((profile.operating_cashflow_growth or 6.0) * (0.82 + progress * 0.22 + (rng.random() - 0.5) * 0.08), 2),
                "free_cashflow_margin": round(max(base_cashflow_margin * 100 * (0.82 + progress * 0.2 + (rng.random() - 0.5) * 0.06), 0.5), 2),
                "current_ratio": round(max((profile.current_ratio or 1.0) * (0.88 + progress * 0.14 + (rng.random() - 0.5) * 0.04), 0.2), 2),
                "quick_ratio": round(max((profile.quick_ratio or 0.8) * (0.88 + progress * 0.14 + (rng.random() - 0.5) * 0.04), 0.2), 2),
                "interest_coverage": round(max((profile.interest_coverage or 4.0) * (0.82 + progress * 0.2 + (rng.random() - 0.5) * 0.05), 0.5), 2),
                "debt_to_asset": round(min(max((profile.debt_to_asset or 40.0) * (0.96 + (rng.random() - 0.5) * 0.08), 1.0), 95.0), 2),
                "dividend_yield": round(max((profile.dividend_yield or 0.5) * (0.82 + progress * 0.18 + (rng.random() - 0.5) * 0.08), 0.0), 2),
                "pe_ttm": round(max((profile.pe_ttm or 12.0) * (0.85 + (rng.random() - 0.5) * 0.12), 1.0), 2),
                "pb": round(max((profile.pb or 1.5) * (0.84 + (rng.random() - 0.5) * 0.14), 0.2), 2),
                "market_cap": round(max((profile.market_cap or 5_000.0) * (0.74 + progress * 0.32 + (rng.random() - 0.5) * 0.12), 100.0), 2),
            }
            snapshots.append(snapshot)
        return list(reversed(snapshots))

    def _generate_demo_minute_bars(self, instrument_id: str, bars: int = 240) -> list[dict[str, Any]]:
        """Generate deterministic recent one-minute bars for one stock."""

        profile = self.profiles[instrument_id]
        instrument = self.instruments[instrument_id]
        seed = int(hashlib.sha256(f"minute:{instrument_id}".encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed)
        timestamps = self._recent_market_minutes(instrument, count=bars)
        current_price = max(profile.last_price or 20.0, 0.5) * (0.96 + rng.random() * 0.08)
        volatility = 0.0009 + rng.random() * 0.0028
        generated: list[dict[str, Any]] = []
        for timestamp in timestamps:
            drift = (rng.random() - 0.495) * volatility * 1.6
            open_price = max(current_price, 0.2)
            close_price = max(open_price * (1.0 + drift), 0.2)
            wick = open_price * volatility * (0.4 + rng.random() * 0.9)
            high_price = max(open_price, close_price) + wick
            low_price = max(min(open_price, close_price) - wick, 0.1)
            # Estimate shares outstanding from market_cap (yuan) and last price (yuan/share)
            shares_outstanding = max(profile.market_cap or 5_000.0, 1.0) / max(profile.last_price or 20.0, 0.1)
            # Per-minute volume: 0.005% to 0.05% of shares outstanding (realistic intraday)
            volume = max(shares_outstanding * (0.00005 + rng.random() * 0.00045), 100.0)
            turnover = volume * close_price
            generated.append(
                {
                    "instrument_id": instrument_id,
                    "bar_time": timestamp.astimezone(timezone.utc).isoformat(),
                    "open": round(open_price, 2),
                    "high": round(high_price, 2),
                    "low": round(low_price, 2),
                    "close": round(close_price, 2),
                    "volume": round(volume, 0),
                    "turnover": round(turnover, 2),
                    "market_region": profile.market_region,
                    "exchange_code": profile.exchange_code,
                }
            )
            current_price = close_price
        scale = max(profile.last_price or 20.0, 0.5) / max(generated[-1]["close"], 0.5) if generated else 1.0
        for bar in generated:
            bar["open"] = round(bar["open"] * scale, 2)
            bar["high"] = round(bar["high"] * scale, 2)
            bar["low"] = round(bar["low"] * scale, 2)
            bar["close"] = round(bar["close"] * scale, 2)
            bar["turnover"] = round(bar["volume"] * bar["close"], 2)
        return generated

    def _recent_market_minutes(self, instrument: Instrument, *, count: int) -> list[datetime]:
        """Return the most recent valid trading minutes for one stock instrument."""

        timezone_name = self.MARKET_TIMEZONES.get(instrument.market_region, "UTC")
        local_zone = ZoneInfo(timezone_name)
        sessions = [
            (
                datetime.strptime(start_at, "%H:%M").time(),
                datetime.strptime(end_at, "%H:%M").time(),
            )
            for start_at, end_at in instrument.trading_sessions
        ]
        if not sessions:
            sessions = [(datetime.strptime("09:30", "%H:%M").time(), datetime.strptime("16:00", "%H:%M").time())]
        cursor = datetime.now(timezone.utc).astimezone(local_zone).replace(second=0, microsecond=0)
        result: list[datetime] = []
        while len(result) < count:
            if cursor.weekday() < 5 and any(start_at <= cursor.time() < end_at for start_at, end_at in sessions):
                result.append(cursor)
            cursor -= timedelta(minutes=1)
        result.reverse()
        return result

    def _serialize_profile(self, profile: StockProfile) -> dict[str, Any]:
        """Convert a stock profile into a JSON-safe dictionary."""

        payload = asdict(profile)
        payload["concepts"] = list(profile.concepts)
        payload["listing_date"] = profile.listing_date.isoformat() if profile.listing_date else None
        return payload

    def _canonicalize_models(self, instrument: Instrument, profile: StockProfile) -> tuple[Instrument, StockProfile]:
        """Normalize model identifiers before they are stored."""

        normalized_id = self._normalize_instrument_id(profile.instrument_id or instrument.instrument_id)
        normalized_symbol = self._normalize_symbol(profile.symbol or instrument.symbol, profile.market_region)
        if normalized_id == instrument.instrument_id and normalized_symbol == profile.symbol:
            return instrument, self._enrich_profile_defaults(profile)
        normalized_instrument = Instrument(
            instrument_id=normalized_id,
            symbol=normalized_symbol,
            market=instrument.market,
            instrument_type=instrument.instrument_type,
            market_region=instrument.market_region,
            tick_size=instrument.tick_size,
            lot_size=instrument.lot_size,
            contract_multiplier=instrument.contract_multiplier,
            quote_currency=instrument.quote_currency,
            base_currency=instrument.base_currency,
            settlement_cycle=instrument.settlement_cycle,
            short_sellable=instrument.short_sellable,
            expiry_at=instrument.expiry_at,
            trading_sessions=instrument.trading_sessions,
            trading_rules=instrument.trading_rules,
        )
        normalized_profile = StockProfile(
            instrument_id=normalized_id,
            symbol=normalized_symbol,
            company_name=profile.company_name,
            market_region=profile.market_region,
            exchange_code=profile.exchange_code,
            board=profile.board,
            sector=profile.sector,
            industry=profile.industry,
            concepts=profile.concepts,
            f10_summary=profile.f10_summary,
            main_business=profile.main_business,
            products_services=profile.products_services,
            competitive_advantages=profile.competitive_advantages,
            risks=profile.risks,
            pe_ttm=profile.pe_ttm,
            pb=profile.pb,
            roe=profile.roe,
            revenue_growth=profile.revenue_growth,
            net_profit_growth=profile.net_profit_growth,
            gross_margin=profile.gross_margin,
            net_margin=profile.net_margin,
            debt_to_asset=profile.debt_to_asset,
            dividend_yield=profile.dividend_yield,
            operating_cashflow_growth=profile.operating_cashflow_growth,
            free_cashflow_margin=profile.free_cashflow_margin,
            current_ratio=profile.current_ratio,
            quick_ratio=profile.quick_ratio,
            interest_coverage=profile.interest_coverage,
            asset_turnover=profile.asset_turnover,
            market_cap=profile.market_cap,
            float_market_cap=profile.float_market_cap,
            last_price=profile.last_price,
            listing_date=profile.listing_date,
            currency=profile.currency,
        )
        return normalized_instrument, self._enrich_profile_defaults(normalized_profile)

    def _enrich_profile_defaults(self, profile: StockProfile) -> StockProfile:
        """Backfill deterministic baseline financial fields for imported stocks."""

        seed = int(hashlib.sha256(f"profile:{profile.instrument_id}".encode("utf-8")).hexdigest()[:16], 16)
        rng = random.Random(seed)

        def uniform(low: float, high: float, digits: int = 2) -> float:
            return round(rng.uniform(low, high), digits)

        sector_key = f"{profile.sector} {profile.industry}".lower()
        if "financial" in sector_key or "bank" in sector_key or "insurance" in sector_key or "brokerage" in sector_key:
            config = {
                "price": (4.0, 28.0) if profile.market_region == "CN" else (8.0, 90.0),
                "market_cap": (40_000_000_000.0, 900_000_000_000.0),
                "pe_ttm": (4.5, 12.5),
                "pb": (0.45, 1.9),
                "roe": (8.0, 18.0),
                "revenue_growth": (1.0, 14.0),
                "net_profit_growth": (0.5, 16.0),
                "gross_margin": (42.0, 78.0),
                "net_margin": (16.0, 34.0),
                "debt_to_asset": (58.0, 88.0),
                "dividend_yield": (1.2, 6.5),
                "operating_cashflow_growth": (2.0, 15.0),
                "free_cashflow_margin": (8.0, 28.0),
                "current_ratio": (0.9, 1.5),
                "quick_ratio": (0.8, 1.4),
                "interest_coverage": (2.0, 8.0),
                "asset_turnover": (0.12, 0.65),
            }
        elif "technology" in sector_key or "software" in sector_key or "semiconductor" in sector_key:
            config = {
                "price": (12.0, 420.0),
                "market_cap": (8_000_000_000.0, 2_400_000_000_000.0),
                "pe_ttm": (18.0, 58.0),
                "pb": (2.0, 10.0),
                "roe": (10.0, 32.0),
                "revenue_growth": (8.0, 42.0),
                "net_profit_growth": (6.0, 55.0),
                "gross_margin": (28.0, 74.0),
                "net_margin": (8.0, 30.0),
                "debt_to_asset": (8.0, 48.0),
                "dividend_yield": (0.0, 1.8),
                "operating_cashflow_growth": (6.0, 38.0),
                "free_cashflow_margin": (10.0, 34.0),
                "current_ratio": (1.1, 2.8),
                "quick_ratio": (1.0, 2.5),
                "interest_coverage": (6.0, 28.0),
                "asset_turnover": (0.25, 1.15),
            }
        elif "consumer staples" in sector_key or "beverage" in sector_key or "food" in sector_key:
            config = {
                "price": (10.0, 260.0),
                "market_cap": (10_000_000_000.0, 1_500_000_000_000.0),
                "pe_ttm": (12.0, 35.0),
                "pb": (1.5, 9.0),
                "roe": (12.0, 36.0),
                "revenue_growth": (4.0, 24.0),
                "net_profit_growth": (4.0, 28.0),
                "gross_margin": (18.0, 68.0),
                "net_margin": (6.0, 28.0),
                "debt_to_asset": (12.0, 55.0),
                "dividend_yield": (0.8, 5.2),
                "operating_cashflow_growth": (4.0, 22.0),
                "free_cashflow_margin": (6.0, 26.0),
                "current_ratio": (1.0, 2.4),
                "quick_ratio": (0.8, 2.0),
                "interest_coverage": (4.0, 18.0),
                "asset_turnover": (0.35, 1.35),
            }
        else:
            config = {
                "price": (5.0, 180.0),
                "market_cap": (5_000_000_000.0, 800_000_000_000.0),
                "pe_ttm": (7.0, 28.0),
                "pb": (0.8, 4.5),
                "roe": (7.0, 22.0),
                "revenue_growth": (-3.0, 22.0),
                "net_profit_growth": (-6.0, 28.0),
                "gross_margin": (12.0, 52.0),
                "net_margin": (4.0, 22.0),
                "debt_to_asset": (18.0, 68.0),
                "dividend_yield": (0.0, 4.8),
                "operating_cashflow_growth": (-2.0, 24.0),
                "free_cashflow_margin": (2.0, 22.0),
                "current_ratio": (0.9, 2.2),
                "quick_ratio": (0.7, 1.9),
                "interest_coverage": (2.0, 16.0),
                "asset_turnover": (0.2, 1.45),
            }

        market_cap = profile.market_cap if profile.market_cap is not None else uniform(*config["market_cap"], digits=0)
        float_market_cap = (
            profile.float_market_cap
            if profile.float_market_cap is not None
            else round(market_cap * rng.uniform(0.42, 0.88), 0)
        )
        generated = {
            "pe_ttm": uniform(*config["pe_ttm"]),
            "pb": uniform(*config["pb"]),
            "roe": uniform(*config["roe"]),
            "revenue_growth": uniform(*config["revenue_growth"]),
            "net_profit_growth": uniform(*config["net_profit_growth"]),
            "gross_margin": uniform(*config["gross_margin"]),
            "net_margin": uniform(*config["net_margin"]),
            "debt_to_asset": uniform(*config["debt_to_asset"]),
            "dividend_yield": uniform(*config["dividend_yield"]),
            "operating_cashflow_growth": uniform(*config["operating_cashflow_growth"]),
            "free_cashflow_margin": uniform(*config["free_cashflow_margin"]),
            "current_ratio": uniform(*config["current_ratio"]),
            "quick_ratio": uniform(*config["quick_ratio"]),
            "interest_coverage": uniform(*config["interest_coverage"]),
            "asset_turnover": uniform(*config["asset_turnover"]),
            "market_cap": market_cap,
            "float_market_cap": float_market_cap,
            "last_price": profile.last_price if profile.last_price is not None else uniform(*config["price"]),
        }
        return replace(
            profile,
            pe_ttm=profile.pe_ttm if profile.pe_ttm is not None else generated["pe_ttm"],
            pb=profile.pb if profile.pb is not None else generated["pb"],
            roe=profile.roe if profile.roe is not None else generated["roe"],
            revenue_growth=profile.revenue_growth if profile.revenue_growth is not None else generated["revenue_growth"],
            net_profit_growth=profile.net_profit_growth if profile.net_profit_growth is not None else generated["net_profit_growth"],
            gross_margin=profile.gross_margin if profile.gross_margin is not None else generated["gross_margin"],
            net_margin=profile.net_margin if profile.net_margin is not None else generated["net_margin"],
            debt_to_asset=profile.debt_to_asset if profile.debt_to_asset is not None else generated["debt_to_asset"],
            dividend_yield=profile.dividend_yield if profile.dividend_yield is not None else generated["dividend_yield"],
            operating_cashflow_growth=(
                profile.operating_cashflow_growth
                if profile.operating_cashflow_growth is not None
                else generated["operating_cashflow_growth"]
            ),
            free_cashflow_margin=(
                profile.free_cashflow_margin
                if profile.free_cashflow_margin is not None
                else generated["free_cashflow_margin"]
            ),
            current_ratio=profile.current_ratio if profile.current_ratio is not None else generated["current_ratio"],
            quick_ratio=profile.quick_ratio if profile.quick_ratio is not None else generated["quick_ratio"],
            interest_coverage=profile.interest_coverage if profile.interest_coverage is not None else generated["interest_coverage"],
            asset_turnover=profile.asset_turnover if profile.asset_turnover is not None else generated["asset_turnover"],
            market_cap=market_cap,
            float_market_cap=float_market_cap,
            last_price=generated["last_price"],
        )

    def _canonical_instrument_id(self, instrument_id: str) -> str:
        """Normalize user-supplied instrument identifiers before lookup."""

        if instrument_id in self.profiles:
            return instrument_id
        normalized = self._normalize_instrument_id(instrument_id)
        return normalized if normalized in self.profiles else instrument_id

    def _normalize_instrument_id(self, instrument_id: str) -> str:
        """Normalize identifiers such as Hong Kong stock codes into canonical form."""

        if "." not in instrument_id:
            return instrument_id
        symbol, suffix = instrument_id.rsplit(".", 1)
        if suffix.upper() == "HK" and symbol.isdigit():
            return f"{symbol.zfill(5)}.HK"
        return f"{symbol}.{suffix.upper()}"

    def _normalize_symbol(self, symbol: str, market_region: str) -> str:
        """Normalize stored stock symbols without altering non-HK identifiers."""

        if market_region == "HK":
            return self._normalize_instrument_id(symbol)
        return symbol

    def _instrument_from_payload(self, payload: dict[str, Any]) -> Instrument:
        """Reconstruct one instrument from persisted JSON payload."""

        trading_sessions = tuple(tuple(session) for session in payload.get("trading_sessions", ()))
        return Instrument(
            instrument_id=payload["instrument_id"],
            symbol=payload["symbol"],
            market=MarketType(payload["market"]),
            instrument_type=payload.get("instrument_type", "equity"),
            market_region=payload.get("market_region", "GLOBAL"),
            tick_size=float(payload.get("tick_size", 0.01)),
            lot_size=float(payload.get("lot_size", 1.0)),
            contract_multiplier=float(payload.get("contract_multiplier", 1.0)),
            quote_currency=payload.get("quote_currency", "USD"),
            base_currency=payload.get("base_currency", ""),
            settlement_cycle=payload.get("settlement_cycle"),
            short_sellable=bool(payload.get("short_sellable", False)),
            trading_sessions=trading_sessions,
            trading_rules=dict(payload.get("trading_rules", {})),
        )

    def _profile_from_payload(self, payload: dict[str, Any]) -> StockProfile:
        """Reconstruct one stock profile from persisted JSON payload."""

        listing_date = payload.get("listing_date")
        return StockProfile(
            instrument_id=payload["instrument_id"],
            symbol=payload["symbol"],
            company_name=payload["company_name"],
            market_region=payload["market_region"],
            exchange_code=payload["exchange_code"],
            board=payload["board"],
            sector=payload["sector"],
            industry=payload["industry"],
            concepts=tuple(payload.get("concepts", ())),
            f10_summary=payload.get("f10_summary", ""),
            main_business=payload.get("main_business", ""),
            products_services=payload.get("products_services", ""),
            competitive_advantages=payload.get("competitive_advantages", ""),
            risks=payload.get("risks", ""),
            pe_ttm=payload.get("pe_ttm"),
            pb=payload.get("pb"),
            roe=payload.get("roe"),
            revenue_growth=payload.get("revenue_growth"),
            net_profit_growth=payload.get("net_profit_growth"),
            gross_margin=payload.get("gross_margin"),
            net_margin=payload.get("net_margin"),
            debt_to_asset=payload.get("debt_to_asset"),
            dividend_yield=payload.get("dividend_yield"),
            operating_cashflow_growth=payload.get("operating_cashflow_growth"),
            free_cashflow_margin=payload.get("free_cashflow_margin"),
            current_ratio=payload.get("current_ratio"),
            quick_ratio=payload.get("quick_ratio"),
            interest_coverage=payload.get("interest_coverage"),
            asset_turnover=payload.get("asset_turnover"),
            market_cap=payload.get("market_cap"),
            float_market_cap=payload.get("float_market_cap"),
            last_price=payload.get("last_price"),
            listing_date=date.fromisoformat(listing_date) if listing_date else None,
            currency=payload.get("currency", "USD"),
        )

    def _average_scores(self, values: list[float | None]) -> float:
        """Average score values while skipping missing inputs."""

        valid = [value for value in values if value is not None]
        if not valid:
            return 0.0
        return round(sum(valid) / len(valid), 1)

    def _score_higher_better(self, value: float | None, thresholds: list[tuple[float, float]], default: float = 0.0) -> float | None:
        """Score a metric where larger values indicate better quality."""

        if value is None:
            return None
        for threshold, score in thresholds:
            if value >= threshold:
                return score
        return default

    def _score_lower_better(self, value: float | None, thresholds: list[tuple[float, float]], default: float = 0.0) -> float | None:
        """Score a metric where smaller values indicate better quality."""

        if value is None:
            return None
        for threshold, score in thresholds:
            if value <= threshold:
                return score
        return default

    def _rating_label(self, score: float) -> str:
        """Translate a numeric financial score into a human-readable label."""

        if score >= 85:
            return "Excellent"
        if score >= 72:
            return "Strong"
        if score >= 60:
            return "Balanced"
        if score >= 45:
            return "Mixed"
        return "Fragile"

    def _build_summary(
        self,
        record: dict[str, Any],
        overall_score: float,
        valuation_score: float,
        profitability_score: float,
        growth_score: float,
        cashflow_score: float,
        solvency_score: float,
    ) -> str:
        """Create a compact narrative summary for the financial analysis."""

        valuation_view = "估值偏友好" if valuation_score >= 72 else "估值中性" if valuation_score >= 55 else "估值偏贵"
        profit_view = "盈利能力强" if profitability_score >= 75 else "盈利能力稳健" if profitability_score >= 58 else "盈利质量一般"
        growth_view = "成长动能强" if growth_score >= 75 else "成长稳定" if growth_score >= 58 else "成长动能偏弱"
        cashflow_view = "现金流表现扎实" if cashflow_score >= 72 else "现金流可接受" if cashflow_score >= 55 else "现金流需要关注"
        solvency_view = "偿债压力较低" if solvency_score >= 72 else "资产负债结构中性" if solvency_score >= 55 else "偿债压力偏高"
        return (
            f"{record['company_name']} 当前财务综合评分为 {overall_score} 分，评级 {self._rating_label(overall_score)}。"
            f" 从财务结构看，{profit_view}、{growth_view}，{cashflow_view}；"
            f" 同时 {valuation_view}，{solvency_view}。"
        )

    def _collect_strengths(
        self,
        record: dict[str, Any],
        valuation_score: float,
        profitability_score: float,
        growth_score: float,
        cashflow_score: float,
        solvency_score: float,
    ) -> list[str]:
        """Collect concise positive findings for the stock."""

        strengths: list[str] = []
        if record.get("roe") is not None and record["roe"] >= 20:
            strengths.append("ROE 较高，资本回报能力强")
        if record.get("gross_margin") is not None and record["gross_margin"] >= 40:
            strengths.append("毛利率较高，业务壁垒或产品结构较好")
        if record.get("net_profit_growth") is not None and record["net_profit_growth"] >= 20:
            strengths.append("净利润增速较快，利润弹性明显")
        if record.get("operating_cashflow_growth") is not None and record["operating_cashflow_growth"] >= 10:
            strengths.append("经营现金流保持扩张，利润兑现能力较好")
        if valuation_score >= 72:
            strengths.append("估值相对财务质量较有吸引力")
        if solvency_score >= 72:
            strengths.append("资产负债结构稳健，偿债压力可控")
        return strengths[:4]

    def _collect_concerns(
        self,
        record: dict[str, Any],
        valuation_score: float,
        profitability_score: float,
        growth_score: float,
        cashflow_score: float,
        solvency_score: float,
    ) -> list[str]:
        """Collect concise risk flags for the stock."""

        concerns: list[str] = []
        if valuation_score < 50:
            concerns.append("当前估值对未来增长有较高预期")
        if growth_score < 55:
            concerns.append("收入或利润增长动能偏弱")
        if cashflow_score < 55:
            concerns.append("现金流质量一般，需要关注利润含金量")
        if solvency_score < 55 or (record.get("debt_to_asset") is not None and record["debt_to_asset"] >= 70):
            concerns.append("负债或流动性指标存在一定压力")
        if profitability_score < 55:
            concerns.append("盈利能力与盈利稳定性仍需验证")
        return concerns[:4]

    def _demo_stocks(self) -> list[tuple[Instrument, StockProfile]]:
        """Return the built-in demo stock directory for the web workbench."""

        return [
            (
                Instrument(
                    instrument_id="600519.SH",
                    symbol="600519.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="600519.SH",
                    symbol="600519.SH",
                    company_name="贵州茅台",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Consumer Staples",
                    industry="Baijiu",
                    concepts=("白酒", "高端消费", "品牌龙头"),
                    f10_summary="高端白酒龙头，品牌壁垒深厚，现金流质量高，渠道掌控力强。",
                    main_business="高端白酒的生产、销售与品牌运营。",
                    products_services="飞天茅台、系列酒、渠道服务。",
                    competitive_advantages="品牌稀缺性强，提价能力突出，经营现金流稳定。",
                    risks="消费景气波动、渠道库存波动、政策监管。",
                    pe_ttm=28.5,
                    pb=10.2,
                    roe=34.8,
                    revenue_growth=15.6,
                    net_profit_growth=16.9,
                    gross_margin=91.8,
                    net_margin=52.4,
                    debt_to_asset=14.2,
                    dividend_yield=2.7,
                    operating_cashflow_growth=18.6,
                    free_cashflow_margin=38.0,
                    current_ratio=4.6,
                    quick_ratio=4.1,
                    interest_coverage=45.0,
                    asset_turnover=0.72,
                    market_cap=24_000.0,
                    float_market_cap=20_500.0,
                    last_price=1_920.0,
                    listing_date=date(2001, 8, 27),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="000333.SZ",
                    symbol="000333.SZ",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="000333.SZ",
                    symbol="000333.SZ",
                    company_name="美的集团",
                    market_region="CN",
                    exchange_code="SZSE",
                    board="Main Board",
                    sector="Industrials",
                    industry="Home Appliances",
                    concepts=("家电", "智能制造", "机器人"),
                    f10_summary="家电和自动化平台型公司，具备渠道、供应链和制造效率优势。",
                    main_business="家电、暖通空调、机器人与自动化业务。",
                    products_services="空调、冰箱、洗衣机、机器人与楼宇科技。",
                    competitive_advantages="全球化制造能力，成本控制能力强，品牌矩阵完整。",
                    risks="地产链波动、原材料价格波动、海外需求变化。",
                    pe_ttm=13.4,
                    pb=3.1,
                    roe=24.6,
                    revenue_growth=8.8,
                    net_profit_growth=14.3,
                    gross_margin=25.5,
                    net_margin=9.4,
                    debt_to_asset=63.0,
                    dividend_yield=4.8,
                    operating_cashflow_growth=11.8,
                    free_cashflow_margin=7.4,
                    current_ratio=1.3,
                    quick_ratio=0.95,
                    interest_coverage=6.5,
                    asset_turnover=1.12,
                    market_cap=5_600.0,
                    float_market_cap=4_900.0,
                    last_price=74.8,
                    listing_date=date(2013, 9, 18),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="0700.HK",
                    symbol="0700.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=100,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="0700.HK",
                    symbol="0700.HK",
                    company_name="腾讯控股",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Communication Services",
                    industry="Internet Platforms",
                    concepts=("社交", "游戏", "云服务", "AI"),
                    f10_summary="社交、游戏、广告和云服务平台，具备超级流量入口和生态协同优势。",
                    main_business="社交网络、游戏、广告、金融科技和云服务。",
                    products_services="微信、QQ、腾讯游戏、腾讯云、广告平台。",
                    competitive_advantages="用户基础庞大，生态协同强，现金流稳定。",
                    risks="监管变化、游戏版号、广告景气波动。",
                    pe_ttm=17.2,
                    pb=3.4,
                    roe=19.5,
                    revenue_growth=9.4,
                    net_profit_growth=27.1,
                    gross_margin=48.9,
                    net_margin=31.2,
                    debt_to_asset=38.2,
                    dividend_yield=1.1,
                    operating_cashflow_growth=14.2,
                    free_cashflow_margin=24.7,
                    current_ratio=1.9,
                    quick_ratio=1.7,
                    interest_coverage=18.0,
                    asset_turnover=0.58,
                    market_cap=36_500.0,
                    float_market_cap=36_500.0,
                    last_price=392.0,
                    listing_date=date(2004, 6, 16),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="9988.HK",
                    symbol="9988.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=100,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="9988.HK",
                    symbol="9988.HK",
                    company_name="阿里巴巴-W",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="E-Commerce",
                    concepts=("电商", "云计算", "跨境", "AI"),
                    f10_summary="电商和云计算双核心平台，具备商家生态和数据积累优势。",
                    main_business="电商平台、云计算、物流与本地生活。",
                    products_services="淘宝天猫、阿里云、菜鸟、国际电商。",
                    competitive_advantages="平台规模大，商家生态完善，云业务具备扩张空间。",
                    risks="消费景气、行业竞争、监管与海外扩张不确定性。",
                    pe_ttm=12.8,
                    pb=1.7,
                    roe=12.5,
                    revenue_growth=6.9,
                    net_profit_growth=11.4,
                    gross_margin=38.0,
                    net_margin=13.1,
                    debt_to_asset=34.0,
                    dividend_yield=1.0,
                    operating_cashflow_growth=8.5,
                    free_cashflow_margin=9.8,
                    current_ratio=1.6,
                    quick_ratio=1.45,
                    interest_coverage=10.0,
                    asset_turnover=0.67,
                    market_cap=16_200.0,
                    float_market_cap=16_200.0,
                    last_price=82.0,
                    listing_date=date(2019, 11, 26),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="AAPL.US",
                    symbol="AAPL.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="AAPL.US",
                    symbol="AAPL.US",
                    company_name="Apple",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Information Technology",
                    industry="Consumer Electronics",
                    concepts=("硬件生态", "服务", "AI终端"),
                    f10_summary="硬件和服务生态协同，品牌强势，现金回流能力极强。",
                    main_business="智能手机、PC、可穿戴设备与数字服务。",
                    products_services="iPhone、Mac、iPad、Wearables、Services。",
                    competitive_advantages="品牌粘性高，硬件软件生态闭环，回购能力强。",
                    risks="新品周期、供应链风险、反垄断与区域需求波动。",
                    pe_ttm=29.7,
                    pb=40.3,
                    roe=155.0,
                    revenue_growth=4.2,
                    net_profit_growth=7.6,
                    gross_margin=45.1,
                    net_margin=25.8,
                    debt_to_asset=82.0,
                    dividend_yield=0.5,
                    operating_cashflow_growth=6.5,
                    free_cashflow_margin=22.5,
                    current_ratio=1.1,
                    quick_ratio=0.98,
                    interest_coverage=14.0,
                    asset_turnover=1.08,
                    market_cap=280_000.0,
                    float_market_cap=279_000.0,
                    last_price=192.0,
                    listing_date=date(1980, 12, 12),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="MSFT.US",
                    symbol="MSFT.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="MSFT.US",
                    symbol="MSFT.US",
                    company_name="Microsoft",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Information Technology",
                    industry="Software",
                    concepts=("云服务", "企业软件", "AI"),
                    f10_summary="企业软件和云服务双核心平台，AI 商业化落地能力强。",
                    main_business="生产力软件、云服务、操作系统和 AI 平台。",
                    products_services="Office、Azure、Windows、GitHub、Copilot。",
                    competitive_advantages="企业客户粘性高，订阅业务稳定，云业务具备规模效应。",
                    risks="云竞争加剧、AI 投入回收周期、反垄断风险。",
                    pe_ttm=33.6,
                    pb=11.8,
                    roe=35.3,
                    revenue_growth=16.2,
                    net_profit_growth=19.8,
                    gross_margin=68.9,
                    net_margin=35.1,
                    debt_to_asset=41.0,
                    dividend_yield=0.8,
                    operating_cashflow_growth=17.0,
                    free_cashflow_margin=31.5,
                    current_ratio=1.8,
                    quick_ratio=1.65,
                    interest_coverage=28.0,
                    asset_turnover=0.53,
                    market_cap=310_000.0,
                    float_market_cap=309_000.0,
                    last_price=415.0,
                    listing_date=date(1986, 3, 13),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="NVDA.US",
                    symbol="NVDA.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="NVDA.US",
                    symbol="NVDA.US",
                    company_name="NVIDIA",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Information Technology",
                    industry="Semiconductors",
                    concepts=("GPU", "AI算力", "数据中心"),
                    f10_summary="AI 算力核心公司，数据中心 GPU 需求强，软件生态壁垒明显。",
                    main_business="GPU、数据中心芯片、软件与平台生态。",
                    products_services="GeForce、Data Center GPU、CUDA、Networking。",
                    competitive_advantages="软件生态深、产品领先、AI 基础设施受益核心。",
                    risks="供需波动、出口限制、竞争加剧。",
                    pe_ttm=41.4,
                    pb=35.0,
                    roe=74.0,
                    revenue_growth=126.0,
                    net_profit_growth=148.0,
                    gross_margin=75.0,
                    net_margin=56.0,
                    debt_to_asset=28.0,
                    dividend_yield=0.1,
                    operating_cashflow_growth=120.0,
                    free_cashflow_margin=41.0,
                    current_ratio=3.3,
                    quick_ratio=2.9,
                    interest_coverage=40.0,
                    asset_turnover=1.04,
                    market_cap=220_000.0,
                    float_market_cap=219_000.0,
                    last_price=880.0,
                    listing_date=date(1999, 1, 22),
                    currency="USD",
                ),
            ),
            # ── CN A-share additions ─────────────────────────────────
            (
                Instrument(
                    instrument_id="601318.SH",
                    symbol="601318.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="601318.SH",
                    symbol="601318.SH",
                    company_name="中国平安",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Insurance",
                    concepts=("保险", "金融科技", "综合金融"),
                    f10_summary="综合金融龙头，保险、银行、投资与科技业务协同发展。",
                    main_business="人寿保险、财产保险、银行、资产管理与金融科技。",
                    products_services="寿险、车险、平安银行、陆金所、平安好医生。",
                    competitive_advantages="综合金融牌照齐全，科技赋能效率高，客户交叉销售能力强。",
                    risks="利率波动、资本市场波动、信用风险敞口。",
                    pe_ttm=9.2,
                    pb=1.1,
                    roe=12.8,
                    revenue_growth=4.5,
                    net_profit_growth=6.2,
                    gross_margin=None,
                    net_margin=8.5,
                    debt_to_asset=88.0,
                    dividend_yield=4.2,
                    operating_cashflow_growth=5.8,
                    free_cashflow_margin=4.2,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=3.5,
                    asset_turnover=0.12,
                    market_cap=8_600.0,
                    float_market_cap=7_900.0,
                    last_price=47.2,
                    listing_date=date(2007, 3, 1),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="600036.SH",
                    symbol="600036.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="600036.SH",
                    symbol="600036.SH",
                    company_name="招商银行",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Banking",
                    concepts=("银行", "零售金融", "财富管理"),
                    f10_summary="零售银行标杆，财富管理和零售信贷优势突出，资产质量优良。",
                    main_business="零售银行、公司银行、资产管理与财富管理。",
                    products_services="零售信贷、信用卡、理财、私人银行。",
                    competitive_advantages="零售客户基础庞大，资产质量行业领先，金融科技投入持续。",
                    risks="利率下行压缩息差、房地产风险敞口、监管趋严。",
                    pe_ttm=7.8,
                    pb=1.2,
                    roe=16.2,
                    revenue_growth=3.2,
                    net_profit_growth=8.5,
                    gross_margin=None,
                    net_margin=38.5,
                    debt_to_asset=92.0,
                    dividend_yield=4.5,
                    operating_cashflow_growth=6.5,
                    free_cashflow_margin=None,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=None,
                    asset_turnover=0.04,
                    market_cap=9_200.0,
                    float_market_cap=8_800.0,
                    last_price=36.5,
                    listing_date=date(2002, 4, 9),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="000858.SZ",
                    symbol="000858.SZ",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="000858.SZ",
                    symbol="000858.SZ",
                    company_name="五粮液",
                    market_region="CN",
                    exchange_code="SZSE",
                    board="Main Board",
                    sector="Consumer Staples",
                    industry="Baijiu",
                    concepts=("白酒", "高端消费", "品牌龙头"),
                    f10_summary="浓香型白酒龙头，品牌力仅次于茅台，渠道改革持续推进。",
                    main_business="高端浓香型白酒的生产与销售。",
                    products_services="五粮液、五粮春、系列酒产品。",
                    competitive_advantages="品牌积淀深厚，渠道覆盖面广，提价空间较大。",
                    risks="消费降级风险、渠道库存压力、行业竞争加剧。",
                    pe_ttm=21.5,
                    pb=6.8,
                    roe=28.6,
                    revenue_growth=12.8,
                    net_profit_growth=14.2,
                    gross_margin=76.5,
                    net_margin=37.8,
                    debt_to_asset=18.5,
                    dividend_yield=3.2,
                    operating_cashflow_growth=15.3,
                    free_cashflow_margin=30.5,
                    current_ratio=3.8,
                    quick_ratio=3.4,
                    interest_coverage=38.0,
                    asset_turnover=0.65,
                    market_cap=7_200.0,
                    float_market_cap=6_500.0,
                    last_price=185.0,
                    listing_date=date(1998, 4, 27),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="002594.SZ",
                    symbol="002594.SZ",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="002594.SZ",
                    symbol="002594.SZ",
                    company_name="比亚迪",
                    market_region="CN",
                    exchange_code="SZSE",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="Automobiles",
                    concepts=("新能源汽车", "动力电池", "智能驾驶"),
                    f10_summary="新能源汽车全产业链龙头，电池、整车和半导体垂直整合优势显著。",
                    main_business="新能源汽车、动力电池、半导体及轨道交通。",
                    products_services="王朝系列、海洋系列、刀片电池、比亚迪半导体。",
                    competitive_advantages="垂直整合产业链，成本控制力强，技术迭代快。",
                    risks="行业价格战、补贴退坡、海外拓展不确定性。",
                    pe_ttm=22.8,
                    pb=5.4,
                    roe=22.5,
                    revenue_growth=42.0,
                    net_profit_growth=80.5,
                    gross_margin=20.4,
                    net_margin=5.2,
                    debt_to_asset=75.0,
                    dividend_yield=0.5,
                    operating_cashflow_growth=55.0,
                    free_cashflow_margin=3.8,
                    current_ratio=1.1,
                    quick_ratio=0.8,
                    interest_coverage=5.5,
                    asset_turnover=0.95,
                    market_cap=8_800.0,
                    float_market_cap=7_200.0,
                    last_price=302.0,
                    listing_date=date(2011, 6, 30),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="600900.SH",
                    symbol="600900.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="600900.SH",
                    symbol="600900.SH",
                    company_name="长江电力",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Utilities",
                    industry="Hydroelectric Power",
                    concepts=("水电", "高股息", "公用事业"),
                    f10_summary="全球最大水电上市公司，现金流稳定，高分红特征明显。",
                    main_business="水力发电、电力销售与水资源综合利用。",
                    products_services="三峡、葛洲坝、溪洛渡、向家坝电站发电。",
                    competitive_advantages="水电资源稀缺，来水稳定后现金流可预测，分红率高。",
                    risks="来水量波动、电价改革政策、新增产能有限。",
                    pe_ttm=20.5,
                    pb=3.8,
                    roe=18.2,
                    revenue_growth=6.8,
                    net_profit_growth=8.5,
                    gross_margin=62.5,
                    net_margin=42.0,
                    debt_to_asset=55.0,
                    dividend_yield=3.8,
                    operating_cashflow_growth=7.2,
                    free_cashflow_margin=35.0,
                    current_ratio=0.8,
                    quick_ratio=0.75,
                    interest_coverage=4.5,
                    asset_turnover=0.18,
                    market_cap=5_800.0,
                    float_market_cap=4_200.0,
                    last_price=25.6,
                    listing_date=date(2003, 11, 18),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="601899.SH",
                    symbol="601899.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="601899.SH",
                    symbol="601899.SH",
                    company_name="紫金矿业",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Materials",
                    industry="Gold & Copper Mining",
                    concepts=("黄金", "铜矿", "资源龙头"),
                    f10_summary="全球化矿业龙头，金、铜、锌多金属布局，储量增长空间大。",
                    main_business="金、铜、锌等矿产资源的采选冶炼与销售。",
                    products_services="矿产金、矿产铜、矿产锌、冶炼产品。",
                    competitive_advantages="矿产资源储量丰富，海外并购扩张能力强，成本控制出色。",
                    risks="金属价格波动、海外政治风险、环保与安全监管。",
                    pe_ttm=12.5,
                    pb=3.2,
                    roe=24.5,
                    revenue_growth=18.5,
                    net_profit_growth=25.0,
                    gross_margin=18.2,
                    net_margin=11.5,
                    debt_to_asset=56.0,
                    dividend_yield=2.5,
                    operating_cashflow_growth=22.0,
                    free_cashflow_margin=8.5,
                    current_ratio=1.4,
                    quick_ratio=0.9,
                    interest_coverage=6.8,
                    asset_turnover=0.75,
                    market_cap=4_200.0,
                    float_market_cap=3_800.0,
                    last_price=15.8,
                    listing_date=date(2008, 4, 25),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="300750.SZ",
                    symbol="300750.SZ",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="300750.SZ",
                    symbol="300750.SZ",
                    company_name="宁德时代",
                    market_region="CN",
                    exchange_code="SZSE",
                    board="ChiNext",
                    sector="Industrials",
                    industry="Battery Manufacturing",
                    concepts=("动力电池", "储能", "新能源"),
                    f10_summary="全球动力电池龙头，市占率领先，储能业务快速增长。",
                    main_business="动力电池系统、储能系统的研发、生产与销售。",
                    products_services="动力电池、储能电池、电池材料、电池回收。",
                    competitive_advantages="技术领先，规模效应显著，客户绑定深度高。",
                    risks="上游原材料波动、技术路线变化、海外竞争加剧。",
                    pe_ttm=20.8,
                    pb=5.6,
                    roe=22.0,
                    revenue_growth=22.5,
                    net_profit_growth=28.0,
                    gross_margin=22.8,
                    net_margin=12.5,
                    debt_to_asset=68.0,
                    dividend_yield=0.6,
                    operating_cashflow_growth=35.0,
                    free_cashflow_margin=6.5,
                    current_ratio=1.3,
                    quick_ratio=1.0,
                    interest_coverage=7.5,
                    asset_turnover=0.82,
                    market_cap=10_500.0,
                    float_market_cap=7_800.0,
                    last_price=215.0,
                    listing_date=date(2018, 6, 11),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="002415.SZ",
                    symbol="002415.SZ",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="002415.SZ",
                    symbol="002415.SZ",
                    company_name="海康威视",
                    market_region="CN",
                    exchange_code="SZSE",
                    board="Main Board",
                    sector="Information Technology",
                    industry="Security & Surveillance",
                    concepts=("安防", "AI", "智能物联"),
                    f10_summary="全球安防龙头，AI赋能智能物联转型，海外渠道布局完善。",
                    main_business="视频监控产品、智能物联解决方案与创新业务。",
                    products_services="摄像头、NVR、AI开放平台、机器人、汽车电子。",
                    competitive_advantages="研发投入持续高位，产品线完整，全球渠道覆盖广。",
                    risks="地缘政治制裁、行业需求波动、创新业务盈利周期长。",
                    pe_ttm=18.5,
                    pb=4.2,
                    roe=22.8,
                    revenue_growth=7.5,
                    net_profit_growth=9.8,
                    gross_margin=44.2,
                    net_margin=18.5,
                    debt_to_asset=40.0,
                    dividend_yield=3.5,
                    operating_cashflow_growth=12.0,
                    free_cashflow_margin=12.8,
                    current_ratio=2.2,
                    quick_ratio=1.8,
                    interest_coverage=15.0,
                    asset_turnover=0.72,
                    market_cap=4_500.0,
                    float_market_cap=3_900.0,
                    last_price=38.5,
                    listing_date=date(2010, 5, 28),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="600276.SH",
                    symbol="600276.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="600276.SH",
                    symbol="600276.SH",
                    company_name="恒瑞医药",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Health Care",
                    industry="Pharmaceuticals",
                    concepts=("创新药", "肿瘤", "国际化"),
                    f10_summary="国内创新药龙头，肿瘤管线丰富，国际化进程加速。",
                    main_business="创新药与仿制药的研发、生产与销售。",
                    products_services="抗肿瘤药、麻醉药、造影剂、创新生物药。",
                    competitive_advantages="研发管线深厚，创新药获批节奏快，销售团队经验丰富。",
                    risks="集采降价风险、研发失败风险、国际化进展不确定。",
                    pe_ttm=42.0,
                    pb=8.5,
                    roe=18.5,
                    revenue_growth=10.2,
                    net_profit_growth=15.5,
                    gross_margin=85.0,
                    net_margin=22.5,
                    debt_to_asset=15.0,
                    dividend_yield=0.8,
                    operating_cashflow_growth=18.0,
                    free_cashflow_margin=16.5,
                    current_ratio=5.5,
                    quick_ratio=5.0,
                    interest_coverage=55.0,
                    asset_turnover=0.52,
                    market_cap=3_200.0,
                    float_market_cap=3_000.0,
                    last_price=50.2,
                    listing_date=date(2000, 10, 18),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="601012.SH",
                    symbol="601012.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="601012.SH",
                    symbol="601012.SH",
                    company_name="隆基绿能",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Industrials",
                    industry="Solar Energy",
                    concepts=("光伏", "硅片", "绿色能源"),
                    f10_summary="全球光伏硅片和组件龙头，一体化产能布局完善。",
                    main_business="单晶硅片、电池组件、光伏电站的研发与销售。",
                    products_services="单晶硅片、HPBC电池、组件、分布式电站。",
                    competitive_advantages="技术路线领先，一体化成本优势明显，全球渠道成熟。",
                    risks="产能过剩、硅料价格波动、技术迭代风险。",
                    pe_ttm=15.2,
                    pb=1.8,
                    roe=10.5,
                    revenue_growth=-8.5,
                    net_profit_growth=-35.0,
                    gross_margin=17.5,
                    net_margin=6.2,
                    debt_to_asset=58.0,
                    dividend_yield=2.2,
                    operating_cashflow_growth=-15.0,
                    free_cashflow_margin=2.5,
                    current_ratio=1.2,
                    quick_ratio=0.85,
                    interest_coverage=4.8,
                    asset_turnover=0.68,
                    market_cap=1_800.0,
                    float_market_cap=1_600.0,
                    last_price=22.5,
                    listing_date=date(2012, 4, 11),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="000001.SZ",
                    symbol="000001.SZ",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="000001.SZ",
                    symbol="000001.SZ",
                    company_name="平安银行",
                    market_region="CN",
                    exchange_code="SZSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Banking",
                    concepts=("银行", "零售转型", "金融科技"),
                    f10_summary="平安集团旗下银行，零售转型成效显著，科技驱动运营效率提升。",
                    main_business="零售银行、对公银行、资金同业业务。",
                    products_services="零售信贷、信用卡、对公贷款、理财产品。",
                    competitive_advantages="背靠平安集团，客户导流优势明显，科技投入领先同业。",
                    risks="息差收窄、资产质量波动、零售信贷风险。",
                    pe_ttm=6.5,
                    pb=0.6,
                    roe=10.2,
                    revenue_growth=-2.5,
                    net_profit_growth=2.1,
                    gross_margin=None,
                    net_margin=28.5,
                    debt_to_asset=93.0,
                    dividend_yield=5.8,
                    operating_cashflow_growth=4.2,
                    free_cashflow_margin=None,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=None,
                    asset_turnover=0.03,
                    market_cap=2_600.0,
                    float_market_cap=2_500.0,
                    last_price=13.4,
                    listing_date=date(1991, 4, 3),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="600309.SH",
                    symbol="600309.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="600309.SH",
                    symbol="600309.SH",
                    company_name="万华化学",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Materials",
                    industry="Chemicals",
                    concepts=("MDI", "化工龙头", "新材料"),
                    f10_summary="全球MDI龙头，化工新材料布局持续扩展，成本优势显著。",
                    main_business="MDI、TDI等聚氨酯系列产品及新材料的研发与销售。",
                    products_services="MDI、TDI、聚醚多元醇、特种化学品。",
                    competitive_advantages="全球MDI市占率领先，一体化装置成本低，技术壁垒高。",
                    risks="化工品价格周期波动、产能扩张风险、环保政策。",
                    pe_ttm=16.5,
                    pb=3.5,
                    roe=20.8,
                    revenue_growth=10.5,
                    net_profit_growth=12.8,
                    gross_margin=28.5,
                    net_margin=14.2,
                    debt_to_asset=55.0,
                    dividend_yield=2.8,
                    operating_cashflow_growth=15.0,
                    free_cashflow_margin=9.5,
                    current_ratio=1.5,
                    quick_ratio=1.1,
                    interest_coverage=8.5,
                    asset_turnover=0.68,
                    market_cap=2_800.0,
                    float_market_cap=2_500.0,
                    last_price=88.5,
                    listing_date=date(2001, 1, 5),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="002714.SZ",
                    symbol="002714.SZ",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="002714.SZ",
                    symbol="002714.SZ",
                    company_name="牧原股份",
                    market_region="CN",
                    exchange_code="SZSE",
                    board="Main Board",
                    sector="Consumer Staples",
                    industry="Animal Husbandry",
                    concepts=("生猪养殖", "猪周期", "农业龙头"),
                    f10_summary="生猪养殖龙头，自繁自养模式成本领先，出栏量持续增长。",
                    main_business="生猪的养殖与销售。",
                    products_services="商品猪、仔猪、种猪。",
                    competitive_advantages="自繁自养一体化，养殖成本行业最低，规模扩张能力强。",
                    risks="猪价周期波动、疫病风险、资金链压力。",
                    pe_ttm=18.0,
                    pb=4.5,
                    roe=25.0,
                    revenue_growth=28.0,
                    net_profit_growth=120.0,
                    gross_margin=22.0,
                    net_margin=12.5,
                    debt_to_asset=58.0,
                    dividend_yield=1.0,
                    operating_cashflow_growth=45.0,
                    free_cashflow_margin=5.5,
                    current_ratio=1.1,
                    quick_ratio=0.7,
                    interest_coverage=4.2,
                    asset_turnover=0.55,
                    market_cap=3_500.0,
                    float_market_cap=2_800.0,
                    last_price=65.0,
                    listing_date=date(2014, 1, 28),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="601888.SH",
                    symbol="601888.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="601888.SH",
                    symbol="601888.SH",
                    company_name="中国中免",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="Duty-Free Retail",
                    concepts=("免税", "消费复苏", "旅游"),
                    f10_summary="全球最大免税运营商之一，受益出境游复苏和离岛免税政策。",
                    main_business="免税商品的批发与零售。",
                    products_services="香化、精品、烟酒、食品等免税商品。",
                    competitive_advantages="免税牌照稀缺，采购规模优势大，线上线下渠道融合。",
                    risks="出行需求波动、政策变化、代购分流。",
                    pe_ttm=25.0,
                    pb=4.8,
                    roe=18.5,
                    revenue_growth=12.0,
                    net_profit_growth=16.0,
                    gross_margin=32.5,
                    net_margin=12.0,
                    debt_to_asset=45.0,
                    dividend_yield=1.5,
                    operating_cashflow_growth=20.0,
                    free_cashflow_margin=8.5,
                    current_ratio=1.6,
                    quick_ratio=1.2,
                    interest_coverage=12.0,
                    asset_turnover=0.82,
                    market_cap=3_000.0,
                    float_market_cap=2_200.0,
                    last_price=145.0,
                    listing_date=date(2009, 10, 15),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="603259.SH",
                    symbol="603259.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="603259.SH",
                    symbol="603259.SH",
                    company_name="药明康德",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Health Care",
                    industry="CRO/CDMO",
                    concepts=("CXO", "创新药服务", "全球化"),
                    f10_summary="全球领先CRO/CDMO平台，一体化服务能力强，客户粘性高。",
                    main_business="药物发现、研发服务、生产外包（CDMO）。",
                    products_services="化学药研发服务、生物学服务、细胞与基因疗法CDMO。",
                    competitive_advantages="一体化研发平台，全球客户覆盖，技术与产能壁垒高。",
                    risks="地缘政治风险、大客户集中、行业投融资波动。",
                    pe_ttm=22.0,
                    pb=4.5,
                    roe=18.0,
                    revenue_growth=8.5,
                    net_profit_growth=10.2,
                    gross_margin=38.0,
                    net_margin=20.5,
                    debt_to_asset=32.0,
                    dividend_yield=0.8,
                    operating_cashflow_growth=12.0,
                    free_cashflow_margin=14.0,
                    current_ratio=2.5,
                    quick_ratio=2.2,
                    interest_coverage=18.0,
                    asset_turnover=0.48,
                    market_cap=2_500.0,
                    float_market_cap=2_100.0,
                    last_price=85.0,
                    listing_date=date(2018, 5, 8),
                    currency="CNY",
                ),
            ),
            # ── HK additions ────────────────────────────────────────
            (
                Instrument(
                    instrument_id="03690.HK",
                    symbol="03690.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=100,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="03690.HK",
                    symbol="03690.HK",
                    company_name="美团-W",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="Local Services Platform",
                    concepts=("外卖", "本地生活", "即时零售"),
                    f10_summary="本地生活服务平台龙头，外卖、到店与新业务协同发展。",
                    main_business="外卖配送、到店酒旅、社区团购与即时零售。",
                    products_services="美团外卖、大众点评、美团优选、美团买菜。",
                    competitive_advantages="配送网络壁垒深厚，商家和用户双边粘性强。",
                    risks="行业竞争加剧、监管政策、新业务亏损。",
                    pe_ttm=28.5,
                    pb=4.2,
                    roe=15.0,
                    revenue_growth=22.5,
                    net_profit_growth=55.0,
                    gross_margin=35.8,
                    net_margin=8.5,
                    debt_to_asset=42.0,
                    dividend_yield=0.0,
                    operating_cashflow_growth=40.0,
                    free_cashflow_margin=6.2,
                    current_ratio=1.8,
                    quick_ratio=1.6,
                    interest_coverage=12.0,
                    asset_turnover=0.72,
                    market_cap=9_500.0,
                    float_market_cap=9_500.0,
                    last_price=155.0,
                    listing_date=date(2018, 9, 20),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="01810.HK",
                    symbol="01810.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=200,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 200},
                ),
                StockProfile(
                    instrument_id="01810.HK",
                    symbol="01810.HK",
                    company_name="小米集团-W",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Information Technology",
                    industry="Consumer Electronics",
                    concepts=("手机", "IoT", "智能汽车"),
                    f10_summary="智能手机与AIoT生态平台，汽车业务开启第二增长曲线。",
                    main_business="智能手机、IoT与生活消费产品、互联网服务、智能汽车。",
                    products_services="小米手机、Redmi、小米汽车SU7、米家IoT产品。",
                    competitive_advantages="性价比品牌定位精准，IoT生态链完善，汽车业务打开增量空间。",
                    risks="手机行业竞争、汽车业务投入大、海外市场地缘风险。",
                    pe_ttm=22.0,
                    pb=4.8,
                    roe=18.5,
                    revenue_growth=18.0,
                    net_profit_growth=25.0,
                    gross_margin=21.5,
                    net_margin=6.8,
                    debt_to_asset=48.0,
                    dividend_yield=0.0,
                    operating_cashflow_growth=30.0,
                    free_cashflow_margin=4.5,
                    current_ratio=1.6,
                    quick_ratio=1.2,
                    interest_coverage=10.0,
                    asset_turnover=1.05,
                    market_cap=7_200.0,
                    float_market_cap=7_200.0,
                    last_price=28.5,
                    listing_date=date(2018, 7, 9),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="09618.HK",
                    symbol="09618.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=50,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 50},
                ),
                StockProfile(
                    instrument_id="09618.HK",
                    symbol="09618.HK",
                    company_name="京东集团-SW",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="E-Commerce",
                    concepts=("电商", "物流", "供应链"),
                    f10_summary="自营电商与物流一体化平台，品质电商定位差异化明显。",
                    main_business="自营电商、第三方市场、物流与健康业务。",
                    products_services="京东商城、京东物流、京东健康、京东工业。",
                    competitive_advantages="自建物流网络完善，供应链效率高，用户信任度强。",
                    risks="消费景气、低价竞争、物流投入持续。",
                    pe_ttm=11.5,
                    pb=1.6,
                    roe=14.2,
                    revenue_growth=5.5,
                    net_profit_growth=18.0,
                    gross_margin=15.2,
                    net_margin=3.8,
                    debt_to_asset=52.0,
                    dividend_yield=1.8,
                    operating_cashflow_growth=15.0,
                    free_cashflow_margin=3.2,
                    current_ratio=1.2,
                    quick_ratio=0.95,
                    interest_coverage=8.0,
                    asset_turnover=1.85,
                    market_cap=4_200.0,
                    float_market_cap=4_200.0,
                    last_price=135.0,
                    listing_date=date(2020, 6, 18),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="09888.HK",
                    symbol="09888.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=100,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="09888.HK",
                    symbol="09888.HK",
                    company_name="百度集团-SW",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Communication Services",
                    industry="Internet Search & AI",
                    concepts=("搜索", "AI大模型", "自动驾驶"),
                    f10_summary="搜索引擎龙头，AI大模型与自动驾驶双轮驱动转型。",
                    main_business="搜索引擎、AI云服务、自动驾驶与智能驾驶。",
                    products_services="百度搜索、文心一言、百度智能云、Apollo。",
                    competitive_advantages="AI技术积累深厚，搜索现金流稳定，自动驾驶先发布局。",
                    risks="AI商业化节奏、搜索份额下滑、自动驾驶投入回收周期长。",
                    pe_ttm=12.0,
                    pb=1.2,
                    roe=9.8,
                    revenue_growth=5.8,
                    net_profit_growth=12.5,
                    gross_margin=52.0,
                    net_margin=16.5,
                    debt_to_asset=35.0,
                    dividend_yield=0.0,
                    operating_cashflow_growth=10.0,
                    free_cashflow_margin=12.0,
                    current_ratio=2.5,
                    quick_ratio=2.2,
                    interest_coverage=15.0,
                    asset_turnover=0.42,
                    market_cap=3_200.0,
                    float_market_cap=3_200.0,
                    last_price=92.0,
                    listing_date=date(2021, 3, 23),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="02318.HK",
                    symbol="02318.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=500,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 500},
                ),
                StockProfile(
                    instrument_id="02318.HK",
                    symbol="02318.HK",
                    company_name="中国平安",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Financials",
                    industry="Insurance",
                    concepts=("保险", "综合金融", "金融科技"),
                    f10_summary="综合金融龙头H股，保险、银行、投资多元业务协同。",
                    main_business="人寿保险、财产保险、银行、资产管理与金融科技。",
                    products_services="寿险、产险、平安银行、陆金所。",
                    competitive_advantages="综合金融牌照齐全，科技赋能运营效率高。",
                    risks="利率波动、资本市场波动、信用风险。",
                    pe_ttm=8.8,
                    pb=1.0,
                    roe=12.5,
                    revenue_growth=4.2,
                    net_profit_growth=5.8,
                    gross_margin=None,
                    net_margin=8.2,
                    debt_to_asset=88.0,
                    dividend_yield=4.5,
                    operating_cashflow_growth=5.5,
                    free_cashflow_margin=4.0,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=3.2,
                    asset_turnover=0.12,
                    market_cap=8_200.0,
                    float_market_cap=8_200.0,
                    last_price=45.0,
                    listing_date=date(2004, 6, 24),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="00941.HK",
                    symbol="00941.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=500,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 500},
                ),
                StockProfile(
                    instrument_id="00941.HK",
                    symbol="00941.HK",
                    company_name="中国移动",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Communication Services",
                    industry="Telecom",
                    concepts=("5G", "通信", "高股息"),
                    f10_summary="全球用户规模最大的电信运营商，5G和数字化转型持续推进。",
                    main_business="移动通信、宽带及数字化服务。",
                    products_services="移动通信、家庭宽带、政企服务、移动云。",
                    competitive_advantages="用户规模最大，5G投资领先，分红稳定。",
                    risks="行业增速放缓、资本开支压力、竞争格局。",
                    pe_ttm=11.5,
                    pb=1.4,
                    roe=12.0,
                    revenue_growth=6.5,
                    net_profit_growth=8.0,
                    gross_margin=32.0,
                    net_margin=14.5,
                    debt_to_asset=35.0,
                    dividend_yield=5.5,
                    operating_cashflow_growth=7.0,
                    free_cashflow_margin=10.5,
                    current_ratio=1.0,
                    quick_ratio=0.9,
                    interest_coverage=20.0,
                    asset_turnover=0.48,
                    market_cap=18_000.0,
                    float_market_cap=18_000.0,
                    last_price=82.0,
                    listing_date=date(1997, 10, 23),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="01211.HK",
                    symbol="01211.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=500,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 500},
                ),
                StockProfile(
                    instrument_id="01211.HK",
                    symbol="01211.HK",
                    company_name="比亚迪股份",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="Automobiles",
                    concepts=("新能源汽车", "电池", "智能驾驶"),
                    f10_summary="新能源汽车全产业链龙头H股，电池和整车垂直整合。",
                    main_business="新能源汽车、动力电池、半导体。",
                    products_services="王朝系列、海洋系列、刀片电池。",
                    competitive_advantages="垂直整合，成本控制力强，出海加速。",
                    risks="行业价格战、补贴退坡、海外拓展风险。",
                    pe_ttm=23.5,
                    pb=5.6,
                    roe=22.0,
                    revenue_growth=40.0,
                    net_profit_growth=78.0,
                    gross_margin=20.2,
                    net_margin=5.0,
                    debt_to_asset=75.0,
                    dividend_yield=0.4,
                    operating_cashflow_growth=52.0,
                    free_cashflow_margin=3.5,
                    current_ratio=1.1,
                    quick_ratio=0.8,
                    interest_coverage=5.2,
                    asset_turnover=0.95,
                    market_cap=8_500.0,
                    float_market_cap=8_500.0,
                    last_price=290.0,
                    listing_date=date(2002, 7, 31),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="00388.HK",
                    symbol="00388.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=100,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="00388.HK",
                    symbol="00388.HK",
                    company_name="香港交易所",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Financials",
                    industry="Exchanges",
                    concepts=("交易所", "互联互通", "金融基础设施"),
                    f10_summary="亚洲领先交易所集团，受益互联互通与IPO市场活跃。",
                    main_business="股票、衍生品、大宗商品交易与结算服务。",
                    products_services="现货市场、衍生品市场、LME、沪深港通。",
                    competitive_advantages="垄断地位，互联互通独特优势，收入与市场活跃度正相关。",
                    risks="市场成交量波动、监管变化、地缘政治。",
                    pe_ttm=32.0,
                    pb=8.5,
                    roe=24.0,
                    revenue_growth=8.5,
                    net_profit_growth=12.0,
                    gross_margin=68.0,
                    net_margin=52.0,
                    debt_to_asset=25.0,
                    dividend_yield=2.8,
                    operating_cashflow_growth=10.0,
                    free_cashflow_margin=45.0,
                    current_ratio=2.8,
                    quick_ratio=2.5,
                    interest_coverage=35.0,
                    asset_turnover=0.22,
                    market_cap=4_800.0,
                    float_market_cap=4_800.0,
                    last_price=375.0,
                    listing_date=date(2000, 6, 27),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="02020.HK",
                    symbol="02020.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=200,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 200},
                ),
                StockProfile(
                    instrument_id="02020.HK",
                    symbol="02020.HK",
                    company_name="安踏体育",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="Sportswear",
                    concepts=("运动服饰", "品牌矩阵", "消费升级"),
                    f10_summary="中国运动品牌龙头，多品牌矩阵覆盖全价位段。",
                    main_business="运动鞋服的设计、研发、生产与销售。",
                    products_services="安踏、FILA、始祖鸟、迪桑特、可隆。",
                    competitive_advantages="多品牌战略成功，DTC转型领先，供应链效率高。",
                    risks="消费景气波动、品牌老化风险、海外拓展。",
                    pe_ttm=22.5,
                    pb=6.2,
                    roe=26.5,
                    revenue_growth=16.5,
                    net_profit_growth=20.0,
                    gross_margin=62.5,
                    net_margin=18.5,
                    debt_to_asset=38.0,
                    dividend_yield=1.5,
                    operating_cashflow_growth=18.0,
                    free_cashflow_margin=14.5,
                    current_ratio=2.2,
                    quick_ratio=1.8,
                    interest_coverage=22.0,
                    asset_turnover=0.85,
                    market_cap=2_800.0,
                    float_market_cap=2_800.0,
                    last_price=98.0,
                    listing_date=date(2007, 7, 10),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="01024.HK",
                    symbol="01024.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=100,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="01024.HK",
                    symbol="01024.HK",
                    company_name="快手-W",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Communication Services",
                    industry="Short Video & Live Streaming",
                    concepts=("短视频", "直播电商", "AI"),
                    f10_summary="短视频和直播电商平台，用户规模庞大，商业化持续优化。",
                    main_business="短视频社区、直播、电商与广告服务。",
                    products_services="快手App、快手电商、磁力引擎广告。",
                    competitive_advantages="下沉市场用户粘性高，电商闭环持续完善。",
                    risks="行业竞争激烈、内容监管、商业化天花板。",
                    pe_ttm=18.0,
                    pb=2.5,
                    roe=12.0,
                    revenue_growth=15.0,
                    net_profit_growth=85.0,
                    gross_margin=50.0,
                    net_margin=10.5,
                    debt_to_asset=28.0,
                    dividend_yield=0.0,
                    operating_cashflow_growth=45.0,
                    free_cashflow_margin=8.5,
                    current_ratio=3.0,
                    quick_ratio=2.8,
                    interest_coverage=25.0,
                    asset_turnover=0.62,
                    market_cap=2_400.0,
                    float_market_cap=2_400.0,
                    last_price=55.0,
                    listing_date=date(2021, 2, 5),
                    currency="HKD",
                ),
            ),
            # ── US additions ─────────────────────────────────────────
            (
                Instrument(
                    instrument_id="GOOGL.US",
                    symbol="GOOGL.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="GOOGL.US",
                    symbol="GOOGL.US",
                    company_name="Alphabet",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Communication Services",
                    industry="Internet Search & Advertising",
                    concepts=("搜索广告", "云服务", "AI"),
                    f10_summary="全球搜索与广告龙头，云计算和AI布局全面，现金流充裕。",
                    main_business="搜索广告、YouTube、Google Cloud、Waymo。",
                    products_services="Google Search、YouTube、Google Cloud、Android。",
                    competitive_advantages="搜索垄断地位，广告生态完整，AI技术储备深厚。",
                    risks="反垄断诉讼、AI竞争加剧、广告景气波动。",
                    pe_ttm=22.5,
                    pb=6.8,
                    roe=28.5,
                    revenue_growth=13.5,
                    net_profit_growth=28.0,
                    gross_margin=57.5,
                    net_margin=25.8,
                    debt_to_asset=22.0,
                    dividend_yield=0.5,
                    operating_cashflow_growth=20.0,
                    free_cashflow_margin=22.0,
                    current_ratio=2.1,
                    quick_ratio=2.0,
                    interest_coverage=45.0,
                    asset_turnover=0.72,
                    market_cap=210_000.0,
                    float_market_cap=209_000.0,
                    last_price=170.0,
                    listing_date=date(2004, 8, 19),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="AMZN.US",
                    symbol="AMZN.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="AMZN.US",
                    symbol="AMZN.US",
                    company_name="Amazon",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="E-Commerce & Cloud",
                    concepts=("电商", "云服务", "AI"),
                    f10_summary="全球电商与云计算双龙头，AWS利润率持续改善。",
                    main_business="电商零售、AWS云服务、广告与订阅服务。",
                    products_services="Amazon.com、AWS、Prime、Alexa。",
                    competitive_advantages="电商物流网络庞大，AWS领先地位稳固，广告业务高增长。",
                    risks="零售利润率压力、云竞争、监管与劳工成本。",
                    pe_ttm=38.0,
                    pb=8.5,
                    roe=22.0,
                    revenue_growth=12.5,
                    net_profit_growth=55.0,
                    gross_margin=48.5,
                    net_margin=7.8,
                    debt_to_asset=52.0,
                    dividend_yield=0.0,
                    operating_cashflow_growth=35.0,
                    free_cashflow_margin=8.5,
                    current_ratio=1.1,
                    quick_ratio=0.85,
                    interest_coverage=12.0,
                    asset_turnover=1.15,
                    market_cap=195_000.0,
                    float_market_cap=194_000.0,
                    last_price=188.0,
                    listing_date=date(1997, 5, 15),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="META.US",
                    symbol="META.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="META.US",
                    symbol="META.US",
                    company_name="Meta Platforms",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Communication Services",
                    industry="Social Media",
                    concepts=("社交网络", "广告", "元宇宙", "AI"),
                    f10_summary="全球社交广告龙头，AI驱动广告效率提升，元宇宙长期投入。",
                    main_business="社交网络平台、数字广告、VR/AR与AI。",
                    products_services="Facebook、Instagram、WhatsApp、Reality Labs。",
                    competitive_advantages="用户基数庞大，广告精准投放能力强，AI持续优化变现。",
                    risks="隐私监管、元宇宙投入回收慢、用户增长放缓。",
                    pe_ttm=24.5,
                    pb=7.8,
                    roe=32.0,
                    revenue_growth=22.0,
                    net_profit_growth=48.0,
                    gross_margin=81.5,
                    net_margin=33.5,
                    debt_to_asset=28.0,
                    dividend_yield=0.4,
                    operating_cashflow_growth=35.0,
                    free_cashflow_margin=22.0,
                    current_ratio=2.7,
                    quick_ratio=2.5,
                    interest_coverage=55.0,
                    asset_turnover=0.72,
                    market_cap=150_000.0,
                    float_market_cap=149_000.0,
                    last_price=590.0,
                    listing_date=date(2012, 5, 18),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="TSLA.US",
                    symbol="TSLA.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="TSLA.US",
                    symbol="TSLA.US",
                    company_name="Tesla",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="Electric Vehicles",
                    concepts=("电动车", "自动驾驶", "能源存储"),
                    f10_summary="全球电动车龙头，软件与能源业务打开第二曲线。",
                    main_business="电动汽车、储能系统、太阳能与自动驾驶。",
                    products_services="Model 3/Y/S/X、Megapack、FSD、Cybertruck。",
                    competitive_advantages="品牌力强，软件OTA能力领先，制造效率持续优化。",
                    risks="竞争加剧、需求波动、CEO风险、自动驾驶法规。",
                    pe_ttm=55.0,
                    pb=12.5,
                    roe=22.0,
                    revenue_growth=8.5,
                    net_profit_growth=-15.0,
                    gross_margin=18.5,
                    net_margin=8.2,
                    debt_to_asset=38.0,
                    dividend_yield=0.0,
                    operating_cashflow_growth=-5.0,
                    free_cashflow_margin=4.5,
                    current_ratio=1.7,
                    quick_ratio=1.3,
                    interest_coverage=15.0,
                    asset_turnover=0.85,
                    market_cap=85_000.0,
                    float_market_cap=84_000.0,
                    last_price=265.0,
                    listing_date=date(2010, 6, 29),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="JPM.US",
                    symbol="JPM.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="JPM.US",
                    symbol="JPM.US",
                    company_name="JPMorgan Chase",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Banking",
                    concepts=("银行", "投行", "资产管理"),
                    f10_summary="全球最大综合性银行，投行、零售银行和资管业务均衡发展。",
                    main_business="零售银行、投资银行、资产管理与商业银行。",
                    products_services="消费银行、CIB、资产与财富管理、商业银行。",
                    competitive_advantages="规模效应显著，风控能力行业领先，科技投入持续高位。",
                    risks="利率环境变化、信用风险、监管资本要求。",
                    pe_ttm=12.5,
                    pb=2.0,
                    roe=17.0,
                    revenue_growth=8.5,
                    net_profit_growth=12.0,
                    gross_margin=None,
                    net_margin=32.0,
                    debt_to_asset=90.0,
                    dividend_yield=2.2,
                    operating_cashflow_growth=10.0,
                    free_cashflow_margin=None,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=None,
                    asset_turnover=0.04,
                    market_cap=58_000.0,
                    float_market_cap=57_000.0,
                    last_price=198.0,
                    listing_date=date(1969, 3, 5),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="V.US",
                    symbol="V.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="V.US",
                    symbol="V.US",
                    company_name="Visa",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Payment Networks",
                    concepts=("支付", "金融科技", "数字支付"),
                    f10_summary="全球最大支付网络，轻资产模式，利润率极高。",
                    main_business="全球支付网络、交易处理与增值服务。",
                    products_services="Visa信用卡网络、Visa Direct、B2B支付。",
                    competitive_advantages="双边网络效应强，轻资产高利润率，全球覆盖广。",
                    risks="监管费率压力、数字货币竞争、跨境交易波动。",
                    pe_ttm=30.5,
                    pb=14.0,
                    roe=45.0,
                    revenue_growth=10.5,
                    net_profit_growth=14.0,
                    gross_margin=80.0,
                    net_margin=54.0,
                    debt_to_asset=45.0,
                    dividend_yield=0.8,
                    operating_cashflow_growth=12.0,
                    free_cashflow_margin=48.0,
                    current_ratio=1.5,
                    quick_ratio=1.5,
                    interest_coverage=25.0,
                    asset_turnover=0.55,
                    market_cap=56_000.0,
                    float_market_cap=55_500.0,
                    last_price=280.0,
                    listing_date=date(2008, 3, 19),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="UNH.US",
                    symbol="UNH.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="UNH.US",
                    symbol="UNH.US",
                    company_name="UnitedHealth Group",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Health Care",
                    industry="Managed Care",
                    concepts=("医疗保险", "健康服务", "PBM"),
                    f10_summary="全球最大医疗保险和健康服务公司，Optum业务快速增长。",
                    main_business="医疗保险（UnitedHealthcare）与健康服务（Optum）。",
                    products_services="商业医保、Medicare Advantage、Optum Health/Rx。",
                    competitive_advantages="规模优势显著，数据和科技能力强，业务多元化。",
                    risks="医疗政策变化、赔付率波动、反垄断审查。",
                    pe_ttm=20.0,
                    pb=5.8,
                    roe=28.0,
                    revenue_growth=12.0,
                    net_profit_growth=10.5,
                    gross_margin=24.5,
                    net_margin=6.2,
                    debt_to_asset=62.0,
                    dividend_yield=1.4,
                    operating_cashflow_growth=8.0,
                    free_cashflow_margin=5.5,
                    current_ratio=0.8,
                    quick_ratio=0.75,
                    interest_coverage=12.0,
                    asset_turnover=1.35,
                    market_cap=48_000.0,
                    float_market_cap=47_500.0,
                    last_price=520.0,
                    listing_date=date(1984, 10, 17),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="WMT.US",
                    symbol="WMT.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="WMT.US",
                    symbol="WMT.US",
                    company_name="Walmart",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Consumer Staples",
                    industry="Retail",
                    concepts=("零售", "电商", "供应链"),
                    f10_summary="全球最大零售商，全渠道转型成效显著，广告和会员业务增长。",
                    main_business="大卖场、电商、会员制零售与国际业务。",
                    products_services="Walmart门店、Walmart+、Sam's Club、电商平台。",
                    competitive_advantages="规模采购优势极强，全渠道布局领先，物流网络密集。",
                    risks="利润率压力、劳工成本上升、电商竞争。",
                    pe_ttm=28.0,
                    pb=5.5,
                    roe=20.0,
                    revenue_growth=5.5,
                    net_profit_growth=8.0,
                    gross_margin=24.5,
                    net_margin=2.8,
                    debt_to_asset=58.0,
                    dividend_yield=1.3,
                    operating_cashflow_growth=7.0,
                    free_cashflow_margin=3.5,
                    current_ratio=0.9,
                    quick_ratio=0.3,
                    interest_coverage=10.0,
                    asset_turnover=2.55,
                    market_cap=45_000.0,
                    float_market_cap=14_000.0,
                    last_price=168.0,
                    listing_date=date(1972, 8, 25),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="JNJ.US",
                    symbol="JNJ.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="JNJ.US",
                    symbol="JNJ.US",
                    company_name="Johnson & Johnson",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Health Care",
                    industry="Pharmaceuticals & MedTech",
                    concepts=("创新药", "医疗器械", "高股息"),
                    f10_summary="全球医药与医疗器械巨头，管线丰富，分红连续增长超60年。",
                    main_business="创新药物与医疗器械的研发、生产与销售。",
                    products_services="肿瘤药、免疫药、外科器械、骨科器械。",
                    competitive_advantages="产品线多元化，研发管线深厚，分红历史悠久。",
                    risks="诉讼风险、专利悬崖、医保谈判降价。",
                    pe_ttm=15.5,
                    pb=5.2,
                    roe=35.0,
                    revenue_growth=4.5,
                    net_profit_growth=8.0,
                    gross_margin=68.5,
                    net_margin=22.0,
                    debt_to_asset=48.0,
                    dividend_yield=3.0,
                    operating_cashflow_growth=5.0,
                    free_cashflow_margin=18.0,
                    current_ratio=1.2,
                    quick_ratio=0.9,
                    interest_coverage=18.0,
                    asset_turnover=0.52,
                    market_cap=38_000.0,
                    float_market_cap=37_500.0,
                    last_price=158.0,
                    listing_date=date(1944, 9, 25),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="PG.US",
                    symbol="PG.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="PG.US",
                    symbol="PG.US",
                    company_name="Procter & Gamble",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Consumer Staples",
                    industry="Household Products",
                    concepts=("日用消费品", "品牌", "高股息"),
                    f10_summary="全球日用消费品龙头，品牌矩阵强大，定价能力突出。",
                    main_business="家庭护理、个人护理、织物护理等日用消费品。",
                    products_services="Tide、Pampers、Gillette、SK-II、Oral-B。",
                    competitive_advantages="品牌护城河宽，全球分销网络完善，提价能力强。",
                    risks="原材料成本波动、新兴品牌竞争、汇率波动。",
                    pe_ttm=25.0,
                    pb=7.5,
                    roe=30.0,
                    revenue_growth=3.5,
                    net_profit_growth=5.0,
                    gross_margin=51.0,
                    net_margin=18.5,
                    debt_to_asset=55.0,
                    dividend_yield=2.4,
                    operating_cashflow_growth=4.5,
                    free_cashflow_margin=16.0,
                    current_ratio=0.7,
                    quick_ratio=0.45,
                    interest_coverage=18.0,
                    asset_turnover=0.62,
                    market_cap=38_000.0,
                    float_market_cap=37_500.0,
                    last_price=162.0,
                    listing_date=date(1890, 1, 1),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="MA.US",
                    symbol="MA.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="MA.US",
                    symbol="MA.US",
                    company_name="Mastercard",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Payment Networks",
                    concepts=("支付", "金融科技", "跨境支付"),
                    f10_summary="全球第二大支付网络，跨境交易收入占比高，利润率极高。",
                    main_business="全球支付网络、交易处理与数据分析服务。",
                    products_services="Mastercard支付网络、跨境支付、数据服务。",
                    competitive_advantages="双寡头地位稳固，跨境支付优势大，轻资产高利润。",
                    risks="监管费率压力、数字货币冲击、经济周期。",
                    pe_ttm=34.0,
                    pb=52.0,
                    roe=165.0,
                    revenue_growth=12.0,
                    net_profit_growth=15.0,
                    gross_margin=78.0,
                    net_margin=46.0,
                    debt_to_asset=82.0,
                    dividend_yield=0.6,
                    operating_cashflow_growth=14.0,
                    free_cashflow_margin=42.0,
                    current_ratio=1.2,
                    quick_ratio=1.2,
                    interest_coverage=18.0,
                    asset_turnover=0.72,
                    market_cap=42_000.0,
                    float_market_cap=41_500.0,
                    last_price=450.0,
                    listing_date=date(2006, 5, 25),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="HD.US",
                    symbol="HD.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="HD.US",
                    symbol="HD.US",
                    company_name="Home Depot",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Consumer Discretionary",
                    industry="Home Improvement Retail",
                    concepts=("家装零售", "地产链", "DIY"),
                    f10_summary="全球最大家装零售商，受益住房翻新需求，供应链效率领先。",
                    main_business="家装建材、工具和园艺产品的零售。",
                    products_services="建材、工具、电器、园艺、专业施工服务。",
                    competitive_advantages="门店网络密集，供应链优势强，Pro客户粘性高。",
                    risks="住房市场波动、利率敏感、劳动力成本上升。",
                    pe_ttm=22.0,
                    pb=120.0,
                    roe=500.0,
                    revenue_growth=3.0,
                    net_profit_growth=5.0,
                    gross_margin=33.5,
                    net_margin=10.5,
                    debt_to_asset=98.0,
                    dividend_yield=2.5,
                    operating_cashflow_growth=4.0,
                    free_cashflow_margin=8.5,
                    current_ratio=1.2,
                    quick_ratio=0.4,
                    interest_coverage=12.0,
                    asset_turnover=2.1,
                    market_cap=36_000.0,
                    float_market_cap=35_500.0,
                    last_price=360.0,
                    listing_date=date(1981, 9, 22),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="COST.US",
                    symbol="COST.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="COST.US",
                    symbol="COST.US",
                    company_name="Costco",
                    market_region="US",
                    exchange_code="NASDAQ",
                    board="Main Board",
                    sector="Consumer Staples",
                    industry="Warehouse Retail",
                    concepts=("会员制零售", "消费", "自有品牌"),
                    f10_summary="全球会员制仓储零售龙头，会员续费率极高，Kirkland品牌强势。",
                    main_business="会员制仓储零售与电商。",
                    products_services="会员制批发、Kirkland Signature、Costco电商。",
                    competitive_advantages="会员模式壁垒高，采购规模大，客户忠诚度极强。",
                    risks="会员增长放缓、通胀影响、国际扩张。",
                    pe_ttm=48.0,
                    pb=14.0,
                    roe=28.0,
                    revenue_growth=8.0,
                    net_profit_growth=12.0,
                    gross_margin=12.8,
                    net_margin=2.8,
                    debt_to_asset=52.0,
                    dividend_yield=0.6,
                    operating_cashflow_growth=10.0,
                    free_cashflow_margin=3.5,
                    current_ratio=1.0,
                    quick_ratio=0.5,
                    interest_coverage=15.0,
                    asset_turnover=3.6,
                    market_cap=35_000.0,
                    float_market_cap=34_500.0,
                    last_price=790.0,
                    listing_date=date(1985, 12, 5),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="ABBV.US",
                    symbol="ABBV.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="ABBV.US",
                    symbol="ABBV.US",
                    company_name="AbbVie",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Health Care",
                    industry="Pharmaceuticals",
                    concepts=("创新药", "免疫", "肿瘤", "高股息"),
                    f10_summary="全球制药巨头，免疫和肿瘤领域管线丰富，Humira后周期新品放量。",
                    main_business="创新药物的研发、生产与商业化。",
                    products_services="Skyrizi、Rinvoq、Imbruvica、美容产品线。",
                    competitive_advantages="管线深度和广度行业领先，销售团队执行力强。",
                    risks="Humira仿制药竞争、新药研发风险、定价压力。",
                    pe_ttm=16.0,
                    pb=18.0,
                    roe=65.0,
                    revenue_growth=5.0,
                    net_profit_growth=8.5,
                    gross_margin=70.0,
                    net_margin=18.0,
                    debt_to_asset=82.0,
                    dividend_yield=3.5,
                    operating_cashflow_growth=6.0,
                    free_cashflow_margin=22.0,
                    current_ratio=0.9,
                    quick_ratio=0.8,
                    interest_coverage=6.5,
                    asset_turnover=0.42,
                    market_cap=32_000.0,
                    float_market_cap=31_500.0,
                    last_price=180.0,
                    listing_date=date(2013, 1, 2),
                    currency="USD",
                ),
            ),
            (
                Instrument(
                    instrument_id="BAC.US",
                    symbol="BAC.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="BAC.US",
                    symbol="BAC.US",
                    company_name="Bank of America",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Banking",
                    concepts=("银行", "消费金融", "财富管理"),
                    f10_summary="美国第二大银行，零售银行与财富管理业务规模领先。",
                    main_business="消费银行、财富管理、全球银行与全球市场。",
                    products_services="个人银行、Merrill Lynch、全球交易服务。",
                    competitive_advantages="存款基础庞大，数字银行用户领先，利率敏感性高受益加息。",
                    risks="利率环境变化、信用损失、监管要求。",
                    pe_ttm=11.0,
                    pb=1.2,
                    roe=11.0,
                    revenue_growth=5.5,
                    net_profit_growth=10.0,
                    gross_margin=None,
                    net_margin=28.0,
                    debt_to_asset=89.0,
                    dividend_yield=2.5,
                    operating_cashflow_growth=8.0,
                    free_cashflow_margin=None,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=None,
                    asset_turnover=0.03,
                    market_cap=30_000.0,
                    float_market_cap=29_500.0,
                    last_price=38.0,
                    listing_date=date(1972, 1, 3),
                    currency="USD",
                ),
            ),
            # ── Additional stocks to reach 50 ───────────────────────
            (
                Instrument(
                    instrument_id="600030.SH",
                    symbol="600030.SH",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="CN",
                    lot_size=100,
                    settlement_cycle="T+1",
                    trading_sessions=(("09:30", "11:30"), ("13:00", "15:00")),
                    trading_rules={"board_lot": 100},
                ),
                StockProfile(
                    instrument_id="600030.SH",
                    symbol="600030.SH",
                    company_name="中信证券",
                    market_region="CN",
                    exchange_code="SSE",
                    board="Main Board",
                    sector="Financials",
                    industry="Securities",
                    concepts=("券商", "投行", "财富管理"),
                    f10_summary="国内券商龙头，投行、经纪和资管业务市占率领先。",
                    main_business="证券经纪、投资银行、资产管理与自营投资。",
                    products_services="经纪交易、IPO承销、资管产品、FICC。",
                    competitive_advantages="综合实力行业第一，机构客户基础深厚，国际化布局领先。",
                    risks="市场成交量波动、监管政策变化、自营投资风险。",
                    pe_ttm=15.8,
                    pb=1.5,
                    roe=10.5,
                    revenue_growth=8.0,
                    net_profit_growth=12.0,
                    gross_margin=None,
                    net_margin=32.0,
                    debt_to_asset=78.0,
                    dividend_yield=3.0,
                    operating_cashflow_growth=15.0,
                    free_cashflow_margin=None,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=None,
                    asset_turnover=0.05,
                    market_cap=3_500.0,
                    float_market_cap=3_200.0,
                    last_price=23.5,
                    listing_date=date(2003, 1, 6),
                    currency="CNY",
                ),
            ),
            (
                Instrument(
                    instrument_id="00005.HK",
                    symbol="00005.HK",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="HK",
                    lot_size=400,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "12:00"), ("13:00", "16:00")),
                    trading_rules={"board_lot": 400},
                ),
                StockProfile(
                    instrument_id="00005.HK",
                    symbol="00005.HK",
                    company_name="汇丰控股",
                    market_region="HK",
                    exchange_code="HKEX",
                    board="Main Board",
                    sector="Financials",
                    industry="Banking",
                    concepts=("银行", "高股息", "全球银行"),
                    f10_summary="全球性银行集团，亚洲业务利润占比高，高分红受追捧。",
                    main_business="零售银行、商业银行、全球银行与市场。",
                    products_services="个人银行、商业贷款、贸易融资、财富管理。",
                    competitive_advantages="亚洲网络深厚，贸易融资优势明显，分红吸引力高。",
                    risks="利率环境变化、地缘政治、信用损失。",
                    pe_ttm=8.5,
                    pb=1.0,
                    roe=12.0,
                    revenue_growth=5.0,
                    net_profit_growth=8.0,
                    gross_margin=None,
                    net_margin=28.0,
                    debt_to_asset=92.0,
                    dividend_yield=6.0,
                    operating_cashflow_growth=6.0,
                    free_cashflow_margin=None,
                    current_ratio=None,
                    quick_ratio=None,
                    interest_coverage=None,
                    asset_turnover=0.03,
                    market_cap=12_000.0,
                    float_market_cap=12_000.0,
                    last_price=62.0,
                    listing_date=date(1991, 12, 17),
                    currency="HKD",
                ),
            ),
            (
                Instrument(
                    instrument_id="LLY.US",
                    symbol="LLY.US",
                    market=MarketType.STOCK,
                    instrument_type="equity",
                    market_region="US",
                    lot_size=1,
                    settlement_cycle="T+2",
                    short_sellable=True,
                    trading_sessions=(("09:30", "16:00"),),
                    trading_rules={"allow_extended_hours": True},
                ),
                StockProfile(
                    instrument_id="LLY.US",
                    symbol="LLY.US",
                    company_name="Eli Lilly",
                    market_region="US",
                    exchange_code="NYSE",
                    board="Main Board",
                    sector="Health Care",
                    industry="Pharmaceuticals",
                    concepts=("GLP-1", "减肥药", "创新药"),
                    f10_summary="全球制药巨头，GLP-1减肥药和糖尿病药物驱动强劲增长。",
                    main_business="创新药物的研发、生产与商业化。",
                    products_services="Mounjaro、Zepbound、Verzenio、Jardiance。",
                    competitive_advantages="GLP-1管线领先，产能扩张快，定价能力强。",
                    risks="竞争对手追赶、产能瓶颈、药品定价政策。",
                    pe_ttm=65.0,
                    pb=55.0,
                    roe=85.0,
                    revenue_growth=28.0,
                    net_profit_growth=35.0,
                    gross_margin=80.5,
                    net_margin=22.0,
                    debt_to_asset=72.0,
                    dividend_yield=0.7,
                    operating_cashflow_growth=30.0,
                    free_cashflow_margin=15.0,
                    current_ratio=1.1,
                    quick_ratio=0.85,
                    interest_coverage=18.0,
                    asset_turnover=0.45,
                    market_cap=72_000.0,
                    float_market_cap=71_500.0,
                    last_price=755.0,
                    listing_date=date(1952, 1, 2),
                    currency="USD",
                ),
            ),
        ]


# ─────────────────────────────────────────────────────────────────────────────
# SW-12 / CHART — Advanced Chart Enhancement Service
# Multi-timeframe charts, K-line/line/area mode switching, research presets
# ─────────────────────────────────────────────────────────────────────────────

class ChartType:
    """Supported chart render modes (SW-12 / CHART-01)."""

    CANDLESTICK = "candlestick"
    LINE = "line"
    AREA = "area"
    HEIKIN_ASHI = "heikin_ashi"
    BAR = "bar"


class Timeframe:
    """Supported chart timeframes (SW-12)."""

    S1 = "1s"
    M1 = "1m"
    M3 = "3m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H2 = "2h"
    H4 = "4h"
    H6 = "6h"
    H12 = "12h"
    D1 = "1d"
    W1 = "1w"
    MO1 = "1M"

    ALL = [S1, M1, M3, M5, M15, M30, H1, H2, H4, H6, H12, D1, W1, MO1]


@dataclass(slots=True)
class ChartPreset:
    """Pre-defined research chart configuration (SW-12)."""

    preset_code: str
    preset_name: str
    chart_type: str = ChartType.CANDLESTICK
    timeframe: str = Timeframe.D1
    main_indicators: tuple[str, ...] = ()
    sub_indicators: tuple[str, ...] = ()
    drawing_tools: tuple[str, ...] = ()
    description: str = ""


@dataclass(slots=True)
class DrawingObject:
    """Chart annotation / drawing tool object (CHART-03)."""

    object_id: str
    instrument_id: str
    tool_type: str  # trendline, horizontal_line, fibonacci, text, trade_marker
    points: tuple[dict[str, Any], ...]  # [{"time": ..., "price": ...}, ...]
    style: dict[str, Any] | None = None
    label: str | None = None
    user_id: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class ChartLayout:
    """Multi-pane chart layout configuration (CHART-04)."""

    layout_id: str
    user_id: str
    name: str
    rows: int = 1
    cols: int = 1
    panes: tuple[dict[str, Any], ...] = ()  # [{"instrument_id", "timeframe", "chart_type", "indicators"}]
    linked: bool = True  # crosshair/time linkage between panes
    created_at: str | None = None


class ChartEnhancementService:
    """Enhanced chart service — SW-12 / CHART-01~07.

    Provides:
    - Multi-timeframe chart data (1s … 1M)
    - Chart type switching (K-line, line, area, Heikin-Ashi)
    - Main + sub indicator overlays
    - Drawing tools (trendlines, fibonacci, annotations)
    - Multi-pane layouts with cross-pane linkage
    - Chart snapshots and sharing
    - User-persistent layout and drawing state
    """

    PRESETS: tuple[ChartPreset, ...] = (
        ChartPreset(
            preset_code="default_kline",
            preset_name="默认 K 线",
            chart_type=ChartType.CANDLESTICK,
            timeframe=Timeframe.D1,
            main_indicators=(),
            sub_indicators=("VOL",),
            description="经典日 K 线配合成交量",
        ),
        ChartPreset(
            preset_code="tech_overlay",
            preset_name="技术指标叠加",
            chart_type=ChartType.CANDLESTICK,
            timeframe=Timeframe.D1,
            main_indicators=("MA:5,10,20", "BOLL:20,2"),
            sub_indicators=("MACD:12,26,9", "KDJ:9,3,3"),
            description="均线布林带主图，MACD+KDJ 副图",
        ),
        ChartPreset(
            preset_code="short_term",
            preset_name="短线交易",
            chart_type=ChartType.CANDLESTICK,
            timeframe=Timeframe.M15,
            main_indicators=("MA:5,10",),
            sub_indicators=("VOL", "MACD:12,26,9"),
            description="15 分钟短线均线 + MACD",
        ),
        ChartPreset(
            preset_code="ha_view",
            preset_name="Heikin-Ashi 视图",
            chart_type=ChartType.HEIKIN_ASHI,
            timeframe=Timeframe.H1,
            main_indicators=("MA:20",),
            sub_indicators=("RSI:14",),
            description="Heikin-Ashi 平滑蜡烛图",
        ),
        ChartPreset(
            preset_code="line_area",
            preset_name="折线 / 面积图",
            chart_type=ChartType.AREA,
            timeframe=Timeframe.D1,
            main_indicators=("MA:60", "MA:120"),
            sub_indicators=(),
            description="均线支撑面积图",
        ),
    )

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._layouts: dict[str, ChartLayout] = {}
        self._drawings: dict[str, list[DrawingObject]] = {}
        self._user_presets: dict[str, list[ChartPreset]] = {}

    # ── Chart type & timeframe ────────────────────────────────────────────────

    def supported_chart_types(self) -> list[str]:
        """Return list of supported chart render modes."""
        return [ChartType.CANDLESTICK, ChartType.LINE, ChartType.AREA, ChartType.HEIKIN_ASHI, ChartType.BAR]

    def supported_timeframes(self) -> list[str]:
        """Return list of supported timeframes."""
        return list(Timeframe.ALL)

    def convert_chart_type(self, bars: list[dict[str, Any]], chart_type: str) -> list[dict[str, Any]]:
        """Convert OHLCV bars to the requested render mode (CHART-01)."""
        if chart_type == ChartType.CANDLESTICK or not bars:
            return bars
        if chart_type == ChartType.LINE:
            return [{"time": b["time"], "close": b["close"], "volume": b.get("volume")} for b in bars]
        if chart_type == ChartType.AREA:
            return [{"time": b["time"], "close": b["close"], "volume": b.get("volume")} for b in bars]
        if chart_type == ChartType.HEIKIN_ASHI:
            result = []
            for i, bar in enumerate(bars):
                ha_close = (bar["open"] + bar["high"] + bar["low"] + bar["close"]) / 4
                if i == 0:
                    ha_open = (bar["open"] + bar["close"]) / 2
                else:
                    prev_ha = result[i - 1]
                    ha_open = (prev_ha["open"] + prev_ha["close"]) / 2
                ha_high = max(bar["high"], ha_open, ha_close)
                ha_low = min(bar["low"], ha_open, ha_close)
                result.append({"time": bar["time"], "open": ha_open, "high": ha_high, "low": ha_low, "close": ha_close, "volume": bar.get("volume")})
            return result
        if chart_type == ChartType.BAR:
            return [{"time": b["time"], "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"], "volume": b.get("volume")} for b in bars]
        return bars

    # ── Indicators ───────────────────────────────────────────────────────────

    def calculate_indicator(self, bars: list[dict[str, Any]], indicator: str) -> list[dict[str, Any]]:
        """Calculate indicator values for bars (CHART-02).

        indicator format: "MA:20" or "BOLL:20,2" or "RSI:14"
        """
        if not bars or not indicator:
            return []
        parts = indicator.split(":", 1)
        name = parts[0].upper()
        params = [float(p) for p in parts[1].split(",")] if len(parts) > 1 else []

        closes = [b["close"] for b in bars]

        if name == "MA":
            period = int(params[0]) if params else 20
            return [{"time": b["time"], "value": sum(closes[max(0, i - period + 1):i + 1]) / min(i + 1, period)} for i, b in enumerate(bars)]

        if name == "EMA":
            period = int(params[0]) if params else 20
            multiplier = 2 / (period + 1)
            ema = [closes[0]]
            for i in range(1, len(closes)):
                ema.append((closes[i] - ema[-1]) * multiplier + ema[-1])
            return [{"time": b["time"], "value": ema[i]} for i, b in enumerate(bars)]

        if name == "BOLL":
            period = int(params[0]) if params else 20
            mult = float(params[1]) if len(params) > 1 else 2.0
            result = []
            for i in range(len(bars)):
                window = closes[max(0, i - period + 1):i + 1]
                mean = sum(window) / len(window)
                variance = sum((x - mean) ** 2 for x in window) / len(window)
                std = variance ** 0.5
                mid = mean
                upper = mean + mult * std
                lower = mean - mult * std
                result.append({"time": bars[i]["time"], "mid": mid, "upper": upper, "lower": lower})
            return result

        if name == "RSI":
            period = int(params[0]) if params else 14
            result = []
            gains = []
            for i in range(1, len(closes)):
                delta = closes[i] - closes[i - 1]
                gains.append(delta if delta > 0 else 0)
            if len(gains) < period:
                return [{"time": b["time"], "value": 50.0} for b in bars]
            for i in range(period, len(gains) + 1):
                avg_gain = sum(gains[i - period:i]) / period
                avg_loss = sum(1e-8 + (-g if g < 0 else 0) for g in gains[i - period:i]) / period
                rs = avg_gain / avg_loss
                rsi = 100 - (100 / (1 + rs))
                result.append({"time": bars[i]["time"], "value": rsi})
            return [{"time": bars[j]["time"], "value": 50.0} for j in range(period)] + result

        if name == "MACD":
            fast = int(params[0]) if len(params) > 0 else 12
            slow = int(params[1]) if len(params) > 1 else 26
            signal = int(params[2]) if len(params) > 2 else 9
            ema_fast = self._ema_values(closes, fast)
            ema_slow = self._ema_values(closes, slow)
            macd_line = [ema_fast[i] - ema_slow[i] for i in range(len(closes))]
            signal_line = self._ema_values(macd_line, signal)
            hist = [macd_line[i] - signal_line[i] for i in range(len(macd_line))]
            return [{"time": bars[i]["time"], "macd": macd_line[i], "signal": signal_line[i], "histogram": hist[i]} for i in range(len(bars))]

        if name == "KDJ":
            n = int(params[0]) if params else 9
            m1 = int(params[1]) if len(params) > 1 else 3
            m2 = int(params[2]) if len(params) > 2 else 3
            result = []
            for i in range(len(bars)):
                window = bars[max(0, i - n + 1):i + 1]
                low_min = min(b["low"] for b in window)
                high_max = max(b["high"] for b in window)
                rsv = 100 * (closes[i] - low_min) / (high_max - low_min + 1e-8)
                if i == 0:
                    k, d = 50, 50
                else:
                    k = (m1 - 1) / m1 * result[-1]["k"] + 1 / m1 * rsv
                    d = (m2 - 1) / m2 * result[-1]["d"] + 1 / m2 * k
                j = 3 * k - 2 * d
                result.append({"time": bars[i]["time"], "k": k, "d": d, "j": j})
            return result

        if name == "VOL":
            return [{"time": b["time"], "value": b.get("volume", 0)} for b in bars]

        return []

    def _ema_values(self, data: list[float], period: int) -> list[float]:
        """Helper: compute EMA series."""
        multiplier = 2 / (period + 1)
        ema = [data[0]]
        for i in range(1, len(data)):
            ema.append((data[i] - ema[-1]) * multiplier + ema[-1])
        return ema

    # ── Drawing tools ────────────────────────────────────────────────────────

    def save_drawing(self, drawing: DrawingObject) -> DrawingObject:
        """Persist a chart drawing object (CHART-03)."""
        key = drawing.user_id or "_global"
        if key not in self._drawings:
            self._drawings[key] = []
        existing = [i for i, d in enumerate(self._drawings[key]) if d.object_id == drawing.object_id]
        if existing:
            self._drawings[key][existing[0]] = drawing
        else:
            self._drawings[key].append(drawing)
        if self.persistence is not None:
            self.persistence.upsert_record(
                "chart_drawings", "object_id", drawing.object_id, asdict(drawing)
            )
        return drawing

    def get_drawings(self, instrument_id: str, user_id: str | None = None) -> list[DrawingObject]:
        """Return all drawings for an instrument (CHART-07)."""
        key = user_id or "_global"
        return [d for d in self._drawings.get(key, []) if d.instrument_id == instrument_id]

    def delete_drawing(self, object_id: str, user_id: str | None = None) -> bool:
        """Delete a drawing by ID."""
        key = user_id or "_global"
        if key in self._drawings:
            self._drawings[key] = [d for d in self._drawings[key] if d.object_id != object_id]
        return True

    # ── Layout management ────────────────────────────────────────────────────

    def create_layout(self, layout: ChartLayout) -> ChartLayout:
        """Create or update a chart layout (CHART-04)."""
        self._layouts[layout.layout_id] = layout
        if self.persistence is not None:
            self.persistence.upsert_record("chart_layouts", "layout_id", layout.layout_id, asdict(layout))
        return layout

    def get_layout(self, layout_id: str) -> ChartLayout | None:
        """Retrieve a saved layout."""
        return self._layouts.get(layout_id)

    def list_layouts(self, user_id: str) -> list[ChartLayout]:
        """List all layouts for a user."""
        return [l for l in self._layouts.values() if l.user_id == user_id]

    def delete_layout(self, layout_id: str) -> bool:
        """Delete a layout."""
        if layout_id in self._layouts:
            del self._layouts[layout_id]
        return True

    # ── Presets ──────────────────────────────────────────────────────────────

    def list_presets(self) -> list[dict[str, Any]]:
        """Return all built-in chart presets."""
        return [asdict(p) for p in self.PRESETS]

    def save_user_preset(self, user_id: str, preset: ChartPreset) -> ChartPreset:
        """Save a custom user preset."""
        if user_id not in self._user_presets:
            self._user_presets[user_id] = []
        existing = [i for i, p in enumerate(self._user_presets[user_id]) if p.preset_code == preset.preset_code]
        if existing:
            self._user_presets[user_id][existing[0]] = preset
        else:
            self._user_presets[user_id].append(preset)
        return preset

    def get_user_presets(self, user_id: str) -> list[ChartPreset]:
        """Return user custom presets."""
        return self._user_presets.get(user_id, [])

    # ── Snapshot ─────────────────────────────────────────────────────────────

    def create_snapshot(self, instrument_id: str, layout_id: str, user_id: str | None = None) -> dict[str, Any]:
        """Create a chart snapshot record for sharing (CHART-05)."""
        import hashlib, uuid
        snapshot_id = hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:16]
        return {
            "snapshot_id": snapshot_id,
            "instrument_id": instrument_id,
            "layout_id": layout_id,
            "user_id": user_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    # ── CHART-06: TPO / Market Profile ──────────────────────────────────────

    def calculate_tpo(self, bars: list[dict[str, Any]], period: int = 30) -> list[dict[str, Any]]:
        """Calculate Time Price Opportunity (Market Profile) for bars (CHART-06).

        Returns a list of TPO buckets with price levels and TPO count.
        """
        if not bars or period <= 0:
            return []

        # Get price range
        highs = [b["high"] for b in bars]
        lows = [b["low"] for b in bars]
        all_prices = sorted(set(round(p, 2) for p in highs + lows))

        if not all_prices:
            return []

        # Build price buckets
        tick_size = 0.01
        price_min = min(all_prices)
        price_max = max(all_prices)
        num_buckets = max(20, int((price_max - price_min) / tick_size) + 1)

        tpo_result = []
        for i in range(min(num_buckets, 100)):
            price_level = round(price_min + i * tick_size, 2)
            tpo_count = 0
            for bar in bars:
                if bar["low"] <= price_level <= bar["high"]:
                    tpo_count += 1
            tpo_result.append({
                "price": price_level,
                "tpo_count": tpo_count,
                "percentage": round(tpo_count / len(bars) * 100, 1) if bars else 0,
            })

        # Identify Value Areas (70%)
        total_tpo = sum(r["tpo_count"] for r in tpo_result)
        if total_tpo == 0:
            return tpo_result

        sorted_by_tpo = sorted(tpo_result, key=lambda x: x["tpo_count"], reverse=True)
        cumsum = 0
        value_area_prices = set()
        for r in sorted_by_tpo:
            if cumsum / total_tpo >= 0.70:
                break
            cumsum += r["tpo_count"]
            value_area_prices.add(r["price"])

        for r in tpo_result:
            r["in_value_area"] = r["price"] in value_area_prices

        # POC (Point of Control)
        if sorted_by_tpo:
            for r in tpo_result:
                r["is_poc"] = r["price"] == sorted_by_tpo[0]["price"]

        return tpo_result

    def calculate_footprint(
        self, bars: list[dict[str, Any]], num_levels: int = 24
    ) -> list[dict[str, Any]]:
        """Calculate candle footprint (volume at each price level) (CHART-06).

        Returns per-bar footprint with bid/ask volume imbalance.
        """
        if not bars:
            return []

        # Build price ladder
        all_prices = sorted(set(round(p, 2) for b in bars for p in (b["high"], b["low"], b.get("open", b["close"]), b["close"])))
        if not all_prices:
            return []

        tick_size = 0.01
        price_min = min(all_prices)
        price_max = max(all_prices)
        num_buckets = min(num_levels, max(10, int((price_max - price_min) / tick_size) + 1))

        footprints = []
        for bar in bars:
            fp = {
                "time": bar.get("time"),
                "open": bar.get("open"),
                "high": bar.get("high"),
                "low": bar.get("low"),
                "close": bar.get("close"),
                "volume": bar.get("volume", 0),
                "levels": [],
            }

            price_range = round((bar["high"] - bar["low"]) / tick_size)
            if price_range <= 0:
                price_range = 1

            step = max(1, price_range // num_buckets)
            bucket_prices = []
            p = bar["low"]
            while p <= bar["high"]:
                bucket_prices.append(round(p, 2))
                p += step * tick_size

            # Simplified: distribute volume proportionally across the range
            total_volume = bar.get("volume", 0)
            for price in bucket_prices[:num_buckets]:
                fp["levels"].append({
                    "price": price,
                    "volume": round(total_volume / len(bucket_prices[:num_buckets]), 0) if bucket_prices else 0,
                })

            # Calculate bid/ask imbalance (simplified)
            close = bar["close"]
            open_ = bar.get("open", close)
            if close > open_:
                fp["imbalance"] = "bid"
                fp["imbalance_ratio"] = round((close - open_) / open_ * 100, 2) if open_ else 0
            elif close < open_:
                fp["imbalance"] = "ask"
                fp["imbalance_ratio"] = round((open_ - close) / close * 100, 2) if close else 0
            else:
                fp["imbalance"] = "neutral"
                fp["imbalance_ratio"] = 0

            footprints.append(fp)

        return footprints

    def calculate_depth_heatmap(
        self, order_book: dict[str, Any], levels: int = 24
    ) -> dict[str, Any]:
        """Calculate depth heatmap from order book data (CHART-06).

        Returns heatmap structure with price levels and cumulative volume.
        """
        bids = order_book.get("bids", [])
        asks = order_book.get("asks", [])

        if not bids or not asks:
            return {"bid_levels": [], "ask_levels": [], "mid_price": None, "spread": None}

        # Sort and limit
        sorted_bids = sorted(bids, key=lambda x: float(x[0]), reverse=True)[:levels]
        sorted_asks = sorted(asks, key=lambda x: float(x[0]))[:levels]

        bid_levels = []
        cum_bid = 0
        for price, volume in sorted_bids:
            cum_bid += float(volume)
            bid_levels.append({
                "price": float(price),
                "volume": float(volume),
                "cumulative_volume": cum_bid,
            })

        ask_levels = []
        cum_ask = 0
        for price, volume in sorted_asks:
            cum_ask += float(volume)
            ask_levels.append({
                "price": float(price),
                "volume": float(volume),
                "cumulative_volume": cum_ask,
            })

        mid_price = (float(bids[0][0]) + float(asks[0][0])) / 2
        spread = float(asks[0][0]) - float(bids[0][0])

        # Calculate heat intensity (0-100)
        max_vol = max(
            max((l["volume"] for l in bid_levels), default=1),
            max((l["volume"] for l in ask_levels), default=1),
        )

        for level in bid_levels:
            level["heat"] = round(level["volume"] / max_vol * 100, 1)
        for level in ask_levels:
            level["heat"] = round(level["volume"] / max_vol * 100, 1)

        return {
            "bid_levels": bid_levels,
            "ask_levels": ask_levels,
            "mid_price": mid_price,
            "spread": spread,
            "spread_bps": round(spread / mid_price * 10000, 2) if mid_price else 0,
        }


# ─────────────────────────────────────────────────────────────────────────────
# SW-13 — Tab-based Stock Screener Workbench
# Watchlist grouping, tab state management, page structure organization
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class WatchlistGroup:
    """Watchlist group within a tab (SW-13)."""

    group_id: str
    name: str
    instrument_ids: tuple[str, ...] = ()
    color: str = "#3B82F6"


@dataclass(slots=True)
class WorkbenchTab:
    """Single tab within the stock screener workbench (SW-13)."""

    tab_id: str
    user_id: str
    name: str
    tab_type: str = "screener"  # screener | chart | comparison | f10
    active_instrument_id: str | None = None
    screener_filters: dict[str, Any] | None = None
    chart_config: dict[str, Any] | None = None
    watchlist_groups: tuple[WatchlistGroup, ...] = ()
    sort_key: str = "symbol"
    sort_desc: bool = False
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(slots=True)
class WorkbenchState:
    """Complete workbench session state for one user (SW-13)."""

    user_id: str
    active_tab_id: str
    tabs: tuple[WorkbenchTab, ...] = ()
    recent_instrument_ids: tuple[str, ...] = ()
    comparison_left: str | None = None
    comparison_right: str | None = None


class StockScreenerWorkbench:
    """Tab-based stock screener workbench service (SW-13).

    Provides:
    - Multi-tab management (create, switch, close, reorder)
    - Tab types: screener, chart, comparison, F10 detail
    - Watchlist grouping within tabs
    - Tab state persistence and cross-session restore
    - Recent instrument tracking
    - Comparison slot management
    """

    DEFAULT_TAB_TYPES = ("screener", "chart", "comparison", "f10")

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._states: dict[str, WorkbenchState] = {}

    def get_or_create_state(self, user_id: str) -> WorkbenchState:
        """Get existing workbench state or create a fresh one."""
        if user_id in self._states:
            return self._states[user_id]
        default_tab = WorkbenchTab(
            tab_id=f"tab_default_{user_id}",
            user_id=user_id,
            name="自选",
            tab_type="screener",
            watchlist_groups=(
                WatchlistGroup(group_id=f"wl_default_{user_id}", name="默认分组"),
            ),
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        state = WorkbenchState(
            user_id=user_id,
            active_tab_id=default_tab.tab_id,
            tabs=(default_tab,),
            recent_instrument_ids=(),
        )
        self._states[user_id] = state
        self._persist_state(state)
        return state

    def create_tab(self, user_id: str, name: str, tab_type: str = "screener") -> WorkbenchTab:
        """Create a new tab in the workbench."""
        state = self.get_or_create_state(user_id)
        import uuid
        tab = WorkbenchTab(
            tab_id=f"tab_{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            name=name,
            tab_type=tab_type,
            created_at=datetime.now(timezone.utc).isoformat(),
        )
        state._replace(
            tabs=state.tabs + (tab,),
            active_tab_id=tab.tab_id,
        )
        self._persist_state(state)
        return tab

    def close_tab(self, user_id: str, tab_id: str) -> bool:
        """Close a tab; if it was active, switch to the first remaining tab."""
        state = self.get_or_create_state(user_id)
        remaining = [t for t in state.tabs if t.tab_id != tab_id]
        if not remaining:
            return False
        new_tabs = tuple(remaining)
        new_active = state.active_tab_id
        if state.active_tab_id == tab_id:
            new_active = remaining[0].tab_id
        state._replace(tabs=new_tabs, active_tab_id=new_active)
        self._persist_state(state)
        return True

    def switch_tab(self, user_id: str, tab_id: str) -> bool:
        """Switch the active tab."""
        state = self.get_or_create_state(user_id)
        if not any(t.tab_id == tab_id for t in state.tabs):
            return False
        state._replace(active_tab_id=tab_id)
        self._persist_state(state)
        return True

    def update_tab(
        self,
        user_id: str,
        tab_id: str,
        *,
        name: str | None = None,
        active_instrument_id: str | None = None,
        screener_filters: dict[str, Any] | None = None,
        chart_config: dict[str, Any] | None = None,
        watchlist_groups: tuple[WatchlistGroup, ...] | None = None,
        sort_key: str | None = None,
        sort_desc: bool | None = None,
    ) -> WorkbenchTab | None:
        """Update tab configuration."""
        state = self.get_or_create_state(user_id)
        for i, tab in enumerate(state.tabs):
            if tab.tab_id == tab_id:
                updated = replace(tab,
                    name=name if name is not None else tab.name,
                    active_instrument_id=active_instrument_id if active_instrument_id is not None else tab.active_instrument_id,
                    screener_filters=screener_filters if screener_filters is not None else tab.screener_filters,
                    chart_config=chart_config if chart_config is not None else tab.chart_config,
                    watchlist_groups=watchlist_groups if watchlist_groups is not None else tab.watchlist_groups,
                    sort_key=sort_key if sort_key is not None else tab.sort_key,
                    sort_desc=sort_desc if sort_desc is not None else tab.sort_desc,
                    updated_at=datetime.now(timezone.utc).isoformat(),
                )
                tabs = list(state.tabs)
                tabs[i] = updated
                state._replace(tabs=tuple(tabs))
                self._persist_state(state)
                return updated
        return None

    def add_to_watchlist(self, user_id: str, tab_id: str, instrument_id: str, group_id: str | None = None) -> bool:
        """Add an instrument to a watchlist group within a tab."""
        state = self.get_or_create_state(user_id)
        for i, tab in enumerate(state.tabs):
            if tab.tab_id == tab_id:
                groups = list(tab.watchlist_groups)
                target_group_id = group_id or (groups[0].group_id if groups else None)
                for j, group in enumerate(groups):
                    if group.group_id == target_group_id:
                        if instrument_id not in group.instrument_ids:
                            groups[j] = replace(group, instrument_ids=group.instrument_ids + (instrument_id,))
                        break
                else:
                    groups.append(WatchlistGroup(group_id=f"wl_{uuid.uuid4().hex[:8]}", name="新分组", instrument_ids=(instrument_id,)))
                tabs = list(state.tabs)
                tabs[i] = replace(tab, watchlist_groups=tuple(groups), updated_at=datetime.now(timezone.utc).isoformat())
                state._replace(tabs=tuple(tabs))
                self._persist_state(state)
                return True
        return False

    def remove_from_watchlist(self, user_id: str, tab_id: str, instrument_id: str) -> bool:
        """Remove an instrument from its watchlist group."""
        state = self.get_or_create_state(user_id)
        for i, tab in enumerate(state.tabs):
            if tab.tab_id == tab_id:
                groups = []
                for group in tab.watchlist_groups:
                    if instrument_id in group.instrument_ids:
                        groups.append(replace(group, instrument_ids=tuple(pid for pid in group.instrument_ids if pid != instrument_id)))
                    else:
                        groups.append(group)
                tabs = list(state.tabs)
                tabs[i] = replace(tab, watchlist_groups=tuple(groups), updated_at=datetime.now(timezone.utc).isoformat())
                state._replace(tabs=tuple(tabs))
                self._persist_state(state)
                return True
        return False

    def set_comparison(self, user_id: str, left: str | None = None, right: str | None = None) -> dict[str, Any]:
        """Set comparison instrument slots."""
        state = self.get_or_create_state(user_id)
        state._replace(comparison_left=left or state.comparison_left, comparison_right=right or state.comparison_right)
        self._persist_state(state)
        return {"left": state.comparison_left, "right": state.comparison_right}

    def add_recent(self, user_id: str, instrument_id: str) -> None:
        """Track recently viewed instrument."""
        state = self.get_or_create_state(user_id)
        recent = list(state.recent_instrument_ids)
        if instrument_id in recent:
            recent.remove(instrument_id)
        recent.insert(0, instrument_id)
        state._replace(recent_instrument_ids=tuple(recent[:50]))
        self._persist_state(state)

    def _persist_state(self, state: WorkbenchState) -> None:
        """Persist workbench state to storage."""
        self._states[state.user_id] = state
        if self.persistence is not None:
            self.persistence.upsert_record(
                "sw_workbench_state", "user_id", state.user_id, asdict(state)
            )


# ─────────────────────────────────────────────────────────────────────────────
# SW-14 — Smart Stock Selector
# Natural language Q&A stock selection, pattern recognition, joint scanner
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class PatternResult:
    """Technical pattern detection result (SW-14)."""

    instrument_id: str
    pattern_type: str  # head_shoulders, double_top, double_bottom, triple_top, triple_bottom, wedge, channel
    direction: str  # bullish, bearish, neutral
    confidence: float  # 0.0 – 1.0
    key_points: dict[str, Any] | None = None
    description: str = ""


@dataclass(slots=True)
class ScanResult:
    """Advanced scanner result entry (SW-14)."""

    instrument_id: str
    score: float
    matched_criteria: tuple[str, ...] = ()
    price: float | None = None
    change_pct: float | None = None
    volume: float | None = None
    market_cap: float | None = None


class TechnicalPattern:
    """Technical chart pattern definitions (SW-14)."""

    HEAD_SHOULDERS = "head_shoulders"
    DOUBLE_TOP = "double_top"
    DOUBLE_BOTTOM = "double_bottom"
    TRIPLE_TOP = "triple_top"
    TRIPLE_BOTTOM = "triple_bottom"
    WEDGE = "wedge"
    CHANNEL = "channel"
    ALL = (HEAD_SHOULDERS, DOUBLE_TOP, DOUBLE_BOTTOM, TRIPLE_TOP, TRIPLE_BOTTOM, WEDGE, CHANNEL)


class SmartStockSelector:
    """AI-augmented stock selection service (SW-14).

    Provides:
    - Natural language Q&A stock screening (keyword/intent extraction)
    - Technical pattern recognition on price series
    - Joint scanner combining financial + technical criteria
    """

    # Keywords for natural language intent mapping
    BULLISH_KEYWORDS = frozenset({"上涨", "看涨", "多头", "买入", "buy", "bullish", "long", "强势", "突破"})
    BEARISH_KEYWORDS = frozenset({"下跌", "看跌", "空头", "卖出", "sell", "bearish", "short", "弱势", "破位"})
    VALUE_KEYWORDS = frozenset({"低估", "价值", "pe", "pb", "便宜", "value", "valuation", "低估值"})
    GROWTH_KEYWORDS = frozenset({"成长", "增长", "增速", "growth", "高增长", "扩张"})
    DIVIDEND_KEYWORDS = frozenset({"股息", "分红", "dividend", "高股息", " yield"})
    LOW_VOL_KEYWORDS = frozenset({"低波动", "稳健", "稳定", "low volatility", "stable"})
    LARGE_CAP_KEYWORDS = frozenset({"大盘", "蓝筹", "large cap", "big", "巨型"})
    SMALL_CAP_KEYWORDS = frozenset({"小盘", "成长", "small cap", "micro"})

    def __init__(self, stock_service: StockDirectoryService | None = None) -> None:
        self.stock_service = stock_service

    def parse_natural_query(self, query: str) -> dict[str, Any]:
        """Parse a natural language query into structured filter criteria (SW-14).

        Returns a filter dict suitable for StockDirectoryService.list_stocks().
        """
        q = query.lower().strip()

        filters: dict[str, Any] = {}

        # Sentiment / direction
        if any(k in q for k in self.BULLISH_KEYWORDS):
            filters["direction"] = "bullish"
        elif any(k in q for k in self.BEARISH_KEYWORDS):
            filters["direction"] = "bearish"

        # Market cap
        if any(k in q for k in self.LARGE_CAP_KEYWORDS):
            filters["min_market_cap"] = 100_000_000_000
        elif any(k in q for k in self.SMALL_CAP_KEYWORDS):
            filters["max_market_cap"] = 10_000_000_000

        # Valuation
        if any(k in q for k in self.VALUE_KEYWORDS):
            filters["max_pe_ttm"] = 20
            filters["max_pb"] = 3

        # Growth
        if any(k in q for k in self.GROWTH_KEYWORDS):
            filters["min_revenue_growth"] = 10
            filters["min_net_profit_growth"] = 10

        # Dividend
        if any(k in q for k in self.DIVIDEND_KEYWORDS):
            filters["min_dividend_yield"] = 2.0

        # Low volatility / stability
        if any(k in q for k in self.LOW_VOL_KEYWORDS):
            filters["max_debt_to_asset"] = 50

        # Market region
        if "美股" in q or "us" in q or "nasdaq" in q or "nyse" in q:
            filters["market_region"] = "US"
        elif "港股" in q or "hk" in q or "hong kong" in q:
            filters["market_region"] = "HK"
        elif "a股" in q or "a share" in q or "沪" in q or "深" in q:
            filters["market_region"] = "CN"

        # Sector / industry
        for sector in ("Technology", "Financials", "Consumer Staples", "Health Care", "Energy", "Industrials"):
            if sector.lower() in q:
                filters["sector"] = sector
                break

        # Numeric filters
        import re
        pe_match = re.search(r"pe\s*[<≤]\s*(\d+\.?\d*)", q)
        if pe_match:
            filters["max_pe_ttm"] = float(pe_match.group(1))
        pb_match = re.search(r"pb\s*[<≤]\s*(\d+\.?\d*)", q)
        if pb_match:
            filters["max_pb"] = float(pb_match.group(1))
        roe_match = re.search(r"roe\s*[>≥]\s*(\d+\.?\d*)", q)
        if roe_match:
            filters["min_roe"] = float(roe_match.group(1))
        mcap_match = re.search(r"市值\s*[>≥]\s*(\d+\.?\d*)\s*亿", q)
        if mcap_match:
            filters["min_market_cap"] = float(mcap_match.group(1)) * 100_000_000

        return filters

    def screen_natural_query(self, query: str) -> list[dict[str, Any]]:
        """Execute a natural language stock screening query (SW-14)."""
        if self.stock_service is None:
            return []
        filters = self.parse_natural_query(query)
        return self.stock_service.list_stocks(filters)

    def detect_pattern(self, bars: list[dict[str, Any]], pattern_type: str) -> PatternResult | None:
        """Detect a technical pattern in OHLCV bars (SW-14)."""
        if len(bars) < 30:
            return None

        highs = [b["high"] for b in bars]
        lows = [b["low"] for b in bars]
        closes = [b["close"] for b in bars]

        if pattern_type == TechnicalPattern.DOUBLE_TOP:
            return self._detect_double_top(highs, lows, closes)
        if pattern_type == TechnicalPattern.DOUBLE_BOTTOM:
            return self._detect_double_bottom(highs, lows, closes)
        if pattern_type == TechnicalPattern.HEAD_SHOULDERS:
            return self._detect_head_shoulders(highs, lows, closes)
        if pattern_type == TechnicalPattern.CHANNEL:
            return self._detect_channel(highs, lows)
        if pattern_type == TechnicalPattern.WEDGE:
            return self._detect_wedge(highs, lows)

        return None

    def _detect_double_top(self, highs: list[float], lows: list[float], closes: list[float]) -> PatternResult | None:
        """Detect double top pattern."""
        n = len(highs)
        window = highs[max(0, n - 60):n]
        peak_idx = window.index(max(window))
        peak_val = window[peak_idx]

        # Find second peak within 20 bars of first
        second_search = window[peak_idx + 5:]
        if not second_search:
            return None
        second_idx = second_search.index(max(second_search)) + peak_idx + 5 if any(second_search) else -1

        if second_idx <= 0 or second_idx >= n - 5:
            return None
        if abs(window[peak_idx] - window[second_idx - max(0, n - 60)]) / window[peak_idx] > 0.03:
            return None  # peaks not similar

        # Check neckline break
        mid_low_idx = (peak_idx + second_idx - max(0, n - 60)) // 2 + max(0, n - 60)
        neckline = min(lows[max(0, n - 40):n - 10]) if n > 40 else min(lows)
        last_close = closes[-1]

        if last_close < neckline:
            return PatternResult(
                instrument_id="",
                pattern_type=TechnicalPattern.DOUBLE_TOP,
                direction="bearish",
                confidence=0.72,
                description="双顶形态，颈线破位，看跌信号",
            )
        return PatternResult(
            instrument_id="",
            pattern_type=TechnicalPattern.DOUBLE_TOP,
            direction="neutral",
            confidence=0.55,
            description="双顶形态形成中",
        )

    def _detect_double_bottom(self, highs: list[float], lows: list[float], closes: list[float]) -> PatternResult | None:
        """Detect double bottom pattern."""
        n = len(lows)
        window = lows[max(0, n - 60):n]
        trough_idx = window.index(min(window))
        trough_val = window[trough_idx]

        second_search = window[trough_idx + 5:]
        if not second_search:
            return None
        second_idx = second_search.index(min(second_search)) + trough_idx + 5 if any(second_search) else -1

        if second_idx <= 0 or second_idx >= n - 5:
            return None
        if abs(window[trough_idx] - window[second_idx - max(0, n - 60)]) / window[trough_idx] > 0.03:
            return None

        neckline = max(highs[max(0, n - 40):n - 10]) if n > 40 else max(highs)
        last_close = closes[-1]

        if last_close > neckline:
            return PatternResult(
                instrument_id="",
                pattern_type=TechnicalPattern.DOUBLE_BOTTOM,
                direction="bullish",
                confidence=0.75,
                description="双底形态，颈线突破，看涨信号",
            )
        return PatternResult(
            instrument_id="",
            pattern_type=TechnicalPattern.DOUBLE_BOTTOM,
            direction="neutral",
            confidence=0.58,
            description="双底形态形成中",
        )

    def _detect_head_shoulders(self, highs: list[float], lows: list[float], closes: list[float]) -> PatternResult | None:
        """Detect head and shoulders pattern (simplified)."""
        n = len(highs)
        if n < 60:
            return None

        recent = highs[n - 60:]
        left_shoulder = max(recent[:20])
        head = max(recent[20:40])
        right_shoulder = max(recent[40:])

        # Head must be highest
        if head <= left_shoulder or head <= right_shoulder:
            return PatternResult(
                instrument_id="",
                pattern_type=TechnicalPattern.HEAD_SHOULDERS,
                direction="neutral",
                confidence=0.40,
                description="未形成标准头肩顶形态",
            )

        # Shoulders roughly symmetric
        if abs(left_shoulder - right_shoulder) / left_shoulder > 0.05:
            return PatternResult(
                instrument_id="",
                pattern_type=TechnicalPattern.HEAD_SHOULDERS,
                direction="neutral",
                confidence=0.45,
                description="肩部不对称，形态需观察",
            )

        neckline = min(lows[n - 60:n - 10]) if n > 60 else min(lows)
        if closes[-1] < neckline:
            return PatternResult(
                instrument_id="",
                pattern_type=TechnicalPattern.HEAD_SHOULDERS,
                direction="bearish",
                confidence=0.78,
                description="头肩顶形态，颈线破位，看跌信号",
            )

        return PatternResult(
            instrument_id="",
            pattern_type=TechnicalPattern.HEAD_SHOULDERS,
            direction="neutral",
            confidence=0.65,
            description="头肩顶形态形成中",
        )

    def _detect_channel(self, highs: list[float], lows: list[float]) -> PatternResult | None:
        """Detect price channel (parallel resistance/support)."""
        n = len(highs)
        if n < 20:
            return None

        recent_highs = highs[n - 20:]
        recent_lows = lows[n - 20:]

        high_trend = (recent_highs[-1] - recent_highs[0]) / recent_highs[0]
        low_trend = (recent_lows[-1] - recent_lows[0]) / recent_lows[0]

        if abs(high_trend - low_trend) < 0.01:
            return PatternResult(
                instrument_id="",
                pattern_type=TechnicalPattern.CHANNEL,
                direction="neutral",
                confidence=0.70,
                description="平行通道整理中",
            )

        direction = "bullish" if high_trend > 0 and low_trend > 0 else "bearish" if high_trend < 0 and low_trend < 0 else "neutral"
        return PatternResult(
            instrument_id="",
            pattern_type=TechnicalPattern.CHANNEL,
            direction=direction,
            confidence=0.62,
            description=f"{'上升' if direction == 'bullish' else '下降' if direction == 'bearish' else '横向'}通道",
        )

    def _detect_wedge(self, highs: list[float], lows: list[float]) -> PatternResult | None:
        """Detect wedge pattern (converging triangles)."""
        n = len(highs)
        if n < 30:
            return None

        recent_highs = highs[n - 30:]
        recent_lows = lows[n - 30:]

        high_slope = (recent_highs[-1] - recent_highs[0]) / len(recent_highs)
        low_slope = (recent_lows[-1] - recent_lows[0]) / len(recent_lows)

        # Wedge: slopes converge but same direction
        if high_slope * low_slope > 0 and abs(high_slope - low_slope) < abs(high_slope) * 0.5:
            direction = "bullish" if high_slope < 0 else "bearish"
            return PatternResult(
                instrument_id="",
                pattern_type=TechnicalPattern.WEDGE,
                direction=direction,
                confidence=0.68,
                description=f"{'下降' if direction == 'bullish' else '上升'}楔形，预示{'向上' if direction == 'bullish' else '向下'}突破",
            )

        return PatternResult(
            instrument_id="",
            pattern_type=TechnicalPattern.WEDGE,
            direction="neutral",
            confidence=0.40,
            description="未形成标准楔形",
        )

    def scan_patterns(self, instrument_ids: list[str], pattern_type: str, stock_service: StockDirectoryService) -> list[PatternResult]:
        """Scan multiple instruments for a specific pattern (SW-14)."""
        results = []
        for iid in instrument_ids:
            try:
                history = stock_service.get_stock_history(iid, limit=120)
                bars = history.get("bars", [])
                if not bars:
                    continue
                result = self.detect_pattern(bars, pattern_type)
                if result and result.direction != "neutral":
                    result = replace(result, instrument_id=iid)
                    results.append(result)
            except Exception:
                continue
        return results

    def joint_scan(
        self,
        financial_filters: dict[str, Any],
        technical_pattern: str | None = None,
        stock_service: StockDirectoryService | None = None,
    ) -> list[ScanResult]:
        """Combined financial + technical scanner (SW-14)."""
        if stock_service is None:
            return []

        candidates = stock_service.list_stocks(financial_filters)
        results: list[ScanResult] = []

        for candidate in candidates:
            iid = candidate["instrument_id"]
            score = 50.0
            matched = list(financial_filters.keys())

            # Technical confirmation
            if technical_pattern:
                try:
                    history = stock_service.get_stock_history(iid, limit=60)
                    bars = history.get("bars", [])
                    pattern_result = self.detect_pattern(bars, technical_pattern)
                    if pattern_result and pattern_result.direction == "bullish":
                        score += 20
                        matched.append(f"pattern:{technical_pattern}_bullish")
                    elif pattern_result and pattern_result.direction == "bearish":
                        score -= 10
                        matched.append(f"pattern:{technical_pattern}_bearish")
                except Exception:
                    pass

            # Price momentum bonus
            if candidate.get("change_pct") and candidate["change_pct"] > 3:
                score += 10
                matched.append("momentum_green")
            elif candidate.get("change_pct") and candidate["change_pct"] < -3:
                score -= 10
                matched.append("momentum_red")

            results.append(ScanResult(
                instrument_id=iid,
                score=score,
                matched_criteria=tuple(matched),
                price=candidate.get("last_price"),
                change_pct=candidate.get("change_pct"),
                volume=candidate.get("volume"),
                market_cap=candidate.get("market_cap"),
            ))

        results.sort(key=lambda x: x.score, reverse=True)
        return results

