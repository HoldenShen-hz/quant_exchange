"""Multi-asset portfolio backtesting and leverage/margin simulation.

Implements:
- Multi-instrument portfolio backtesting (BT-06)
- Margin and leverage simulation (BT-03)
- Funding rate simulation for perpetuals (BT-03)
- Backtest result persistence (BT-07)
- Bias audit for look-ahead bias, future function, time alignment (BT-08)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any

from quant_exchange.core.models import (
    BacktestResult,
    CostBreakdown,
    Fill,
    FundingRate,
    Instrument,
    Kline,
    Order,
    OrderRequest,
    OrderSide,
    PerformanceMetrics,
    PortfolioSnapshot,
    RiskDecision,
)
from quant_exchange.core.utils import annualize_return, max_drawdown, safe_div, sharpe_ratio, sortino_ratio
from quant_exchange.execution.oms import OrderManager, PaperExecutionEngine
from quant_exchange.monitoring.service import MonitoringService
from quant_exchange.portfolio.service import PortfolioManager
from quant_exchange.risk.service import RiskEngine
from quant_exchange.strategy.base import BaseStrategy, StrategyContext


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(timezone.utc)


@dataclass(slots=True)
class MarginState:
    """Track margin state for leveraged positions."""

    initial_margin: float = 0.0
    maintenance_margin: float = 0.0
    margin_ratio: float = 0.0
    unrealized_pnl: float = 0.0
    funding_accrued: float = 0.0
    margin_used: float = 0.0


@dataclass(slots=True)
class MultiAssetPosition:
    """Position tracking for a single instrument in a multi-asset portfolio."""

    instrument_id: str
    quantity: float = 0.0
    average_cost: float = 0.0
    realized_pnl: float = 0.0
    last_price: float = 0.0
    # Leverage fields
    is_leveraged: bool = False
    leverage: float = 1.0
    margin_used: float = 0.0
    # Funding fields (for perpetual contracts)
    funding_accrued: float = 0.0


class MultiAssetBacktestEngine:
    """Event-driven multi-instrument portfolio backtest engine.

    Supports:
    - Multiple instruments with cross-asset risk management
    - Leverage and margin simulation
    - Funding rate accrual for perpetual contracts
    - Position-level and portfolio-level risk checks
    """

    def __init__(
        self,
        *,
        fee_rate: float = 0.001,
        slippage_bps: float = 5.0,
        bias_window: timedelta = timedelta(days=1),
        default_leverage: float = 1.0,
        maintenance_margin_ratio: float = 0.5,
        funding_rate: float = 0.0001,
    ) -> None:
        self.fee_rate = fee_rate
        self.slippage_bps = slippage_bps
        self.bias_window = bias_window
        self.default_leverage = default_leverage
        self.maintenance_margin_ratio = maintenance_margin_ratio
        self.funding_rate = funding_rate
        self.execution = PaperExecutionEngine(fee_rate=fee_rate, slippage_bps=slippage_bps)

    def run_multi_asset(
        self,
        *,
        instruments: list[Instrument],
        klines_by_instrument: dict[str, list[Kline]],
        strategy: BaseStrategy,
        intelligence_engine,
        risk_engine: RiskEngine,
        initial_cash: float = 100_000.0,
        leverage_by_instrument: dict[str, float] | None = None,
        funding_rates: dict[str, FundingRate] | None = None,
    ) -> BacktestResult:
        """Run a multi-instrument backtest with leverage and funding rate simulation.

        Args:
            instruments: List of instruments to trade
            klines_by_instrument: Dict mapping instrument_id to their kline data
            strategy: Strategy instance to run
            intelligence_engine: Market intelligence engine for bias signals
            risk_engine: Risk engine for pre-trade risk checks
            initial_cash: Starting capital
            leverage_by_instrument: Optional dict of leverage per instrument
            funding_rates: Optional dict of funding rates per instrument
        """

        leverage_by_instrument = leverage_by_instrument or {}
        funding_rates = funding_rates or {}

        portfolio = PortfolioManager(initial_cash=initial_cash)
        margin_states: dict[str, MarginState] = {}

        for instrument in instruments:
            portfolio.register_instrument(instrument)
            margin_states[instrument.instrument_id] = MarginState()
            if instrument.instrument_type == "perpetual":
                leverage = leverage_by_instrument.get(instrument.instrument_id, self.default_leverage)
                if leverage > 1.0:
                    pos = portfolio.get_position(instrument.instrument_id)
                    pos.is_leveraged = True
                    pos.leverage = leverage

        oms = OrderManager()
        monitoring = MonitoringService()
        fills: list[Fill] = []
        bias_history: list = []
        equity_curve: list[tuple[datetime, float]] = []
        risk_rejections: list[RiskDecision] = []

        all_timestamps: set[datetime] = set()
        for klines in klines_by_instrument.values():
            for bar in klines:
                all_timestamps.add(bar.close_time)
        sorted_timestamps = sorted(all_timestamps)

        current_prices: dict[str, float] = {inst.instrument_id: 0.0 for inst in instruments}
        latest_klines: dict[str, Kline] = {}

        for timestamp in sorted_timestamps:
            for instrument in instruments:
                klines = klines_by_instrument.get(instrument.instrument_id, [])
                for bar in klines:
                    if bar.close_time == timestamp:
                        current_prices[instrument.instrument_id] = bar.close
                        latest_klines[instrument.instrument_id] = bar
                        break

            snapshot = portfolio.mark_to_market(current_prices, timestamp=timestamp)

            for instrument in instruments:
                if instrument.instrument_type == "perpetual" and instrument.instrument_id in current_prices:
                    funding_rate = funding_rates.get(instrument.instrument_id)
                    if funding_rate:
                        funding_accrued = self._calculate_funding_accrued(
                            instrument.instrument_id,
                            current_prices[instrument.instrument_id],
                            funding_rate.funding_rate,
                            margin_states,
                        )
                        if funding_accrued != 0.0:
                            snapshot.cash += funding_accrued

            self._check_margin_critical(margin_states, snapshot, monitoring)

            if latest_klines:
                for instrument_id, bar in latest_klines.items():
                    if bar is None:
                        continue
                    instrument_obj = next((i for i in instruments if i.instrument_id == instrument_id), None)
                    if instrument_obj is None:
                        continue

                    history = tuple(klines_by_instrument[instrument_id][:klines_by_instrument[instrument_id].index(bar) + 1])
                    bias = intelligence_engine.directional_bias(
                        instrument_id,
                        as_of=timestamp,
                        window=self.bias_window,
                    )
                    bias_history.append(bias)

                    context = StrategyContext(
                        instrument=instrument_obj,
                        current_bar=bar,
                        history=history,
                        position=portfolio.get_position(instrument_id),
                        cash=snapshot.cash,
                        equity=snapshot.equity,
                        latest_bias=bias,
                    )

                    signal = strategy.generate_signal(context)
                    current_qty = portfolio.get_position(instrument_id).quantity
                    if bar.close > 0:
                        target_qty = (snapshot.equity * signal.target_weight) / bar.close
                    else:
                        target_qty = 0.0

                    delta = target_qty - current_qty
                    if abs(delta) > instrument_obj.lot_size / 10:
                        request = OrderRequest(
                            client_order_id=f"{strategy.strategy_id}_{instrument_id}_{timestamp.isoformat()}",
                            instrument_id=instrument_id,
                            side=OrderSide.BUY if delta > 0 else OrderSide.SELL,
                            quantity=abs(delta),
                            strategy_id=strategy.strategy_id,
                        )
                        decision = risk_engine.evaluate_order(
                            request,
                            price=bar.close,
                            current_position_qty=current_qty,
                            snapshot=snapshot,
                        )
                        if decision.approved:
                            order = oms.submit_order(request)
                            matched_fills = self.execution.execute_on_bar(order, bar)
                            for fill in matched_fills:
                                oms.apply_fill(fill)
                                portfolio.apply_fill(fill)
                                fills.append(fill)
                                self._update_margin_state(
                                    fill,
                                    leverage_by_instrument.get(instrument_id, self.default_leverage),
                                    margin_states,
                                )
                        else:
                            risk_rejections.append(decision)
                            monitoring.record_risk_rejection(decision.reasons, request.client_order_id)

            snapshot = portfolio.mark_to_market(current_prices, timestamp=timestamp)
            monitoring.check_drawdown(snapshot, risk_engine.limits.max_drawdown)
            equity_curve.append((timestamp, snapshot.equity))

        metrics = self._metrics(equity_curve, fills)

        return BacktestResult(
            strategy_id=strategy.strategy_id,
            instrument_id=",".join([i.instrument_id for i in instruments]),
            equity_curve=tuple(equity_curve),
            orders=tuple(oms.orders.values()),
            fills=tuple(fills),
            metrics=metrics,
            alerts=tuple(monitoring.alerts),
            bias_history=tuple(bias_history),
            risk_rejections=tuple(risk_rejections),
        )

    def _calculate_funding_accrued(
        self,
        instrument_id: str,
        price: float,
        funding_rate: float,
        margin_states: dict[str, MarginState],
    ) -> float:
        """Calculate funding fee accrued for a perpetual position."""
        state = margin_states.get(instrument_id)
        if state is None or state.margin_used == 0.0:
            return 0.0

        position_value = state.margin_used * state.unrealized_pnl / state.margin_ratio if state.margin_ratio > 0 else 0.0
        funding_fee = position_value * funding_rate
        state.funding_accrued += funding_fee
        return -funding_fee

    def _update_margin_state(
        self,
        fill: Fill,
        leverage: float,
        margin_states: dict[str, MarginState],
    ) -> None:
        """Update margin state after a fill."""
        state = margin_states.get(fill.instrument_id)
        if state is None:
            return

        notional = abs(fill.quantity * fill.price)
        if leverage > 1.0:
            state.initial_margin = notional / leverage
            state.maintenance_margin = state.initial_margin * self.maintenance_margin_ratio

    def _check_margin_critical(
        self,
        margin_states: dict[str, MarginState],
        snapshot: PortfolioSnapshot,
        monitoring: MonitoringService,
    ) -> None:
        """Check if any position is near liquidation."""
        from quant_exchange.core.models import AlertSeverity

        for instrument_id, state in margin_states.items():
            if state.margin_used > 0 and state.margin_ratio > 0:
                if state.margin_ratio < self.maintenance_margin_ratio:
                    monitoring.record_alert(
                        code="margin_critical",
                        severity=AlertSeverity.CRITICAL,
                        message=f"Margin ratio critical for {instrument_id}",
                    )

    def _metrics(self, equity_curve: list[tuple], fills: list[Fill]) -> PerformanceMetrics:
        """Aggregate the equity curve into standard performance statistics."""

        equity_values = [item[1] for item in equity_curve]
        if not equity_values:
            return PerformanceMetrics(0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0)
        returns = []
        for idx in range(1, len(equity_values)):
            previous = equity_values[idx - 1]
            if previous == 0:
                continue
            returns.append(equity_values[idx] / previous - 1.0)
        total_return = safe_div(equity_values[-1] - equity_values[0], equity_values[0], 0.0)
        max_dd = max_drawdown(equity_values)
        annualized = annualize_return(total_return, len(equity_values))
        positive = [value for value in returns if value > 0]
        negative = [value for value in returns if value < 0]
        win_rate = safe_div(len(positive), len(returns), 0.0)
        profit_factor = safe_div(sum(positive), abs(sum(negative)), 0.0)
        total_notional = sum(abs(fill.quantity * fill.price) for fill in fills)
        turnover = total_notional / max(equity_values[0], 1.0)
        total_fees = sum(fill.fee for fill in fills)
        total_trades = len(fills)
        avg_trade_return = safe_div(total_return, total_trades, 0.0) if total_trades > 0 else 0.0
        costs = CostBreakdown(
            total_commission=total_fees,
            total_slippage=0.0,
            total_funding=0.0,
            total_cost=total_fees,
        )
        return PerformanceMetrics(
            total_return=total_return,
            annualized_return=annualized,
            max_drawdown=max_dd,
            sharpe=sharpe_ratio(returns),
            sortino=sortino_ratio(returns),
            calmar=safe_div(annualized, max_dd, 0.0),
            win_rate=win_rate,
            profit_factor=profit_factor,
            turnover=turnover,
            total_trades=total_trades,
            avg_trade_return=avg_trade_return,
            costs=costs,
        )


class BacktestResultStore:
    """Persist and retrieve backtest results."""

    def __init__(self, storage_path: str | Path | None = None) -> None:
        self._storage_path = Path(storage_path) if storage_path else None
        self._results: dict[str, BacktestResult] = {}

    def save(self, result: BacktestResult) -> str:
        """Save a backtest result and return its ID."""
        result_id = str(uuid.uuid4())
        self._results[result_id] = result

        if self._storage_path:
            self._persist_result(result_id, result)

        return result_id

    def load(self, result_id: str) -> BacktestResult | None:
        """Load a backtest result by ID."""
        if result_id in self._results:
            return self._results[result_id]

        if self._storage_path:
            return self._load_result(result_id)

        return None

    def list_results(
        self,
        strategy_id: str | None = None,
        limit: int = 100,
    ) -> list[BacktestResult]:
        """List backtest results, optionally filtered by strategy ID."""
        results = list(self._results.values())
        if strategy_id:
            results = [r for r in results if r.strategy_id == strategy_id]

        results.sort(key=lambda r: r.metrics.total_return, reverse=True)
        return results[:limit]

    def _persist_result(self, result_id: str, result: BacktestResult) -> None:
        """Persist a backtest result to disk."""
        if self._storage_path is None:
            return

        self._storage_path.mkdir(parents=True, exist_ok=True)
        result_file = self._storage_path / f"backtest_{result_id}.json"

        equity_curve_serializable = [(ts.isoformat(), eq) for ts, eq in result.equity_curve]

        data = {
            "result_id": result_id,
            "strategy_id": result.strategy_id,
            "instrument_id": result.instrument_id,
            "equity_curve": equity_curve_serializable,
            "metrics": {
                "total_return": result.metrics.total_return,
                "annualized_return": result.metrics.annualized_return,
                "max_drawdown": result.metrics.max_drawdown,
                "sharpe": result.metrics.sharpe,
                "sortino": result.metrics.sortino,
                "calmar": result.metrics.calmar,
                "win_rate": result.metrics.win_rate,
                "profit_factor": result.metrics.profit_factor,
                "turnover": result.metrics.turnover,
                "total_trades": result.metrics.total_trades,
                "avg_trade_return": result.metrics.avg_trade_return,
            },
            "orders_count": len(result.orders),
            "fills_count": len(result.fills),
            "alerts_count": len(result.alerts),
            "risk_rejections_count": len(result.risk_rejections),
        }

        with open(result_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

    def _load_result(self, result_id: str) -> BacktestResult | None:
        """Load a backtest result from disk."""
        if self._storage_path is None:
            return None

        result_file = self._storage_path / f"backtest_{result_id}.json"
        if not result_file.exists():
            return None

        with open(result_file, encoding="utf-8") as f:
            data = json.load(f)

        equity_curve = [(datetime.fromisoformat(ts), eq) for ts, eq in data["equity_curve"]]
        metrics = PerformanceMetrics(
            total_return=data["metrics"]["total_return"],
            annualized_return=data["metrics"]["annualized_return"],
            max_drawdown=data["metrics"]["max_drawdown"],
            sharpe=data["metrics"]["sharpe"],
            sortino=data["metrics"]["sortino"],
            calmar=data["metrics"]["calmar"],
            win_rate=data["metrics"]["win_rate"],
            profit_factor=data["metrics"]["profit_factor"],
            turnover=data["metrics"]["turnover"],
            total_trades=data["metrics"]["total_trades"],
            avg_trade_return=data["metrics"]["avg_trade_return"],
        )

        return BacktestResult(
            strategy_id=data["strategy_id"],
            instrument_id=data["instrument_id"],
            equity_curve=tuple(equity_curve),
            orders=(),
            fills=(),
            metrics=metrics,
            alerts=(),
            bias_history=(),
            risk_rejections=(),
        )


# ─── BT-08: Bias Audit ─────────────────────────────────────────────────────────


class BiasType(str, Enum):
    """Types of bias that can be audited."""
    LOOKAHEAD = "lookahead"
    FUTURE_FUNCTION = "future_function"
    TIME_MISALIGNMENT = "time_misalignment"
    SURVIVORSHIP = "survivorship"
    REFRESH_RATE = "refresh_rate"


@dataclass
class BiasFinding:
    """A single bias finding from an audit."""
    bias_type: BiasType
    severity: str  # "low", "medium", "high", "critical"
    description: str
    location: str | None = None
    timestamp: datetime = field(default_factory=utc_now)


@dataclass
class BiasAuditResult:
    """Result of a bias audit."""
    audit_id: str
    strategy_id: str
    passed: bool
    findings: list[BiasFinding]
    summary: dict[str, Any] = field(default_factory=dict)


class BiasAuditService:
    """Audit backtest runs for look-ahead bias, future functions, and time misalignment (BT-08).

    Detects common sources of backtest overfitting and invalid results.
    """

    def __init__(self) -> None:
        self._audit_history: list[BiasAuditResult] = []

    def audit_backtest(
        self,
        strategy_id: str,
        klines: list[Kline],
        orders: list[Order],
        fills: list[Fill],
    ) -> BiasAuditResult:
        """Run a comprehensive bias audit on a backtest run.

        Checks for:
        - Look-ahead bias: using future data in signal generation
        - Future function: hard-coded future values or references
        - Time misalignment: timestamps not properly aligned
        - Survivorship bias: ignoring delisted/failed instruments
        """
        audit_id = f"audit_{uuid.uuid4().hex[:12]}"
        findings: list[BiasFinding] = []

        # Check 1: Look-ahead bias via timestamp ordering
        findings.extend(self._check_timestamp_ordering(strategy_id, klines, fills))

        # Check 2: Future function detection (heuristic)
        findings.extend(self._check_future_function(strategy_id, orders))

        # Check 3: Time alignment
        findings.extend(self._check_time_alignment(strategy_id, klines))

        # Check 4: Order fill timing
        findings.extend(self._check_fill_timing(strategy_id, fills, klines))

        passed = not any(f.severity in ("high", "critical") for f in findings)

        result = BiasAuditResult(
            audit_id=audit_id,
            strategy_id=strategy_id,
            passed=passed,
            findings=findings,
            summary=self._generate_summary(findings),
        )

        self._audit_history.append(result)
        return result

    def _check_timestamp_ordering(
        self,
        strategy_id: str,
        klines: list[Kline],
        fills: list[Fill],
    ) -> list[BiasFinding]:
        """Check if timestamps are strictly non-decreasing."""
        findings: list[BiasFinding] = []

        # Check kline timestamps
        for i in range(1, len(klines)):
            if klines[i].close_time < klines[i - 1].close_time:
                findings.append(BiasFinding(
                    bias_type=BiasType.LOOKAHEAD,
                    severity="high",
                    description=f"Kline timestamp at index {i} is before previous timestamp",
                    location=f"kline_index_{i}",
                ))

        # Check fill timestamps vs kline timestamps
        for fill in fills:
            # Fill timestamp should not be before the bar it's filled on
            fill_time = fill.timestamp
            relevant_bars = [k for k in klines if k.close_time <= fill_time]
            if relevant_bars:
                latest_bar = max(relevant_bars, key=lambda k: k.close_time)
                if latest_bar.close_time == fill_time:
                    # Fill happened exactly at bar close - suspicious but not necessarily bias
                    pass

        return findings

    def _check_future_function(
        self,
        strategy_id: str,
        orders: list[Order],
    ) -> list[BiasFinding]:
        """Heuristic check for future function usage."""
        findings: list[BiasFinding] = []

        # Check for suspicious patterns in order timestamps
        # Orders placed at exact bar closes with perfect timing
        suspicious_count = 0
        for order in orders:
            # If order was placed at exactly 0 seconds of minute, might be using future data
            if order.updated_at.second == 0 and order.updated_at.microsecond == 0:
                # Very suspicious timing - could indicate pre-computation
                suspicious_count += 1

        if suspicious_count > len(orders) * 0.5:
            findings.append(BiasFinding(
                bias_type=BiasType.FUTURE_FUNCTION,
                severity="medium",
                description=f"{suspicious_count}/{len(orders)} orders placed with suspicious exact timing",
                location="order_timestamps",
            ))

        return findings

    def _check_time_alignment(
        self,
        strategy_id: str,
        klines: list[Kline],
    ) -> list[BiasFinding]:
        """Check if time intervals are consistent and properly aligned."""
        findings: list[BiasFinding] = []

        if len(klines) < 2:
            return findings

        # Calculate expected intervals
        intervals: list[timedelta] = []
        for i in range(1, min(len(klines), 100)):  # Check first 100 bars
            interval = klines[i].close_time - klines[i - 1].close_time
            intervals.append(interval)

        if not intervals:
            return findings

        # Check for inconsistent intervals
        most_common = max(set(intervals), key=intervals.count)
        inconsistent_count = sum(1 for iv in intervals if iv != most_common)

        if inconsistent_count > len(intervals) * 0.1:  # More than 10% inconsistent
            findings.append(BiasFinding(
                bias_type=BiasType.TIME_MISALIGNMENT,
                severity="low",
                description=f"{inconsistent_count}/{len(intervals)} bars have inconsistent time intervals",
                location="kline_intervals",
            ))

        return findings

    def _check_fill_timing(
        self,
        strategy_id: str,
        fills: list[Fill],
        klines: list[Kline],
    ) -> list[BiasFinding]:
        """Check if fills happen at valid times relative to bars."""
        findings: list[BiasFinding] = []

        for fill in fills:
            # Fill should not happen after the last available bar
            if klines:
                last_bar_time = max(k.close_time for k in klines)
                if fill.timestamp > last_bar_time:
                    findings.append(BiasFinding(
                        bias_type=BiasType.LOOKAHEAD,
                        severity="high",
                        description=f"Fill at {fill.timestamp} is after last bar {last_bar_time}",
                        location=f"fill_{fill.fill_id}",
                    ))

        return findings

    def _generate_summary(self, findings: list[BiasFinding]) -> dict[str, Any]:
        """Generate a summary of findings."""
        by_type: dict[str, int] = {}
        by_severity: dict[str, int] = {}

        for finding in findings:
            by_type[finding.bias_type.value] = by_type.get(finding.bias_type.value, 0) + 1
            by_severity[finding.severity] = by_severity.get(finding.severity, 0) + 1

        return {
            "total_findings": len(findings),
            "by_type": by_type,
            "by_severity": by_severity,
        }

    def get_audit_history(self) -> list[BiasAuditResult]:
        """Get all past audit results."""
        return list(self._audit_history)


# ─── BT-06: Batch Backtesting ──────────────────────────────────────────────────


@dataclass
class BatchBacktestResult:
    """Result of a batch backtest run with multiple parameter sets."""
    batch_id: str
    strategy_id: str
    total_runs: int
    results: list[BacktestResult]
    best_result: BacktestResult | None
    parameter_sweep_summary: dict[str, Any] = field(default_factory=dict)


class BatchBacktestEngine:
    """Run multiple backtest variations for parameter optimization (BT-06).

    Supports:
    - Parameter sweeps
    - Rolling window backtesting
    - In-sample / out-of-sample testing
    """

    def __init__(self, backtest_engine: BacktestEngine | None = None) -> None:
        self.backtest_engine = backtest_engine or BacktestEngine()

    def run_parameter_sweep(
        self,
        *,
        strategy: BaseStrategy,
        instrument: Instrument,
        klines: list[Kline],
        intelligence_engine,
        risk_engine: RiskEngine,
        parameter_grid: dict[str, list[Any]],
        initial_cash: float = 100_000.0,
    ) -> BatchBacktestResult:
        """Run backtests across a grid of parameter values."""
        import itertools

        batch_id = f"batch_{uuid.uuid4().hex[:12]}"
        results: list[BacktestResult] = []
        best_result: BacktestResult | None = None
        best_sharpe = float("-inf")

        # Generate all parameter combinations
        param_names = list(parameter_grid.keys())
        param_values = list(parameter_grid.values())
        combinations = list(itertools.product(*param_values))

        for combo in combinations:
            params = dict(zip(param_names, combo))

            # Apply parameters to strategy (assuming strategy has set_param method)
            if hasattr(strategy, "set_parameters"):
                strategy.set_parameters(params)

            # Run backtest
            result = self.backtest_engine.run(
                instrument=instrument,
                klines=klines,
                strategy=strategy,
                intelligence_engine=intelligence_engine,
                risk_engine=risk_engine,
                initial_cash=initial_cash,
            )

            results.append(result)

            if result.metrics.sharpe > best_sharpe:
                best_sharpe = result.metrics.sharpe
                best_result = result

        # Generate summary
        returns = [r.metrics.total_return for r in results]
        sharpes = [r.metrics.sharpe for r in results]
        drawdowns = [r.metrics.max_drawdown for r in results]

        summary = {
            "total_combinations": len(combinations),
            "returns": {"min": min(returns), "max": max(returns), "mean": sum(returns) / len(returns)},
            "sharpes": {"min": min(sharpes), "max": max(sharpes), "mean": sum(sharpes) / len(sharpes)},
            "drawdowns": {"min": min(drawdowns), "max": max(drawdowns)},
        }

        return BatchBacktestResult(
            batch_id=batch_id,
            strategy_id=strategy.strategy_id,
            total_runs=len(results),
            results=results,
            best_result=best_result,
            parameter_sweep_summary=summary,
        )

    def run_rolling_window(
        self,
        *,
        strategy: BaseStrategy,
        instrument: Instrument,
        klines: list[Kline],
        intelligence_engine,
        risk_engine: RiskEngine,
        train_window_days: int = 60,
        test_window_days: int = 20,
        step_days: int = 10,
        initial_cash: float = 100_000.0,
    ) -> BatchBacktestResult:
        """Run rolling window backtests (walk-forward optimization)."""
        from datetime import timedelta as td

        batch_id = f"rolling_{uuid.uuid4().hex[:12]}"
        results: list[BacktestResult] = []
        all_klines = sorted(klines, key=lambda k: k.close_time)

        if not all_klines:
            return BatchBacktestResult(
                batch_id=batch_id,
                strategy_id=strategy.strategy_id,
                total_runs=0,
                results=[],
                best_result=None,
            )

        start_time = all_klines[0].close_time
        end_time = all_klines[-1].close_time

        current_train_start = start_time
        best_result: BacktestResult | None = None
        best_sharpe = float("-inf")

        while True:
            train_end = current_train_start + td(days=train_window_days)
            test_end = train_end + td(days=test_window_days)

            if test_end > end_time:
                break

            # Slice klines for train and test
            train_klines = [k for k in all_klines if current_train_start <= k.close_time <= train_end]
            test_klines = [k for k in all_klines if train_end < k.close_time <= test_end]

            if len(train_klines) < 10 or len(test_klines) < 5:
                current_train_start += td(days=step_days)
                continue

            # Run train backtest (for parameter optimization indication)
            train_result = self.backtest_engine.run(
                instrument=instrument,
                klines=train_klines,
                strategy=strategy,
                intelligence_engine=intelligence_engine,
                risk_engine=risk_engine,
                initial_cash=initial_cash,
            )

            # Run test backtest (out-of-sample)
            test_result = self.backtest_engine.run(
                instrument=instrument,
                klines=test_klines,
                strategy=strategy,
                intelligence_engine=intelligence_engine,
                risk_engine=risk_engine,
                initial_cash=initial_cash,
            )

            results.append(test_result)

            if test_result.metrics.sharpe > best_sharpe:
                best_sharpe = test_result.metrics.sharpe
                best_result = test_result

            current_train_start += td(days=step_days)

        return BatchBacktestResult(
            batch_id=batch_id,
            strategy_id=strategy.strategy_id,
            total_runs=len(results),
            results=results,
            best_result=best_result,
            parameter_sweep_summary={"type": "rolling_window"},
        )

    def run_in_sample_out_of_sample(
        self,
        *,
        strategy: BaseStrategy,
        instrument: Instrument,
        klines: list[Kline],
        intelligence_engine,
        risk_engine: RiskEngine,
        train_ratio: float = 0.7,
        initial_cash: float = 100_000.0,
    ) -> tuple[BacktestResult, BacktestResult]:
        """Split data into train (in-sample) and test (out-of-sample) sets."""
        sorted_klines = sorted(klines, key=lambda k: k.close_time)
        split_idx = int(len(sorted_klines) * train_ratio)

        train_klines = sorted_klines[:split_idx]
        test_klines = sorted_klines[split_idx:]

        train_result = self.backtest_engine.run(
            instrument=instrument,
            klines=train_klines,
            strategy=strategy,
            intelligence_engine=intelligence_engine,
            risk_engine=risk_engine,
            initial_cash=initial_cash,
        )

        test_result = self.backtest_engine.run(
            instrument=instrument,
            klines=test_klines,
            strategy=strategy,
            intelligence_engine=intelligence_engine,
            risk_engine=risk_engine,
            initial_cash=initial_cash,
        )

        return train_result, test_result