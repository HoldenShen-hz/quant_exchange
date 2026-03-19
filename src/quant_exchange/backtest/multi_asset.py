"""Multi-asset portfolio backtesting and leverage/margin simulation.

Implements:
- Multi-instrument portfolio backtesting (BT-06)
- Margin and leverage simulation (BT-03)
- Funding rate simulation for perpetuals (BT-03)
- Backtest result persistence (BT-07)
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
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