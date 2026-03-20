"""Event-driven backtest engine used by the MVP platform."""

from __future__ import annotations
from datetime import timedelta
from typing import Any

from quant_exchange.core.models import (
    BacktestResult,
    CostBreakdown,
    Kline,
    OrderRequest,
    OrderSide,
    PerformanceMetrics,
    PortfolioSnapshot,
    RiskDecision,
)
from quant_exchange.backtest.multi_asset import BiasAuditService
from quant_exchange.core.utils import annualize_return, max_drawdown, safe_div, sharpe_ratio, sortino_ratio
from quant_exchange.execution.oms import OrderManager, PaperExecutionEngine
from quant_exchange.monitoring.service import MonitoringService
from quant_exchange.portfolio.service import PortfolioManager
from quant_exchange.risk.service import RiskEngine
from quant_exchange.strategy.base import BaseStrategy, StrategyContext


class BacktestEngine:
    """Replay bar data through strategy, risk, execution, and portfolio services."""

    def __init__(
        self,
        *,
        fee_rate: float = 0.001,
        slippage_bps: float = 5.0,
        bias_window: timedelta = timedelta(days=1),
        bias_audit: BiasAuditService | None = None,
    ) -> None:
        self.execution = PaperExecutionEngine(fee_rate=fee_rate, slippage_bps=slippage_bps)
        self.bias_window = bias_window
        self.bias_audit = bias_audit if bias_audit is not None else BiasAuditService()

    def _run_bias_audit(self, strategy_id: str, klines: list[Kline], orders, fills) -> Any:
        """Run bias audit if configured (BT-08)."""
        try:
            return self.bias_audit.audit_backtest(
                strategy_id=strategy_id,
                klines=klines,
                orders=list(orders),
                fills=list(fills),
            )
        except Exception:
            return None

    def run(
        self,
        *,
        instrument,
        klines: list[Kline],
        strategy: BaseStrategy,
        intelligence_engine,
        risk_engine: RiskEngine,
        initial_cash: float = 100_000.0,
    ) -> BacktestResult:
        """Run a deterministic single-instrument backtest over bar data."""

        portfolio = PortfolioManager(initial_cash=initial_cash)
        portfolio.register_instrument(instrument)
        oms = OrderManager()
        monitoring = MonitoringService()
        fills = []
        bias_history = []
        equity_curve = []
        risk_rejections: list[RiskDecision] = []
        strategy_events: list[dict] = []

        # ── on_init: one-time initialization before first bar ──────────────────
        if klines:
            init_bar = klines[0]
            init_snapshot = portfolio.mark_to_market(
                {instrument.instrument_id: init_bar.close}, timestamp=init_bar.close_time
            )
            init_context = StrategyContext(
                instrument=instrument,
                current_bar=init_bar,
                history=(init_bar,),
                position=portfolio.get_position(instrument.instrument_id),
                cash=init_snapshot.cash,
                equity=init_snapshot.equity,
                latest_bias=intelligence_engine.directional_bias(
                    instrument.instrument_id,
                    as_of=init_bar.close_time,
                    window=self.bias_window,
                ),
            )
            strategy.on_init(init_context)

        for idx, bar in enumerate(klines):
            history = tuple(klines[: idx + 1])
            snapshot = portfolio.mark_to_market({instrument.instrument_id: bar.close}, timestamp=bar.close_time)
            bias = intelligence_engine.directional_bias(
                instrument.instrument_id,
                as_of=bar.close_time,
                window=self.bias_window,
            )
            bias_history.append(bias)
            context = StrategyContext(
                instrument=instrument,
                current_bar=bar,
                history=history,
                position=portfolio.get_position(instrument.instrument_id),
                cash=snapshot.cash,
                equity=snapshot.equity,
                latest_bias=bias,
            )

            # ── on_bar lifecycle hook ─────────────────────────────────────────
            strategy.on_bar(context)

            signal = strategy.generate_signal(context)
            current_qty = portfolio.get_position(instrument.instrument_id).quantity
            target_qty = 0.0 if bar.close == 0 else (snapshot.equity * signal.target_weight) / bar.close
            delta = target_qty - current_qty
            if abs(delta) > instrument.lot_size / 10:
                request = OrderRequest(
                    client_order_id=f"{strategy.strategy_id}_{bar.close_time.isoformat()}",
                    instrument_id=instrument.instrument_id,
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
                        # ── on_order_update lifecycle hook ──────────────────────
                        strategy.on_order_update(order, fill)
                else:
                    risk_rejections.append(decision)
                    monitoring.record_risk_rejection(decision.reasons, request.client_order_id)
                    # ── on_risk_event lifecycle hook ───────────────────────────
                    strategy.on_risk_event(decision)
                    strategy_events.append({
                        "event": "risk_rejection",
                        "bar_time": bar.close_time.isoformat(),
                        "reasons": decision.reasons,
                    })
            snapshot = portfolio.mark_to_market({instrument.instrument_id: bar.close}, timestamp=bar.close_time)
            monitoring.check_drawdown(snapshot, risk_engine.limits.max_drawdown)
            equity_curve.append((bar.close_time, snapshot.equity))
        metrics = self._metrics(equity_curve, fills)
        return BacktestResult(
            strategy_id=strategy.strategy_id,
            instrument_id=instrument.instrument_id,
            equity_curve=tuple(equity_curve),
            orders=tuple(oms.orders.values()),
            fills=tuple(fills),
            metrics=metrics,
            alerts=tuple(monitoring.alerts),
            bias_history=tuple(bias_history),
            risk_rejections=tuple(risk_rejections),
            audit_result=self._run_bias_audit(strategy.strategy_id, klines, oms.orders.values(), fills) if self.bias_audit else None,
        )

    def _metrics(self, equity_curve: list[tuple], fills) -> PerformanceMetrics:
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
            total_slippage=0.0,  # Slippage is baked into fill prices
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
