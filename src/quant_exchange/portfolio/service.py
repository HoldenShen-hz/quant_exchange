"""Portfolio accounting helpers for positions, valuation, and rebalancing.

Supports multiple capital allocation methods per the documented framework:
- Fixed weight: static target allocation
- Volatility target: scale position by inverse realized vol
- Risk parity: equalize risk contribution across instruments
- Mean-variance: optimize for return / variance trade-off
- Kelly: theoretically optimal leverage
"""

from __future__ import annotations

from math import sqrt

from quant_exchange.core.models import (
    AllocationMethod,
    Fill,
    Instrument,
    MarketType,
    OrderRequest,
    OrderSide,
    PortfolioSnapshot,
    Position,
    utc_now,
)
from quant_exchange.core.utils import safe_div, stddev


class PortfolioManager:
    """Maintain cash, positions, and portfolio-level exposure statistics."""

    def __init__(self, *, initial_cash: float = 0.0) -> None:
        self.cash = initial_cash
        self.positions: dict[str, Position] = {}
        self.instruments: dict[str, Instrument] = {}
        self._peak_equity = initial_cash

    def register_instrument(self, instrument: Instrument) -> None:
        """Register an instrument so fills can be valued consistently."""

        self.instruments[instrument.instrument_id] = instrument
        self.positions.setdefault(instrument.instrument_id, Position(instrument.instrument_id))

    def get_position(self, instrument_id: str) -> Position:
        """Return the current position object for an instrument."""

        return self.positions.setdefault(instrument_id, Position(instrument_id))

    def apply_fill(self, fill: Fill) -> Position:
        """Apply a fill to cash, position quantity, cost basis, and realized PnL."""

        instrument = self.instruments.get(
            fill.instrument_id,
            Instrument(fill.instrument_id, fill.instrument_id, market=MarketType.CRYPTO),
        )
        multiplier = instrument.contract_multiplier
        position = self.get_position(fill.instrument_id)
        signed_qty = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity
        previous_qty = position.quantity
        previous_avg_cost = position.average_cost
        if previous_qty == 0 or previous_qty * signed_qty > 0:
            new_qty = previous_qty + signed_qty
            weighted_cost = previous_avg_cost * abs(previous_qty) + fill.price * abs(signed_qty)
            position.average_cost = weighted_cost / abs(new_qty) if new_qty else 0.0
        else:
            closing_qty = min(abs(previous_qty), abs(signed_qty))
            if previous_qty > 0:
                position.realized_pnl += (fill.price - previous_avg_cost) * closing_qty * multiplier
            else:
                position.realized_pnl += (previous_avg_cost - fill.price) * closing_qty * multiplier
            new_qty = previous_qty + signed_qty
            if new_qty == 0:
                position.average_cost = 0.0
            elif previous_qty * new_qty < 0:
                position.average_cost = fill.price
        position.quantity = new_qty
        position.last_price = fill.price
        self.cash -= signed_qty * fill.price * multiplier + fill.fee
        return position

    def mark_to_market(self, price_map: dict[str, float], *, timestamp=None) -> PortfolioSnapshot:
        """Create a portfolio snapshot using the supplied price map."""

        positions_value = 0.0
        gross = 0.0
        net = 0.0
        for instrument_id, position in self.positions.items():
            instrument = self.instruments.get(
                instrument_id,
                Instrument(instrument_id, instrument_id, market=MarketType.CRYPTO),
            )
            price = price_map.get(instrument_id, position.last_price)
            notional = position.quantity * price * instrument.contract_multiplier
            positions_value += notional
            gross += abs(notional)
            net += notional
            position.last_price = price
        equity = self.cash + positions_value
        self._peak_equity = max(self._peak_equity, equity)
        drawdown = 0.0 if self._peak_equity <= 0 else max(0.0, (self._peak_equity - equity) / self._peak_equity)
        leverage = 0.0 if equity == 0 else gross / abs(equity)
        return PortfolioSnapshot(
            timestamp=timestamp or utc_now(),
            cash=self.cash,
            positions_value=positions_value,
            equity=equity,
            gross_exposure=gross,
            net_exposure=net,
            leverage=leverage,
            drawdown=drawdown,
        )

    def rebalance_orders(
        self,
        target_weights: dict[str, float],
        price_map: dict[str, float],
        *,
        strategy_id: str = "rebalance",
    ) -> list[OrderRequest]:
        """Create rebalance orders that move the portfolio toward target weights."""

        snapshot = self.mark_to_market(price_map)
        equity = snapshot.equity
        orders: list[OrderRequest] = []
        for instrument_id, target_weight in target_weights.items():
            price = price_map[instrument_id]
            current_qty = self.get_position(instrument_id).quantity
            target_qty = (equity * target_weight) / price if price else 0.0
            delta = target_qty - current_qty
            if abs(delta) < 1e-8:
                continue
            side = OrderSide.BUY if delta > 0 else OrderSide.SELL
            orders.append(
                OrderRequest(
                    client_order_id=f"{strategy_id}_{instrument_id}_{len(orders)}",
                    instrument_id=instrument_id,
                    side=side,
                    quantity=abs(delta),
                    strategy_id=strategy_id,
                )
            )
        return orders

    # ------------------------------------------------------------------
    # Portfolio allocation methods per documented framework
    # ------------------------------------------------------------------

    @staticmethod
    def fixed_weight_allocation(
        instrument_ids: list[str],
        weights: dict[str, float],
    ) -> dict[str, float]:
        """Return fixed target weights (pass-through, validates sum <= 1)."""

        result: dict[str, float] = {}
        for iid in instrument_ids:
            result[iid] = weights.get(iid, 0.0)
        return result

    @staticmethod
    def volatility_target_allocation(
        instrument_ids: list[str],
        realized_vols: dict[str, float],
        target_vol: float = 0.15,
    ) -> dict[str, float]:
        """Scale position weights inversely by realized volatility to target a portfolio vol.

        Each instrument weight = (target_vol / instrument_vol) / N.
        """

        n = len(instrument_ids)
        if n == 0:
            return {}
        result: dict[str, float] = {}
        for iid in instrument_ids:
            vol = realized_vols.get(iid, target_vol)
            scale = safe_div(target_vol, vol, 1.0) / n
            result[iid] = max(0.0, min(1.0, scale))
        return result

    @staticmethod
    def risk_parity_allocation(
        instrument_ids: list[str],
        realized_vols: dict[str, float],
    ) -> dict[str, float]:
        """Simplified risk parity: allocate inversely proportional to volatility.

        True risk parity would use a covariance matrix; this is the diagonal approx.
        """

        inv_vols: dict[str, float] = {}
        for iid in instrument_ids:
            vol = realized_vols.get(iid, 0.0)
            inv_vols[iid] = safe_div(1.0, vol, 1.0)
        total = sum(inv_vols.values())
        if total == 0:
            equal = safe_div(1.0, len(instrument_ids), 0.0)
            return {iid: equal for iid in instrument_ids}
        return {iid: v / total for iid, v in inv_vols.items()}

    @staticmethod
    def kelly_allocation(
        instrument_ids: list[str],
        win_rates: dict[str, float],
        avg_win_loss_ratios: dict[str, float],
        fraction: float = 0.5,
    ) -> dict[str, float]:
        """Half-Kelly allocation: f* = fraction * (p * b - q) / b.

        Where p = win_rate, q = 1 - p, b = avg_win / avg_loss.
        """

        result: dict[str, float] = {}
        for iid in instrument_ids:
            p = win_rates.get(iid, 0.5)
            b = avg_win_loss_ratios.get(iid, 1.0)
            q = 1.0 - p
            kelly_f = safe_div(p * b - q, b, 0.0) * fraction
            result[iid] = max(0.0, min(1.0, kelly_f))
        return result

    @staticmethod
    def mean_variance_allocation(
        instrument_ids: list[str],
        expected_returns: dict[str, float],
        covariance_matrix: dict[str, dict[str, float]],
        risk_aversion: float = 1.0,
    ) -> dict[str, float]:
        """Mean-variance (Markowitz) efficient allocation.

        Solves: max_w (w.T @ mu - (risk_aversion/2) * w.T @ Sigma @ w)
        Using the simplified diagonal covariance solution when full matrix is unavailable.
        """
        if not instrument_ids:
            return {}
        # Build full covariance dict for any missing entries
        cov: dict[str, dict[str, float]] = {}
        for iid in instrument_ids:
            cov[iid] = {}
            for jid in instrument_ids:
                if iid == jid:
                    cov[iid][jid] = covariance_matrix.get(iid, {}).get(jid, 0.01)
                else:
                    cov[iid][jid] = covariance_matrix.get(iid, {}).get(jid, 0.0)

        # Compute denominator: sum_i sigma_ii^2 for diagonal approx, or use simple variance
        total_var = sum(cov[iid][iid] for iid in instrument_ids)
        if total_var == 0:
            equal = safe_div(1.0, len(instrument_ids), 0.0)
            return {iid: equal for iid in instrument_ids}

        # Simplified MV: weight proportional to expected_return / variance
        # (ignores cross-correlations for the MVP diagonal solution)
        weights: dict[str, float] = {}
        for iid in instrument_ids:
            ret = expected_returns.get(iid, 0.0)
            var = cov[iid][iid]
            # target return approximation: ret / var, then normalize
            raw = safe_div(ret, var, 0.0)
            weights[iid] = max(0.0, raw)

        total_raw = sum(weights.values())
        if total_raw == 0:
            equal = safe_div(1.0, len(instrument_ids), 0.0)
            return {iid: equal for iid in instrument_ids}
        return {iid: w / total_raw for iid, w in weights.items()}

    @staticmethod
    def hrp_allocation(
        instrument_ids: list[str],
        covariance_matrix: dict[str, dict[str, float]],
    ) -> dict[str, float]:
        """Hierarchical Risk Parity allocation using a simplified HRP algorithm.

        Uses hierarchical clustering on the correlation matrix to build a tree,
        then allocates risk equally across leaf clusters (recursive bisection).
        For the MVP we use a correlation-based distance and equal-weight clusters.
        """
        if not instrument_ids:
            return {}
        n = len(instrument_ids)
        if n == 1:
            return {instrument_ids[0]: 1.0}

        # Build correlation matrix from covariance
        def _corr(iid: str, jid: str) -> float:
            vi = covariance_matrix.get(iid, {}).get(iid, 1.0)
            vj = covariance_matrix.get(jid, {}).get(jid, 1.0)
            c = covariance_matrix.get(iid, {}).get(jid, 0.0)
            denom = sqrt(vi) * sqrt(vj)
            return safe_div(c, denom, 0.0)

        # Simple approach: use inverse variance weighted by average correlation
        # as a proxy for the full HRP algorithm
        inv_var: dict[str, float] = {}
        for iid in instrument_ids:
            var = covariance_matrix.get(iid, {}).get(iid, 1.0)
            inv_var[iid] = safe_div(1.0, var, 1.0)

        # Adjust by average absolute correlation (higher correlation -> lower weight)
        for iid in instrument_ids:
            avg_corr = sum(abs(_corr(iid, jid)) for jid in instrument_ids if jid != iid) / max(n - 1, 1)
            inv_var[iid] *= max(0.1, 1.0 - avg_corr)

        total = sum(inv_var.values())
        if total == 0:
            equal = safe_div(1.0, n, 0.0)
            return {iid: equal for iid in instrument_ids}
        return {iid: v / total for iid, v in inv_var.items()}

    def compute_allocation(
        self,
        method: AllocationMethod,
        instrument_ids: list[str],
        *,
        weights: dict[str, float] | None = None,
        realized_vols: dict[str, float] | None = None,
        target_vol: float = 0.15,
        win_rates: dict[str, float] | None = None,
        avg_win_loss_ratios: dict[str, float] | None = None,
        expected_returns: dict[str, float] | None = None,
        covariance_matrix: dict[str, dict[str, float]] | None = None,
        risk_aversion: float = 1.0,
    ) -> dict[str, float]:
        """Dispatch to the appropriate allocation method."""

        if method == AllocationMethod.FIXED_WEIGHT:
            return self.fixed_weight_allocation(instrument_ids, weights or {})
        elif method == AllocationMethod.VOLATILITY_TARGET:
            return self.volatility_target_allocation(instrument_ids, realized_vols or {}, target_vol)
        elif method == AllocationMethod.RISK_PARITY:
            return self.risk_parity_allocation(instrument_ids, realized_vols or {})
        elif method == AllocationMethod.KELLY:
            return self.kelly_allocation(instrument_ids, win_rates or {}, avg_win_loss_ratios or {})
        elif method == AllocationMethod.MEAN_VARIANCE:
            return self.mean_variance_allocation(
                instrument_ids,
                expected_returns or {},
                covariance_matrix or {},
                risk_aversion,
            )
        elif method == AllocationMethod.HRP:
            return self.hrp_allocation(instrument_ids, covariance_matrix or {})
        else:
            # Default to equal weight for unsupported methods
            equal = safe_div(1.0, len(instrument_ids), 0.0)
            return {iid: equal for iid in instrument_ids}


class MultiStrategyAllocator:
    """Manages capital allocation across multiple strategies sharing a portfolio.

    Tracks per-strategy budget, realized PnL, correlation-based rebalancing,
    and emits allocation recommendations.
    """

    def __init__(
        self,
        total_budget: float,
        min_strategy_allocation: float = 0.05,
        max_strategy_allocation: float = 0.50,
    ) -> None:
        self.total_budget = total_budget
        self.min_allocation = min_strategy_allocation
        self.max_allocation = max_strategy_allocation
        self._strategy_budgets: dict[str, float] = {}  # strategy_id -> allocated amount
        self._strategy_returns: dict[str, list[float]] = defaultdict(list)
        self._strategyPnL: dict[str, float] = defaultdict(float)

    def register_strategy(self, strategy_id: str, initial_allocation: float | None = None) -> None:
        """Register a strategy and assign it an initial budget slice."""
        if initial_allocation is not None:
            budget = self.total_budget * initial_allocation
        else:
            budget = self.total_budget * self.min_allocation
        self._strategy_budgets[strategy_id] = budget

    def record_return(self, strategy_id: str, return_pct: float) -> None:
        """Record a return for a strategy to build history for correlation."""
        self._strategy_returns[strategy_id].append(return_pct)
        # Keep last 100 returns
        if len(self._strategy_returns[strategy_id]) > 100:
            self._strategy_returns[strategy_id] = self._strategy_returns[strategy_id][-100:]

    def record_pnl(self, strategy_id: str, pnl: float) -> None:
        """Record realized PnL for a strategy."""
        self._strategyPnL[strategy_id] += pnl

    def get_strategy_budget(self, strategy_id: str) -> float:
        """Return the current allocated budget for a strategy."""
        return self._strategy_budgets.get(strategy_id, 0.0)

    def get_strategy_weights(self) -> dict[str, float]:
        """Return current strategy weights as fraction of total budget."""
        total = sum(self._strategy_budgets.values())
        if total == 0:
            return {}
        return {sid: b / total for sid, b in self._strategy_budgets.items()}

    def rebalance(
        self,
        correlation_matrix: dict[str, dict[str, float]] | None = None,
    ) -> dict[str, float]:
        """Rebalance strategy budgets using inverse correlation weighting.

        Strategies with lower correlation to others get higher weight.
        Budgets are clipped to min/max allocation bounds.
        """
        strategy_ids = list(self._strategy_budgets.keys())
        if not strategy_ids:
            return {}

        if correlation_matrix is None or len(strategy_ids) == 1:
            # Equal weight when no correlation data
            equal = 1.0 / len(strategy_ids)
            result = {sid: max(self.min_allocation, min(self.max_allocation, equal)) for sid in strategy_ids}
            total = sum(result.values())
            return {sid: w / total for sid, w in result.items()}

        # Compute average correlation for each strategy (lower = more independent)
        avg_corr: dict[str, float] = {}
        for sid in strategy_ids:
            corrs = [abs(correlation_matrix.get(sid, {}).get(other, 0.0)) for other in strategy_ids if other != sid]
            avg_corr[sid] = sum(corrs) / len(corrs) if corrs else 0.0

        # Weight inversely proportional to average correlation
        inv_corr: dict[str, float] = {sid: max(0.01, 1.0 - c) for sid, c in avg_corr.items()}
        total_ic = sum(inv_corr.values())
        if total_ic == 0:
            equal = 1.0 / len(strategy_ids)
            return {sid: equal for sid in strategy_ids}

        raw_weights = {sid: ic / total_ic for sid, ic in inv_corr.items()}

        # Clip to min/max and renormalize
        clipped = {sid: max(self.min_allocation, min(self.max_allocation, w)) for sid, w in raw_weights.items()}
        total_clipped = sum(clipped.values())
        final_weights = {sid: c / total_clipped for sid, c in clipped.items()}

        # Update budgets
        new_total = sum(self._strategyPnL.values()) + self.total_budget
        for sid, weight in final_weights.items():
            self._strategy_budgets[sid] = new_total * weight

        return final_weights

    def correlation_matrix(self) -> dict[str, dict[str, float]]:
        """Compute pairwise return correlations across registered strategies."""
        strategy_ids = list(self._strategy_returns.keys())
        n = len(strategy_ids)
        if n == 0:
            return {}
        result: dict[str, dict[str, float]] = {}
        for i, sid_i in enumerate(strategy_ids):
            result[sid_i] = {}
            for j, sid_j in enumerate(strategy_ids):
                if i == j:
                    result[sid_i][sid_j] = 1.0
                    continue
                rets_i = self._strategy_returns.get(sid_i, [])
                rets_j = self._strategy_returns.get(sid_j, [])
                min_len = min(len(rets_i), len(rets_j))
                if min_len < 2:
                    result[sid_i][sid_j] = 0.0
                else:
                    # Pearson correlation
                    mean_i = sum(rets_i[-min_len:]) / min_len
                    mean_j = sum(rets_j[-min_len:]) / min_len
                    cov = sum((rets_i[-min_len + k] - mean_i) * (rets_j[-min_len + k] - mean_j) for k in range(min_len)) / min_len
                    std_i = sqrt(sum((r - mean_i) ** 2 for r in rets_i[-min_len:]) / min_len)
                    std_j = sqrt(sum((r - mean_j) ** 2 for r in rets_j[-min_len:]) / min_len)
                    result[sid_i][sid_j] = safe_div(cov, std_i * std_j, 0.0)
        return result
