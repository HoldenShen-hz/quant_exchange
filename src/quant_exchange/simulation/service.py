"""Simulated trading service for web-based paper accounts and manual order flows."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from threading import RLock
from typing import Any
from uuid import uuid4

from quant_exchange.core.models import (
    Direction,
    DirectionalBias,
    Fill,
    Instrument,
    Kline,
    Order,
    OrderRequest,
    OrderSide,
    OrderStatus,
    OrderType,
    Position,
    TimeInForce,
)
from quant_exchange.execution import OrderManager, PaperExecutionEngine
from quant_exchange.portfolio import PortfolioManager
from quant_exchange.strategy import MovingAverageSentimentStrategy, StrategyContext


def _now_iso() -> str:
    """Return a timezone-aware ISO timestamp."""

    return datetime.now(timezone.utc).isoformat()


class SimulatedTradingService:
    """Provide a resumable paper account, simulated execution, and dashboard summaries."""

    DEFAULT_ACCOUNT_CODE = "paper_stock_main"
    DEFAULT_INITIAL_CASH = 1_000_000.0

    def __init__(
        self,
        *,
        persistence=None,
        stock_directory=None,
        risk_engine=None,
        market_rules=None,
        backtest_engine=None,
        intelligence_engine=None,
    ) -> None:
        self.persistence = persistence
        self.stock_directory = stock_directory
        self.risk_engine = risk_engine
        self.market_rules = market_rules
        self.backtest_engine = backtest_engine
        self.intelligence_engine = intelligence_engine
        self.realtime_market = None
        self._lock = RLock()
        self._runtimes: dict[str, dict[str, Any]] = {}
        self.ensure_account()

    def ensure_account(
        self,
        account_code: str = DEFAULT_ACCOUNT_CODE,
        *,
        account_name: str = "股票模拟账户",
        initial_cash: float = DEFAULT_INITIAL_CASH,
    ) -> dict[str, Any]:
        """Create the default paper account when it does not exist yet."""

        if self.persistence is None:
            return {
                "account_code": account_code,
                "account_name": account_name,
                "market_type": "stock",
                "environment": "PAPER",
                "status": "ACTIVE",
                "initial_cash": initial_cash,
                "created_at": _now_iso(),
            }
        row = self.persistence.fetch_one(
            "acct_trading_accounts",
            where="account_code = :account_code",
            params={"account_code": account_code},
        )
        if row is not None:
            payload = row["payload"]
            payload.setdefault("initial_cash", initial_cash)
            return payload
        payload = {
            "account_code": account_code,
            "account_name": account_name,
            "market_type": "stock",
            "environment": "PAPER",
            "status": "ACTIVE",
            "initial_cash": float(initial_cash),
            "base_currency": "CNY",
            "created_at": _now_iso(),
            "updated_at": _now_iso(),
        }
        self.persistence.upsert_record(
            "acct_trading_accounts",
            "account_code",
            account_code,
            payload,
            extra_columns={
                "account_name": account_name,
                "market_type": "stock",
                "environment": "PAPER",
                "status": "ACTIVE",
            },
        )
        return payload

    def dashboard(
        self,
        *,
        account_code: str = DEFAULT_ACCOUNT_CODE,
        instrument_id: str | None = None,
        order_limit: int = 12,
        fill_limit: int = 12,
    ) -> dict[str, Any]:
        """Return a full web dashboard for one paper account."""

        with self._lock:
            runtime = self._runtime(account_code)
            refresh_happened = self._refresh_open_orders(runtime)
            snapshot = runtime["portfolio"].mark_to_market(self._price_map(runtime), timestamp=self._as_of(runtime))
            if refresh_happened:
                self._persist_snapshot(account_code, snapshot)
                self._persist_positions(account_code, runtime["portfolio"], snapshot.equity)
            positions = self._position_rows(runtime["portfolio"], snapshot.equity)
            orders = self._order_rows(runtime["oms"], limit=order_limit)
            fills = self._fill_rows(account_code, limit=fill_limit)
            focus = instrument_id or self._focus_instrument(positions, orders)
            return {
                "account": runtime["account"],
                "snapshot": self._snapshot_payload(snapshot),
                "positions": positions,
                "orders": orders,
                "fills": fills,
                "open_order_count": sum(1 for item in orders if item["status"] in {"accepted", "partially_filled"}),
                "focus_instrument_id": focus,
                "strategy_diff": self._strategy_diff(runtime, focus, snapshot),
            }

    def submit_order(
        self,
        *,
        instrument_id: str,
        side: str,
        quantity: float,
        account_code: str = DEFAULT_ACCOUNT_CODE,
        order_type: str = "market",
        limit_price: float | None = None,
        strategy_id: str = "manual_web",
    ) -> dict[str, Any]:
        """Submit one simulated order and match it against the latest market bar."""

        with self._lock:
            runtime = self._runtime(account_code)
            instrument = self._instrument(instrument_id)
            bar = self._current_bar(instrument_id)
            position = runtime["portfolio"].get_position(instrument_id)
            request = OrderRequest(
                client_order_id=f"paper:{uuid4().hex[:12]}",
                instrument_id=instrument_id,
                side=OrderSide(side),
                quantity=float(quantity),
                order_type=OrderType(order_type),
                price=float(limit_price) if limit_price not in (None, "") else None,
                tif=TimeInForce.GTC,
                strategy_id=strategy_id,
                metadata={"account_code": account_code, "channel": "web_paper"},
            )
            market_decision = self.market_rules.validate_order(
                instrument,
                request,
                as_of=bar.close_time,
                available_position_qty=max(position.quantity, 0.0),
            )
            if not market_decision.approved:
                rejected = runtime["oms"].reject_order(request, ",".join(market_decision.reasons))
                self._persist_order(account_code, rejected)
                return self.dashboard(account_code=account_code, instrument_id=instrument_id) | {
                    "last_action": {"type": "reject", "reasons": list(market_decision.reasons)}
                }
            snapshot = runtime["portfolio"].mark_to_market(self._price_map(runtime), timestamp=bar.close_time)
            risk_decision = self.risk_engine.evaluate_order(
                request,
                price=bar.close,
                current_position_qty=position.quantity,
                snapshot=snapshot,
            )
            if not risk_decision.approved:
                rejected = runtime["oms"].reject_order(request, ",".join(risk_decision.reasons))
                self._persist_order(account_code, rejected)
                return self.dashboard(account_code=account_code, instrument_id=instrument_id) | {
                    "last_action": {"type": "risk_reject", "reasons": list(risk_decision.reasons)}
                }
            order = runtime["oms"].submit_order(request)
            self._persist_order(account_code, order)
            fills = self._match_order(runtime, account_code, order, bar)
            snapshot = runtime["portfolio"].mark_to_market(self._price_map(runtime), timestamp=bar.close_time)
            self._persist_snapshot(account_code, snapshot)
            self._persist_positions(account_code, runtime["portfolio"], snapshot.equity)
            payload = self.dashboard(account_code=account_code, instrument_id=instrument_id)
            payload["last_action"] = {
                "type": "submitted",
                "order_id": order.order_id,
                "fill_count": len(fills),
                "status": runtime["oms"].orders[order.order_id].status.value,
            }
            return payload

    def cancel_order(self, order_id: str, *, account_code: str = DEFAULT_ACCOUNT_CODE) -> dict[str, Any]:
        """Cancel a live simulated order."""

        with self._lock:
            runtime = self._runtime(account_code)
            order = runtime["oms"].cancel_order(order_id)
            self._persist_order(account_code, order)
            snapshot = runtime["portfolio"].mark_to_market(self._price_map(runtime), timestamp=self._as_of(runtime))
            self._persist_snapshot(account_code, snapshot)
            return self.dashboard(account_code=account_code, instrument_id=order.request.instrument_id) | {
                "last_action": {"type": "cancelled", "order_id": order_id}
            }

    def reset_account(self, *, account_code: str = DEFAULT_ACCOUNT_CODE) -> dict[str, Any]:
        """Clear paper orders, fills, positions, and snapshots for one account."""

        with self._lock:
            if self.persistence is not None:
                for table in ("trade_orders", "trade_executions", "trade_positions", "acct_account_snapshots"):
                    self.persistence.delete_where(table, where="account_code = :account_code", params={"account_code": account_code})
            self._runtimes.pop(account_code, None)
            account = self.ensure_account(account_code)
            account["last_reset_at"] = _now_iso()
            if self.persistence is not None:
                self.persistence.upsert_record(
                    "acct_trading_accounts",
                    "account_code",
                    account_code,
                    account,
                    extra_columns={
                        "account_name": account["account_name"],
                        "market_type": account["market_type"],
                        "environment": account["environment"],
                        "status": account["status"],
                    },
                )
            return self.dashboard(account_code=account_code)

    def _runtime(self, account_code: str) -> dict[str, Any]:
        """Load or initialize the in-memory runtime for one paper account."""

        runtime = self._runtimes.get(account_code)
        if runtime is not None:
            return runtime
        account = self.ensure_account(account_code)
        portfolio = PortfolioManager(initial_cash=float(account.get("initial_cash", self.DEFAULT_INITIAL_CASH)))
        for instrument in self.stock_directory.instruments.values():
            portfolio.register_instrument(instrument)
        oms = OrderManager()
        runtime = {"account": account, "portfolio": portfolio, "oms": oms}
        self._load_orders(account_code, oms)
        self._load_fills(account_code, portfolio)
        self._runtimes[account_code] = runtime
        return runtime

    def _load_orders(self, account_code: str, oms: OrderManager) -> None:
        """Rehydrate persisted paper orders into the in-memory OMS."""

        if self.persistence is None:
            return
        rows = self.persistence.fetch_all(
            "trade_orders",
            where="account_code = :account_code",
            params={"account_code": account_code},
            order_by="created_at ASC",
        )
        for row in rows:
            payload = row["payload"]
            order = self._deserialize_order(payload)
            oms.orders[order.order_id] = order
            oms.client_order_index[order.request.client_order_id] = order.order_id

    def _load_fills(self, account_code: str, portfolio: PortfolioManager) -> None:
        """Reapply persisted fills into the paper portfolio."""

        if self.persistence is None:
            return
        rows = self.persistence.fetch_all(
            "trade_executions",
            where="account_code = :account_code",
            params={"account_code": account_code},
            order_by="execution_time ASC",
        )
        for row in rows:
            fill = self._deserialize_fill(row["payload"])
            portfolio.apply_fill(fill)

    def _refresh_open_orders(self, runtime: dict[str, Any]) -> bool:
        """Try to match any accepted simulated orders against the latest bar."""

        changed = False
        for order in list(runtime["oms"].orders.values()):
            if order.status not in {OrderStatus.ACCEPTED, OrderStatus.PARTIALLY_FILLED}:
                continue
            bar = self._current_bar(order.request.instrument_id)
            fills = self._match_order(runtime, runtime["account"]["account_code"], order, bar)
            changed = changed or bool(fills)
        return changed

    def _match_order(self, runtime: dict[str, Any], account_code: str, order: Order, bar: Kline) -> list[Fill]:
        """Execute a simulated order on the current market bar."""

        fill_ratio = self._fill_ratio(order, bar)
        engine = PaperExecutionEngine(fee_rate=0.0005, slippage_bps=6.0, max_fill_ratio=fill_ratio)
        fills = engine.execute_on_bar(order, bar)
        for fill in fills:
            runtime["oms"].apply_fill(fill)
            runtime["portfolio"].apply_fill(fill)
            self._persist_fill(account_code, fill)
        self._persist_order(account_code, runtime["oms"].orders[order.order_id])
        return fills

    def _fill_ratio(self, order: Order, bar: Kline) -> float:
        """Return a deterministic participation ratio based on recent one-minute volume."""

        capacity = max(bar.volume * 0.02, self._instrument(order.request.instrument_id).lot_size * 2)
        if order.remaining_quantity <= 0:
            return 0.0
        return min(1.0, max(0.1, capacity / order.remaining_quantity))

    def _persist_order(self, account_code: str, order: Order) -> None:
        """Persist one simulated order to SQLite."""

        if self.persistence is None:
            return
        payload = self._serialize_order(account_code, order)
        self.persistence.upsert_record(
            "trade_orders",
            "order_id",
            order.order_id,
            payload,
            extra_columns={
                "client_order_id": order.request.client_order_id,
                "instrument_id": order.request.instrument_id,
                "status": order.status.value,
                "account_code": account_code,
                "environment": "PAPER",
                "strategy_id": order.request.strategy_id,
            },
        )

    def _persist_fill(self, account_code: str, fill: Fill) -> None:
        """Persist one simulated execution fill."""

        if self.persistence is None:
            return
        payload = self._serialize_fill(account_code, fill)
        self.persistence.upsert_record(
            "trade_executions",
            "execution_id",
            fill.fill_id,
            payload,
            extra_columns={
                "order_id": fill.order_id,
                "instrument_id": fill.instrument_id,
                "execution_time": fill.timestamp.isoformat(),
                "account_code": account_code,
            },
        )

    def _persist_positions(self, account_code: str, portfolio: PortfolioManager, equity: float) -> None:
        """Persist the latest simulated positions."""

        if self.persistence is None:
            return
        for position in portfolio.positions.values():
            if abs(position.quantity) < 1e-10 and abs(position.realized_pnl) < 1e-10:
                continue
            instrument = self._instrument(position.instrument_id)
            market_value = position.quantity * position.last_price * instrument.contract_multiplier
            unrealized_pnl = (position.last_price - position.average_cost) * position.quantity * instrument.contract_multiplier
            payload = {
                "account_code": account_code,
                "instrument_id": position.instrument_id,
                "symbol": instrument.symbol,
                "quantity": position.quantity,
                "average_cost": position.average_cost,
                "last_price": position.last_price,
                "market_value": market_value,
                "unrealized_pnl": unrealized_pnl,
                "realized_pnl": position.realized_pnl,
                "weight": 0.0 if equity == 0 else market_value / equity,
                "updated_at": _now_iso(),
            }
            self.persistence.upsert_record(
                "trade_positions",
                "position_key",
                f"{account_code}:{position.instrument_id}",
                payload,
                extra_columns={
                    "instrument_id": position.instrument_id,
                    "quantity": position.quantity,
                    "account_code": account_code,
                },
            )

    def _persist_snapshot(self, account_code: str, snapshot) -> None:
        """Persist one account valuation snapshot."""

        if self.persistence is None:
            return
        payload = self._snapshot_payload(snapshot) | {"account_code": account_code}
        self.persistence.delete_where(
            "acct_account_snapshots",
            where="account_code = :account_code AND snapshot_time = :snapshot_time",
            params={"account_code": account_code, "snapshot_time": payload["timestamp"]},
            commit=False,
        )
        self.persistence.insert_record(
            "acct_account_snapshots",
            payload,
            extra_columns={
                "account_code": account_code,
                "snapshot_time": payload["timestamp"],
                "equity": snapshot.equity,
                "created_at": payload["timestamp"],
            },
        )

    def _price_map(self, runtime: dict[str, Any]) -> dict[str, float]:
        """Build the latest price map for all instruments relevant to the account."""

        instrument_ids = set(runtime["portfolio"].positions)
        instrument_ids.update(order.request.instrument_id for order in runtime["oms"].orders.values())
        return {instrument_id: self._latest_price(instrument_id) for instrument_id in instrument_ids if instrument_id}

    def _latest_price(self, instrument_id: str) -> float:
        """Return the latest simulated quote price for one stock."""

        if self.realtime_market is not None:
            snapshot = self.realtime_market.snapshot([instrument_id])
            if snapshot["quotes"]:
                return float(snapshot["quotes"][0]["last_price"])
        core = self.stock_directory.get_stock_core(instrument_id)
        if core.get("last_price") not in (None, ""):
            return float(core["last_price"])
        minute_payload = self.stock_directory.get_minute_bars(instrument_id, limit=1)
        if minute_payload["bars"]:
            return float(minute_payload["bars"][-1]["close"])
        history = self.stock_directory.get_stock_history(instrument_id, limit=1)
        if history["bars"]:
            return float(history["bars"][-1]["close"])
        return float(self.stock_directory.get_stock_core(instrument_id).get("last_price") or 0.0)

    def _current_bar(self, instrument_id: str) -> Kline:
        """Return the latest one-minute or daily bar as the simulated execution reference."""

        minute_payload = self.stock_directory.get_minute_bars(instrument_id, limit=1)
        if minute_payload["bars"]:
            row = minute_payload["bars"][-1]
            close_time = datetime.fromisoformat(row["bar_time"])
            return Kline(
                instrument_id=instrument_id,
                timeframe="1m",
                open_time=close_time - timedelta(minutes=1),
                close_time=close_time,
                open=float(row["open"]),
                high=float(row["high"]),
                low=float(row["low"]),
                close=float(row["close"]),
                volume=float(row["volume"]),
            )
        history = self.stock_directory.get_stock_history(instrument_id, limit=1)
        row = history["bars"][-1]
        close_time = datetime.fromisoformat(f"{row['trade_date']}T15:00:00+00:00")
        return Kline(
            instrument_id=instrument_id,
            timeframe="1d",
            open_time=close_time - timedelta(days=1),
            close_time=close_time,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        )

    def _position_rows(self, portfolio: PortfolioManager, equity: float) -> list[dict[str, Any]]:
        """Serialize current paper positions for the web dashboard."""

        rows = []
        for position in portfolio.positions.values():
            if abs(position.quantity) < 1e-10 and abs(position.realized_pnl) < 1e-10:
                continue
            instrument = self._instrument(position.instrument_id)
            market_value = position.quantity * position.last_price * instrument.contract_multiplier
            unrealized_pnl = (position.last_price - position.average_cost) * position.quantity * instrument.contract_multiplier
            rows.append(
                {
                    "instrument_id": position.instrument_id,
                    "symbol": instrument.symbol,
                    "company_name": self.stock_directory.profiles[position.instrument_id].company_name,
                    "quantity": round(position.quantity, 4),
                    "average_cost": round(position.average_cost, 4),
                    "last_price": round(position.last_price, 4),
                    "market_value": round(market_value, 2),
                    "weight": round(0.0 if equity == 0 else market_value / equity, 4),
                    "unrealized_pnl": round(unrealized_pnl, 2),
                    "realized_pnl": round(position.realized_pnl, 2),
                }
            )
        rows.sort(key=lambda item: abs(item["market_value"]), reverse=True)
        return rows

    def _order_rows(self, oms: OrderManager, *, limit: int = 12) -> list[dict[str, Any]]:
        """Serialize recent paper orders for the web dashboard."""

        rows = [self._serialize_order("", order) for order in oms.orders.values()]
        rows.sort(key=lambda item: item["created_at"], reverse=True)
        return rows[:limit]

    def _fill_rows(self, account_code: str, *, limit: int = 12) -> list[dict[str, Any]]:
        """Return recent paper fills from persistence."""

        if self.persistence is None:
            return []
        rows = self.persistence.fetch_all(
            "trade_executions",
            where="account_code = :account_code",
            params={"account_code": account_code},
            order_by="execution_time DESC",
            limit=limit,
        )
        return [row["payload"] for row in rows]

    def _focus_instrument(self, positions: list[dict[str, Any]], orders: list[dict[str, Any]]) -> str | None:
        """Choose one instrument to anchor the strategy-difference card."""

        if positions:
            return positions[0]["instrument_id"]
        if orders:
            return orders[0]["instrument_id"]
        return None

    def _strategy_diff(self, runtime: dict[str, Any], instrument_id: str | None, snapshot) -> dict[str, Any] | None:
        """Compare the paper position against a simple live-signal and short backtest summary."""

        if not instrument_id:
            return None
        history = self.stock_directory.get_stock_history(instrument_id, limit=90)
        bars = [self._history_bar_to_kline(instrument_id, row) for row in history["bars"]]
        if len(bars) < 8:
            return None
        strategy = MovingAverageSentimentStrategy()
        current_position = runtime["portfolio"].get_position(instrument_id)
        latest_bar = bars[-1]
        previous_bar = bars[-2]
        move = 0.0 if previous_bar.close == 0 else (latest_bar.close - previous_bar.close) / previous_bar.close
        bias = DirectionalBias(
            instrument_id=instrument_id,
            as_of=latest_bar.close_time,
            window=timedelta(days=3),
            score=round(move, 4),
            direction=Direction.LONG if move > 0.004 else Direction.SHORT if move < -0.004 else Direction.FLAT,
            confidence=min(round(abs(move) * 25, 3), 0.95),
            supporting_documents=0,
        )
        context = StrategyContext(
            instrument=self._instrument(instrument_id),
            current_bar=latest_bar,
            history=tuple(bars),
            position=Position(
                instrument_id=instrument_id,
                quantity=current_position.quantity,
                average_cost=current_position.average_cost,
                realized_pnl=current_position.realized_pnl,
                last_price=current_position.last_price or latest_bar.close,
            ),
            cash=snapshot.cash,
            equity=snapshot.equity,
            latest_bias=bias,
        )
        signal = strategy.generate_signal(context)
        paper_weight = 0.0 if snapshot.equity == 0 else (current_position.quantity * latest_bar.close) / snapshot.equity
        backtest_metrics = None
        if self.backtest_engine is not None and self.intelligence_engine is not None and len(bars) >= 20:
            result = self.backtest_engine.run(
                instrument=self._instrument(instrument_id),
                klines=bars,
                strategy=MovingAverageSentimentStrategy(),
                intelligence_engine=self.intelligence_engine,
                risk_engine=type(self.risk_engine)(self.risk_engine.limits),
                initial_cash=float(runtime["account"].get("initial_cash", self.DEFAULT_INITIAL_CASH)),
            )
            backtest_metrics = asdict(result.metrics)
        gap = round(signal.target_weight - paper_weight, 4)
        summary = (
            "模拟仓位与策略信号基本一致。"
            if abs(gap) <= 0.05
            else "模拟仓位与策略信号存在偏差，适合继续观察下单节奏、限价挂单和部分成交影响。"
        )
        return {
            "instrument_id": instrument_id,
            "symbol": self._instrument(instrument_id).symbol,
            "signal_target_weight": round(signal.target_weight, 4),
            "paper_weight": round(paper_weight, 4),
            "weight_gap": gap,
            "signal_reason": signal.reason,
            "signal_metadata": signal.metadata,
            "summary": summary,
            "backtest_metrics": backtest_metrics,
        }

    def _as_of(self, runtime: dict[str, Any]) -> datetime:
        """Return a stable timestamp for the latest paper snapshot."""

        focus = self._focus_instrument(self._position_rows(runtime["portfolio"], 1.0), self._order_rows(runtime["oms"], limit=1))
        return self._current_bar(focus).close_time if focus else datetime.now(timezone.utc)

    def _instrument(self, instrument_id: str) -> Instrument:
        """Return one normalized instrument from the stock directory."""

        return self.stock_directory.instruments[instrument_id]

    def _serialize_order(self, account_code: str, order: Order) -> dict[str, Any]:
        """Serialize one order with nested request fields."""

        request = order.request
        return {
            "account_code": account_code or request.metadata.get("account_code"),
            "order_id": order.order_id,
            "instrument_id": request.instrument_id,
            "symbol": self._instrument(request.instrument_id).symbol,
            "side": request.side.value,
            "quantity": request.quantity,
            "filled_quantity": order.filled_quantity,
            "remaining_quantity": order.remaining_quantity,
            "average_fill_price": order.average_fill_price,
            "order_type": request.order_type.value,
            "limit_price": request.price,
            "tif": request.tif.value,
            "status": order.status.value,
            "strategy_id": request.strategy_id,
            "created_at": order.created_at.isoformat(),
            "updated_at": order.updated_at.isoformat(),
            "rejection_reason": order.rejection_reason,
            "request": {
                "client_order_id": request.client_order_id,
                "instrument_id": request.instrument_id,
                "side": request.side.value,
                "quantity": request.quantity,
                "order_type": request.order_type.value,
                "price": request.price,
                "tif": request.tif.value,
                "strategy_id": request.strategy_id,
                "reduce_only": request.reduce_only,
                "metadata": request.metadata,
            },
        }

    def _serialize_fill(self, account_code: str, fill: Fill) -> dict[str, Any]:
        """Serialize one fill for persistence and the web UI."""

        return {
            "account_code": account_code,
            "fill_id": fill.fill_id,
            "order_id": fill.order_id,
            "instrument_id": fill.instrument_id,
            "symbol": self._instrument(fill.instrument_id).symbol,
            "side": fill.side.value,
            "quantity": fill.quantity,
            "price": fill.price,
            "fee": fill.fee,
            "timestamp": fill.timestamp.isoformat(),
        }

    def _deserialize_order(self, payload: dict[str, Any]) -> Order:
        """Recreate an Order dataclass from persisted JSON payload."""

        request_payload = payload["request"]
        request = OrderRequest(
            client_order_id=request_payload["client_order_id"],
            instrument_id=request_payload["instrument_id"],
            side=OrderSide(request_payload["side"]),
            quantity=float(request_payload["quantity"]),
            order_type=OrderType(request_payload["order_type"]),
            price=request_payload.get("price"),
            tif=TimeInForce(request_payload.get("tif", "gtc")),
            strategy_id=request_payload.get("strategy_id", "manual"),
            reduce_only=bool(request_payload.get("reduce_only", False)),
            metadata=request_payload.get("metadata") or {},
        )
        return Order(
            order_id=payload["order_id"],
            request=request,
            status=OrderStatus(payload["status"]),
            created_at=datetime.fromisoformat(payload["created_at"]),
            updated_at=datetime.fromisoformat(payload["updated_at"]),
            filled_quantity=float(payload.get("filled_quantity", 0.0)),
            average_fill_price=float(payload.get("average_fill_price", 0.0)),
            rejection_reason=payload.get("rejection_reason"),
        )

    def _deserialize_fill(self, payload: dict[str, Any]) -> Fill:
        """Recreate a Fill dataclass from persisted JSON payload."""

        return Fill(
            fill_id=payload["fill_id"],
            order_id=payload["order_id"],
            instrument_id=payload["instrument_id"],
            side=OrderSide(payload["side"]),
            quantity=float(payload["quantity"]),
            price=float(payload["price"]),
            timestamp=datetime.fromisoformat(payload["timestamp"]),
            fee=float(payload.get("fee", 0.0)),
        )

    def _history_bar_to_kline(self, instrument_id: str, row: dict[str, Any]) -> Kline:
        """Convert one history row into the Kline format expected by the backtest engine."""

        close_time = datetime.fromisoformat(f"{row['trade_date']}T15:00:00+00:00")
        return Kline(
            instrument_id=instrument_id,
            timeframe="1d",
            open_time=close_time - timedelta(days=1),
            close_time=close_time,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        )

    def _snapshot_payload(self, snapshot) -> dict[str, Any]:
        """Serialize one portfolio snapshot for API responses and persistence."""

        return {
            "timestamp": snapshot.timestamp.isoformat(),
            "cash": round(snapshot.cash, 2),
            "positions_value": round(snapshot.positions_value, 2),
            "equity": round(snapshot.equity, 2),
            "gross_exposure": round(snapshot.gross_exposure, 2),
            "net_exposure": round(snapshot.net_exposure, 2),
            "leverage": round(snapshot.leverage, 4),
            "drawdown": round(snapshot.drawdown, 4),
        }
