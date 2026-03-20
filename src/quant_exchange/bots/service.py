"""FMZ-style strategy bot management, interactions, and notification workflows."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from quant_exchange.core.models import Direction, DirectionalBias, Instrument, Kline, Position
from quant_exchange.strategy import MovingAverageSentimentStrategy, StrategyContext


def _now() -> str:
    """Return an ISO timestamp used by bot payloads and notifications."""

    return datetime.now(timezone.utc).isoformat()


@dataclass(slots=True, frozen=True)
class StrategyTemplate:
    """Metadata for one reusable strategy template."""

    template_code: str
    template_name: str
    category: str
    description: str
    engine_code: str
    default_params: dict[str, float | int]
    parameter_schema: tuple[dict[str, object], ...] = field(default_factory=tuple)
    supported_commands: tuple[str, ...] = ("start", "pause", "stop", "sync_now", "set_param", "liquidate")


class StrategyBotService:
    """Manage strategy bots, bot interactions, and operator notifications."""

    def __init__(self, persistence=None, stock_directory=None) -> None:
        self.persistence = persistence
        self.stock_directory = stock_directory
        self.templates = self._build_templates()

    def list_templates(self) -> list[dict]:
        """Return all available strategy templates."""

        return [asdict(template) for template in self.templates.values()]

    def create_bot(
        self,
        *,
        template_code: str,
        instrument_id: str,
        bot_name: str | None = None,
        mode: str = "paper",
        params: dict | None = None,
    ) -> dict:
        """Create a new bot definition for one instrument."""

        template = self._template(template_code)
        stock = self._stock(instrument_id)
        merged_params = self._normalize_params(template, dict(template["default_params"]) | dict(params or {}))
        bot_id = f"bot:{uuid4().hex[:12]}"
        payload = {
            "bot_id": bot_id,
            "bot_name": bot_name or f"{stock['symbol']} {template['template_name']}",
            "template_code": template["template_code"],
            "template_name": template["template_name"],
            "engine_code": template["engine_code"],
            "category": template["category"],
            "description": template["description"],
            "instrument_id": instrument_id,
            "symbol": stock["symbol"],
            "company_name": stock["company_name"],
            "market_region": stock["market_region"],
            "exchange_code": stock["exchange_code"],
            "mode": mode,
            "status": "draft",
            "params": merged_params,
            "baseline_price": stock.get("last_price"),
            "last_price": stock.get("last_price"),
            "created_at": _now(),
            "started_at": None,
            "stopped_at": None,
            "updated_at": _now(),
            "last_signal": None,
            "metrics": {
                "heartbeat_at": None,
                "price_change_pct": 0.0,
                "signal_weight": 0.0,
                "signal_reason": "not_started",
                "manual_override": None,
            },
            "notes": [],
        }
        self._save_bot(payload)
        self._emit_notification(
            bot_id=bot_id,
            level="info",
            event_type="bot_created",
            title="机器人已创建",
            message=f"{payload['bot_name']} 已创建，当前状态为草稿。",
        )
        return payload

    def list_bots(self, *, refresh_runtime: bool = True) -> list[dict]:
        """Return all known strategy bots, refreshing running bots when requested."""

        bots = self._load_bots()
        if refresh_runtime:
            refreshed = []
            for bot in bots:
                if bot.get("status") == "running":
                    refreshed.append(self._refresh_bot(bot))
                else:
                    refreshed.append(bot)
            bots = refreshed
        bots.sort(key=lambda item: item.get("updated_at", ""), reverse=True)
        return bots

    def start_bot(self, bot_id: str) -> dict:
        """Start or resume one strategy bot."""

        bot = self._bot(bot_id)
        bot["status"] = "running"
        bot["started_at"] = bot.get("started_at") or _now()
        bot["stopped_at"] = None
        bot["metrics"]["manual_override"] = None
        bot = self._refresh_bot(bot)
        self._emit_notification(
            bot_id=bot_id,
            level="info",
            event_type="bot_started",
            title="机器人已启动",
            message=f"{bot['bot_name']} 已进入运行状态。",
        )
        return bot

    def pause_bot(self, bot_id: str) -> dict:
        """Pause one running bot."""

        bot = self._bot(bot_id)
        bot["status"] = "paused"
        bot["updated_at"] = _now()
        self._save_bot(bot)
        self._emit_notification(
            bot_id=bot_id,
            level="warning",
            event_type="bot_paused",
            title="机器人已暂停",
            message=f"{bot['bot_name']} 已暂停，参数和状态已保留。",
        )
        return bot

    def stop_bot(self, bot_id: str) -> dict:
        """Stop one bot and mark the session as closed."""

        bot = self._bot(bot_id)
        bot["status"] = "stopped"
        bot["stopped_at"] = _now()
        bot["updated_at"] = bot["stopped_at"]
        self._save_bot(bot)
        self._emit_notification(
            bot_id=bot_id,
            level="warning",
            event_type="bot_stopped",
            title="机器人已停止",
            message=f"{bot['bot_name']} 已停止运行。",
        )
        return bot

    def interact(self, bot_id: str, command: str, payload: dict | None = None) -> dict:
        """Execute a supported interaction command against one bot."""

        bot = self._bot(bot_id)
        payload = payload or {}
        if command == "sync_now":
            bot = self._refresh_bot(bot)
            message = "已同步一次最新信号和价格。"
            level = "info"
        elif command == "set_param":
            updates = dict(payload.get("updates") or {})
            template = self._template(bot["template_code"])
            bot["params"] = self._normalize_params(template, dict(bot["params"]) | updates)
            bot = self._refresh_bot(bot)
            message = f"已更新参数：{', '.join(sorted(updates)) or '无变化'}。"
            level = "info"
        elif command == "liquidate":
            bot["metrics"]["manual_override"] = "liquidated"
            bot["last_signal"] = {
                "target_weight": 0.0,
                "reason": "manual_liquidate",
                "timestamp": _now(),
                "metadata": {"manual_override": True},
            }
            bot["metrics"]["signal_weight"] = 0.0
            bot["metrics"]["signal_reason"] = "manual_liquidate"
            bot["updated_at"] = _now()
            self._save_bot(bot)
            message = "已触发人工清仓指令。"
            level = "warning"
        elif command == "add_note":
            note = str(payload.get("note") or "").strip()
            if note:
                bot["notes"] = [note, *list(bot.get("notes") or [])][:12]
            bot["updated_at"] = _now()
            self._save_bot(bot)
            message = "已追加运行备注。"
            level = "info"
        else:
            raise ValueError(f"unsupported_bot_command:{command}")
        self._save_command(bot_id, command, payload)
        self._emit_notification(
            bot_id=bot_id,
            level=level,
            event_type=f"bot_command_{command}",
            title="机器人交互已执行",
            message=f"{bot['bot_name']}：{message}",
        )
        return bot

    def list_notifications(self, *, limit: int = 20) -> list[dict]:
        """Return recent bot notifications."""

        if self.persistence is None:
            return []
        rows = self.persistence.fetch_all("ops_notifications", order_by="created_at DESC", limit=limit)
        return [row["payload"] for row in rows]

    def _refresh_bot(self, bot: dict) -> dict:
        """Recompute the latest signal and runtime metrics for one bot."""

        stock = self._stock(bot["instrument_id"])
        strategy = self._strategy(bot)
        context = self._strategy_context(bot, stock)
        signal = strategy.generate_signal(context)
        current_price = stock.get("last_price") or context.current_bar.close
        baseline = bot.get("baseline_price") or current_price
        price_change_pct = 0.0 if not baseline else round((current_price - baseline) / baseline * 100, 2)
        bot["last_price"] = current_price
        bot["updated_at"] = _now()
        bot["last_signal"] = {
            "target_weight": signal.target_weight,
            "reason": signal.reason,
            "timestamp": signal.timestamp.isoformat(),
            "metadata": signal.metadata,
        }
        bot["metrics"] = {
            **dict(bot.get("metrics") or {}),
            "heartbeat_at": _now(),
            "price_change_pct": price_change_pct,
            "signal_weight": round(signal.target_weight, 4),
            "signal_reason": signal.reason,
            "fast_ma": signal.metadata.get("fast"),
            "slow_ma": signal.metadata.get("slow"),
            "bias_score": signal.metadata.get("bias_score"),
        }
        self._save_bot(bot)
        return bot

    def _strategy(self, bot: dict):
        """Build a concrete strategy instance from one bot payload."""

        engine = bot.get("engine_code", "MovingAverageSentimentStrategy")
        if engine == "MovingAverageSentimentStrategy":
            return MovingAverageSentimentStrategy(strategy_id=bot["template_code"], params=bot["params"])
        elif engine == "GridTradingStrategy":
            from quant_exchange.strategy.grid_trading import GridTradingStrategy
            return GridTradingStrategy(strategy_id=bot["template_code"], params=bot["params"])
        elif engine == "TrailingStopStrategy":
            from quant_exchange.strategy.trailing_stop import TrailingStopStrategy
            return TrailingStopStrategy(strategy_id=bot["template_code"], params=bot["params"])
        elif engine == "MeanReversionStrategy":
            from quant_exchange.strategy.mean_reversion import MeanReversionStrategy
            return MeanReversionStrategy(strategy_id=bot["template_code"], params=bot["params"])
        # Fallback
        return MovingAverageSentimentStrategy(strategy_id=bot["template_code"], params=bot["params"])

    def _strategy_context(self, bot: dict, stock: dict) -> StrategyContext:
        """Construct the strategy context from stock history and synthetic bias."""

        slow_window = int(bot["params"].get("slow_window", 5))
        history_payload = self.stock_directory.get_stock_history(bot["instrument_id"], limit=max(slow_window + 5, 12))
        bars = tuple(self._kline(bot["instrument_id"], row) for row in history_payload["bars"])
        current_bar = bars[-1]
        previous_close = bars[-2].close if len(bars) >= 2 else current_bar.close
        move = 0.0 if previous_close == 0 else (current_bar.close - previous_close) / previous_close
        bias = DirectionalBias(
            instrument_id=bot["instrument_id"],
            as_of=current_bar.close_time,
            window=timedelta(days=3),
            score=round(move, 4),
            direction=Direction.LONG if move > 0.004 else Direction.SHORT if move < -0.004 else Direction.FLAT,
            confidence=min(round(abs(move) * 25, 3), 0.95),
            supporting_documents=0,
        )
        return StrategyContext(
            instrument=self.stock_directory.instruments[bot["instrument_id"]],
            current_bar=current_bar,
            history=bars,
            position=Position(
                instrument_id=bot["instrument_id"],
                quantity=0.0,
                average_cost=float(bot.get("baseline_price") or current_bar.close),
                last_price=current_bar.close,
            ),
            cash=100_000.0,
            equity=100_000.0,
            latest_bias=bias,
        )

    def _kline(self, instrument_id: str, row: dict) -> Kline:
        """Convert a stock-history row into a Kline object."""

        trade_date = datetime.fromisoformat(f"{row['trade_date']}T15:00:00+00:00")
        open_time = trade_date - timedelta(days=1)
        return Kline(
            instrument_id=instrument_id,
            timeframe="1d",
            open_time=open_time,
            close_time=trade_date,
            open=float(row["open"]),
            high=float(row["high"]),
            low=float(row["low"]),
            close=float(row["close"]),
            volume=float(row["volume"]),
        )

    def _stock(self, instrument_id: str) -> dict:
        """Return the current stock payload from the stock directory."""

        if self.stock_directory is None:
            raise ValueError("stock_directory_service_required")
        return self.stock_directory.get_stock(instrument_id)

    def _template(self, template_code: str) -> dict:
        """Return one template as a serialized dictionary."""

        template = self.templates.get(template_code)
        if template is None:
            raise KeyError(template_code)
        return asdict(template)

    def _normalize_params(self, template: dict, params: dict) -> dict:
        """Coerce bot parameters to the types declared by the template schema."""

        normalized = dict(params)
        for field in template.get("parameter_schema") or []:
            name = field.get("name")
            if not name or name not in normalized:
                continue
            value = normalized[name]
            if value in ("", None):
                continue
            field_type = field.get("type")
            try:
                if field_type == "int":
                    normalized[name] = int(value)
                elif field_type == "float":
                    normalized[name] = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"invalid_param:{name}") from exc
        return normalized

    def _save_bot(self, payload: dict) -> None:
        """Persist a bot payload into SQLite."""

        if self.persistence is None:
            return
        self.persistence.upsert_record(
            "ops_strategy_bots",
            "bot_id",
            payload["bot_id"],
            payload,
            extra_columns={
                "bot_name": payload["bot_name"],
                "template_code": payload["template_code"],
                "instrument_id": payload["instrument_id"],
                "status": payload["status"],
                "mode": payload["mode"],
            },
        )

    def _load_bots(self) -> list[dict]:
        """Load all persisted bots."""

        if self.persistence is None:
            return []
        return [row["payload"] for row in self.persistence.fetch_all("ops_strategy_bots")]

    def _bot(self, bot_id: str) -> dict:
        """Load one bot payload or raise when it is missing."""

        if self.persistence is None:
            raise KeyError(bot_id)
        row = self.persistence.fetch_one("ops_strategy_bots", where="bot_id = :bot_id", params={"bot_id": bot_id})
        if row is None:
            raise KeyError(bot_id)
        return row["payload"]

    def _save_command(self, bot_id: str, command: str, payload: dict) -> None:
        """Persist the bot command history."""

        if self.persistence is None:
            return
        command_id = f"{bot_id}:{command}:{uuid4().hex[:8]}"
        record = {"command_id": command_id, "bot_id": bot_id, "command": command, "payload": payload, "created_at": _now()}
        self.persistence.upsert_record(
            "ops_bot_commands",
            "command_id",
            command_id,
            record,
            extra_columns={"bot_id": bot_id, "command": command},
        )

    def _emit_notification(self, *, bot_id: str, level: str, event_type: str, title: str, message: str) -> None:
        """Store a bot notification for the web console."""

        if self.persistence is None:
            return
        notification_id = f"notif:{uuid4().hex[:12]}"
        payload = {
            "notification_id": notification_id,
            "bot_id": bot_id,
            "level": level,
            "event_type": event_type,
            "title": title,
            "message": message,
            "created_at": _now(),
        }
        self.persistence.upsert_record(
            "ops_notifications",
            "notification_id",
            notification_id,
            payload,
            extra_columns={"bot_id": bot_id, "level": level, "event_type": event_type},
        )

    def _build_templates(self) -> dict[str, StrategyTemplate]:
        """Build a small template library inspired by hosted quant platforms."""

        ma_schema = (
            {"name": "fast_window", "type": "int", "min": 2, "max": 20},
            {"name": "slow_window", "type": "int", "min": 3, "max": 60},
            {"name": "sentiment_threshold", "type": "float", "min": 0.0, "max": 0.5},
            {"name": "max_weight", "type": "float", "min": 0.1, "max": 1.0},
            {"name": "volatility_cap", "type": "float", "min": 0.1, "max": 2.0},
        )
        grid_schema = (
            {"name": "grid_levels", "type": "int", "min": 3, "max": 20},
            {"name": "grid_spacing_pct", "type": "float", "min": 0.005, "max": 0.1},
            {"name": "position_per_grid", "type": "float", "min": 0.05, "max": 0.5},
            {"name": "max_total_position", "type": "float", "min": 0.1, "max": 1.0},
        )
        trail_schema = (
            {"name": "trail_pct", "type": "float", "min": 0.01, "max": 0.3},
            {"name": "entry_pct", "type": "float", "min": 0.01, "max": 0.2},
            {"name": "max_weight", "type": "float", "min": 0.1, "max": 1.0},
            {"name": "lookback_bars", "type": "int", "min": 5, "max": 60},
        )
        mean_rev_schema = (
            {"name": "ma_window", "type": "int", "min": 5, "max": 60},
            {"name": "z_threshold", "type": "float", "min": 0.5, "max": 5.0},
            {"name": "max_weight", "type": "float", "min": 0.1, "max": 1.0},
            {"name": "exit_threshold", "type": "float", "min": 0.0, "max": 1.0},
        )
        templates = [
            StrategyTemplate(
                template_code="ma_sentiment",
                template_name="MA 情绪趋势",
                category="trend_following",
                description="基于均线和情绪偏置的趋势跟随模板，适合股票波段研究与纸面机器人管理。",
                engine_code="MovingAverageSentimentStrategy",
                default_params={"fast_window": 3, "slow_window": 5, "sentiment_threshold": 0.05, "max_weight": 0.9, "volatility_cap": 0.8},
                parameter_schema=ma_schema,
            ),
            StrategyTemplate(
                template_code="ma_breakout",
                template_name="Breakout 快节奏",
                category="breakout",
                description="更快的参数配置，适合跟踪强趋势与突破类信号。",
                engine_code="MovingAverageSentimentStrategy",
                default_params={"fast_window": 2, "slow_window": 8, "sentiment_threshold": 0.08, "max_weight": 1.0, "volatility_cap": 0.7},
                parameter_schema=ma_schema,
            ),
            StrategyTemplate(
                template_code="ma_defensive",
                template_name="Defensive 防守",
                category="risk_managed",
                description="更保守的仓位和阈值设定，适合稳健研究和低波动跟踪。",
                engine_code="MovingAverageSentimentStrategy",
                default_params={"fast_window": 5, "slow_window": 13, "sentiment_threshold": 0.03, "max_weight": 0.55, "volatility_cap": 0.55},
                parameter_schema=ma_schema,
            ),
            # BOT-04: Grid trading template
            StrategyTemplate(
                template_code="grid_trading",
                template_name="Grid 网格交易",
                category="grid",
                description="在参考价格上下构建等距网格，低位买入、高位卖出，适合震荡行情的量化网格策略。",
                engine_code="GridTradingStrategy",
                default_params={"grid_levels": 5, "grid_spacing_pct": 0.02, "position_per_grid": 0.15, "max_total_position": 0.9},
                parameter_schema=grid_schema,
            ),
            # BOT-04: Trailing stop template
            StrategyTemplate(
                template_code="trailing_stop",
                template_name="Trailing 追踪止损",
                category="trend_following",
                description="追踪峰值价格，设定回撤百分比为止损触发点，适合趋势跟踪与趋势保护。",
                engine_code="TrailingStopStrategy",
                default_params={"trail_pct": 0.05, "entry_pct": 0.02, "max_weight": 0.9, "lookback_bars": 20},
                parameter_schema=trail_schema,
            ),
            # BOT-04: Mean reversion template
            StrategyTemplate(
                template_code="mean_reversion",
                template_name="MeanRev 均值回归",
                category="mean_reversion",
                description="基于 z-score 偏离均值信号，在超卖时买入、超买时卖出，适合均值回归类行情。",
                engine_code="MeanReversionStrategy",
                default_params={"ma_window": 20, "z_threshold": 2.0, "max_weight": 0.85, "exit_threshold": 0.5},
                parameter_schema=mean_rev_schema,
            ),
        ]
        return {template.template_code: template for template in templates}
