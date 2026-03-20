"""Visual strategy editor service (VIS-01~VIS-05).

Covers:
- VIS-01: Block palette and component library
- VIS-02: Drag-and-drop canvas
- VIS-03: Connection management between blocks
- VIS-04: Code generation from visual strategy
- VIS-05: Strategy validation and testing
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class BlockCategory(str, Enum):
    SOURCE = "source"  # data sources
    INDICATOR = "indicator"  # technical indicators
    SIGNAL = "signal"  # signal generation
    FILTER = "filter"  # filters/conditions
    ORDER = "order"  # order execution
    UTILITY = "utility"  # utilities


class BlockType(str, Enum):
    # Sources
    PRICE_DATA = "price_data"
    FUNDAMENTAL_DATA = "fundamental_data"
    NEWS_DATA = "news_data"
    # Indicators
    SMA = "sma"
    EMA = "ema"
    RSI = "rsi"
    MACD = "macd"
    BOLLINGER = "bollinger"
    ATR = "atr"
    # Signals
    CROSSOVER_SIGNAL = "crossover_signal"
    THRESHOLD_SIGNAL = "threshold_signal"
    COMPOSITE_SIGNAL = "composite_signal"
    # Filters
    TIME_FILTER = "time_filter"
    VOLUME_FILTER = "volume_filter"
    TREND_FILTER = "trend_filter"
    # Orders
    MARKET_ORDER = "market_order"
    LIMIT_ORDER = "limit_order"
    STOP_ORDER = "stop_order"
    # Utilities
    VARIABLE = "variable"
    CONSTANT = "constant"
    MATH_OP = "math_op"
    LOGIC_OP = "logic_op"


@dataclass(slots=True)
class Block:
    """A visual block in the editor canvas."""

    block_id: str
    block_type: BlockType
    category: BlockCategory
    label: str
    x: float  # canvas position
    y: float
    parameters: dict[str, Any] = field(default_factory=dict)
    outputs: list[str] = field(default_factory=list)  # output port names
    inputs: list[str] = field(default_factory=list)  # input port names


@dataclass(slots=True)
class Connection:
    """A connection between two blocks."""

    connection_id: str
    source_block_id: str
    source_port: str
    target_block_id: str
    target_port: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Canvas:
    """A visual strategy canvas."""

    canvas_id: str
    user_id: str
    name: str
    blocks: list[Block] = field(default_factory=list)
    connections: list[Connection] = field(default_factory=list)
    strategy_code: str = ""
    is_valid: bool = False
    validation_errors: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class VisualEditorService:
    """Visual strategy editor service (VIS-01~VIS-05)."""

    # Block definitions for the palette (VIS-01)
    BLOCK_DEFINITIONS: dict[str, dict] = {
        "price_data": {"category": BlockCategory.SOURCE, "label": "价格数据", "inputs": [], "outputs": ["price", "volume"]},
        "sma": {"category": BlockCategory.INDICATOR, "label": "简单移动平均", "inputs": ["price"], "outputs": ["sma_value"], "params": {"period": 20}},
        "ema": {"category": BlockCategory.INDICATOR, "label": "指数移动平均", "inputs": ["price"], "outputs": ["ema_value"], "params": {"period": 12}},
        "rsi": {"category": BlockCategory.INDICATOR, "label": "RSI指标", "inputs": ["price"], "outputs": ["rsi_value"], "params": {"period": 14}},
        "macd": {"category": BlockCategory.INDICATOR, "label": "MACD", "inputs": ["price"], "outputs": ["macd", "signal", "histogram"], "params": {"fast": 12, "slow": 26, "signal": 9}},
        "bollinger": {"category": BlockCategory.INDICATOR, "label": "布林带", "inputs": ["price"], "outputs": ["upper", "middle", "lower"], "params": {"period": 20, "std_dev": 2}},
        "atr": {"category": BlockCategory.INDICATOR, "label": "ATR指标", "inputs": ["price"], "outputs": ["atr_value"], "params": {"period": 14}},
        "crossover_signal": {"category": BlockCategory.SIGNAL, "label": "交叉信号", "inputs": ["input_a", "input_b"], "outputs": ["signal"]},
        "threshold_signal": {"category": BlockCategory.SIGNAL, "label": "阈值信号", "inputs": ["value"], "outputs": ["signal"], "params": {"threshold": 50, "condition": "above"}},
        "time_filter": {"category": BlockCategory.FILTER, "label": "时间过滤器", "inputs": ["signal"], "outputs": ["filtered_signal"], "params": {"start_hour": 9, "end_hour": 15}},
        "volume_filter": {"category": BlockCategory.FILTER, "label": "成交量过滤", "inputs": ["signal"], "outputs": ["filtered_signal"], "params": {"min_volume_ratio": 1.5}},
        "trend_filter": {"category": BlockCategory.FILTER, "label": "趋势过滤", "inputs": ["signal", "trend"], "outputs": ["filtered_signal"]},
        "market_order": {"category": BlockCategory.ORDER, "label": "市价单", "inputs": ["signal"], "outputs": []},
        "limit_order": {"category": BlockCategory.ORDER, "label": "限价单", "inputs": ["signal", "price"], "outputs": []},
        "stop_order": {"category": BlockCategory.ORDER, "label": "止损单", "inputs": ["signal", "stop_price"], "outputs": []},
        "variable": {"category": BlockCategory.UTILITY, "label": "变量", "inputs": [], "outputs": ["value"], "params": {"name": "var1", "default_value": 0}},
        "constant": {"category": BlockCategory.UTILITY, "label": "常量", "inputs": [], "outputs": ["value"], "params": {"value": 0}},
        "math_op": {"category": BlockCategory.UTILITY, "label": "数学运算", "inputs": ["a", "b"], "outputs": ["result"], "params": {"operation": "add"}},
        "logic_op": {"category": BlockCategory.UTILITY, "label": "逻辑运算", "inputs": ["a", "b"], "outputs": ["result"], "params": {"operation": "and"}},
    }

    def __init__(self) -> None:
        self._canvases: dict[str, Canvas] = {}

    # ── VIS-01: Block Palette ────────────────────────────────────────────────

    def get_block_palette(self) -> dict[str, dict]:
        """Get all available blocks in the palette (VIS-01)."""
        return self.BLOCK_DEFINITIONS

    def get_blocks_by_category(self, category: BlockCategory) -> list[dict]:
        """Get blocks filtered by category."""
        return {k: v for k, v in self.BLOCK_DEFINITIONS.items() if v["category"] == category}

    # ── VIS-02: Canvas Management ──────────────────────────────────────────

    def create_canvas(self, user_id: str, name: str) -> Canvas:
        """Create a new canvas (VIS-02)."""
        canvas = Canvas(
            canvas_id=f"canvas:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            name=name,
        )
        self._canvases[canvas.canvas_id] = canvas
        return canvas

    def get_canvas(self, canvas_id: str) -> Canvas | None:
        """Get a canvas by ID."""
        return self._canvases.get(canvas_id)

    def list_user_canvases(self, user_id: str) -> list[Canvas]:
        """List all canvases for a user."""
        return [c for c in self._canvases.values() if c.user_id == user_id]

    def update_canvas_name(self, canvas_id: str, name: str) -> bool:
        """Update canvas name."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return False
        canvas.name = name
        canvas.updated_at = datetime.now(timezone.utc)
        return True

    # ── VIS-03: Block & Connection Management ─────────────────────────────

    def add_block(
        self,
        canvas_id: str,
        block_type: BlockType,
        x: float,
        y: float,
        parameters: dict[str, Any] | None = None,
    ) -> Block | None:
        """Add a block to a canvas (VIS-02)."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None

        defn = self.BLOCK_DEFINITIONS.get(block_type.value, {})
        block = Block(
            block_id=f"blk:{uuid.uuid4().hex[:12]}",
            block_type=block_type,
            category=defn.get("category", BlockCategory.UTILITY),
            label=defn.get("label", block_type.value),
            x=x,
            y=y,
            parameters=parameters or defn.get("params", {}),
            outputs=defn.get("outputs", []),
            inputs=defn.get("inputs", []),
        )
        canvas.blocks.append(block)
        canvas.updated_at = datetime.now(timezone.utc)
        return block

    def update_block_position(self, canvas_id: str, block_id: str, x: float, y: float) -> bool:
        """Update block position on canvas."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return False
        for block in canvas.blocks:
            if block.block_id == block_id:
                block.x = x
                block.y = y
                canvas.updated_at = datetime.now(timezone.utc)
                return True
        return False

    def update_block_parameters(self, canvas_id: str, block_id: str, parameters: dict[str, Any]) -> bool:
        """Update block parameters."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return False
        for block in canvas.blocks:
            if block.block_id == block_id:
                block.parameters.update(parameters)
                canvas.updated_at = datetime.now(timezone.utc)
                return True
        return False

    def remove_block(self, canvas_id: str, block_id: str) -> bool:
        """Remove a block from canvas."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return False
        canvas.blocks = [b for b in canvas.blocks if b.block_id != block_id]
        # Also remove connections to/from this block
        canvas.connections = [c for c in canvas.connections if c.source_block_id != block_id and c.target_block_id != block_id]
        canvas.updated_at = datetime.now(timezone.utc)
        return True

    def add_connection(
        self,
        canvas_id: str,
        source_block_id: str,
        source_port: str,
        target_block_id: str,
        target_port: str,
    ) -> Connection | None:
        """Add a connection between blocks (VIS-03)."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None

        # Validate blocks exist
        source_block = next((b for b in canvas.blocks if b.block_id == source_block_id), None)
        target_block = next((b for b in canvas.blocks if b.block_id == target_block_id), None)
        if not source_block or not target_block:
            return None

        # Validate ports exist
        if source_port not in source_block.outputs or target_port not in target_block.inputs:
            return None

        # Check for duplicate connection
        for conn in canvas.connections:
            if conn.target_block_id == target_block_id and conn.target_port == target_port:
                return None  # already connected

        connection = Connection(
            connection_id=f"conn:{uuid.uuid4().hex[:12]}",
            source_block_id=source_block_id,
            source_port=source_port,
            target_block_id=target_block_id,
            target_port=target_port,
        )
        canvas.connections.append(connection)
        canvas.updated_at = datetime.now(timezone.utc)
        return connection

    def remove_connection(self, canvas_id: str, connection_id: str) -> bool:
        """Remove a connection."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return False
        canvas.connections = [c for c in canvas.connections if c.connection_id != connection_id]
        canvas.updated_at = datetime.now(timezone.utc)
        return True

    # ── VIS-04: Code Generation ───────────────────────────────────────────

    def generate_code(self, canvas_id: str) -> str:
        """Generate Python code from canvas (VIS-04)."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return ""

        lines = [
            "# Generated by Visual Strategy Editor",
            f"# Canvas: {canvas.name}",
            f"# Generated at: {datetime.now(timezone.utc).isoformat()}",
            "",
            "from quant_exchange.strategy import BaseStrategy",
            "from quant_exchange.core.models import Direction",
            "",
            "class GeneratedStrategy(BaseStrategy):",
            "    def __init__(self):",
            "        super().__init__(strategy_id='generated')",
            "        self.initialized = False",
            "",
            "    def on_init(self):",
            "        # Register indicators",
        ]

        # Generate indicator registrations
        for block in canvas.blocks:
            if block.category == BlockCategory.INDICATOR:
                params = ", ".join(f"{k}={v}" for k, v in block.parameters.items())
                lines.append(f"        self.register_indicator('{block.block_type.value}', {params})")

        lines.extend([
            "        self.initialized = True",
            "",
            "    def on_bar(self, bar):",
            "        # Signal generation",
        ])

        # Generate signal logic
        for block in canvas.blocks:
            if block.category == BlockCategory.SIGNAL:
                lines.append(f"        signal_{block.block_id[-6:]} = self.get_signal('{block.block_type.value}')")

        # Generate order logic
        for block in canvas.blocks:
            if block.category == BlockCategory.ORDER:
                if block.block_type == BlockType.MARKET_ORDER:
                    lines.append(f"        self.submit_market_order(Direction.LONG, quantity=1.0)  # {block.label}")

        lines.append("")
        strategy_code = "\n".join(lines)
        canvas.strategy_code = strategy_code
        return strategy_code

    # ── VIS-05: Validation ─────────────────────────────────────────────────

    def validate_canvas(self, canvas_id: str) -> dict[str, Any]:
        """Validate a canvas for errors (VIS-05)."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return {"valid": False, "errors": ["Canvas not found"]}

        errors: list[str] = []

        # Check for source blocks
        source_blocks = [b for b in canvas.blocks if b.category == BlockCategory.SOURCE]
        if not source_blocks:
            errors.append("Canvas must have at least one data source block")

        # Check for order blocks
        order_blocks = [b for b in canvas.blocks if b.category == BlockCategory.ORDER]
        if not order_blocks:
            errors.append("Canvas must have at least one order block")

        # Check all connections are valid
        for conn in canvas.connections:
            source = next((b for b in canvas.blocks if b.block_id == conn.source_block_id), None)
            target = next((b for b in canvas.blocks if b.block_id == conn.target_block_id), None)
            if not source or not target:
                errors.append(f"Connection {conn.connection_id} references missing block")
            elif conn.source_port not in (source.outputs or []):
                errors.append(f"Source block {source.block_id} does not have port '{conn.source_port}'")
            elif conn.target_port not in (target.inputs or []):
                errors.append(f"Target block {target.block_id} does not have port '{conn.target_port}'")

        # Check for disconnected required inputs
        for block in canvas.blocks:
            if block.category == BlockCategory.SIGNAL or block.category == BlockCategory.ORDER:
                connected_inputs = {c.target_port for c in canvas.connections if c.target_block_id == block.block_id}
                for inp in block.inputs:
                    if inp not in connected_inputs:
                        errors.append(f"Block '{block.label}' has disconnected required input '{inp}'")

        canvas.is_valid = len(errors) == 0
        canvas.validation_errors = errors

        return {"valid": len(errors) == 0, "errors": errors}
