"""Visual strategy editor service (VIS-01 ~ VIS-05).

Covers:
- Node-based strategy building canvas
- Visual connections between nodes
- Python/QuantScript code export
- Strategy logic validation
- Canvas state persistence
"""

from __future__ import annotations

import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class NodeCategory(str, Enum):
    SOURCE = "source"       # Data sources (price, volume, etc.)
    INDICATOR = "indicator" # Technical indicators
    OPERATOR = "operator"   # Math/logic operators
    SIGNAL = "signal"       # Signal generation
    FILTER = "filter"      # Filters and conditions
    ORDER = "order"        # Order execution
    UTILITY = "utility"    # Utilities and constants


class NodeType(str, Enum):
    # Source nodes
    PRICE_DATA = "price_data"
    VOLUME_DATA = "volume_data"
    FUNDAMENTAL_DATA = "fundamental_data"
    # Indicator nodes
    MA = "ma"
    EMA = "ema"
    RSI = "rsi"
    MACD = "macd"
    BOLLINGER = "bollinger"
    ATR = "atr"
    STOCH = "stoch"
    VWAP = "vwap"
    # Operator nodes
    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"
    COMPARE = "compare"
    AND = "and"
    OR = "or"
    NOT = "not"
    CROSSOVER = "crossover"
    CROSSUNDER = "crossunder"
    # Signal nodes
    BUY_SIGNAL = "buy_signal"
    SELL_SIGNAL = "sell_signal"
    EXIT_SIGNAL = "exit_signal"
    # Filter nodes
    THRESHOLD = "threshold"
    TIME_FILTER = "time_filter"
    VOLUME_FILTER = "volume_filter"
    # Order nodes
    MARKET_ORDER = "market_order"
    LIMIT_ORDER = "limit_order"
    STOP_ORDER = "stop_order"
    # Utility nodes
    CONSTANT = "constant"
    VARIABLE = "variable"
    LAG = "lag"


@dataclass(slots=True)
class PortDefinition:
    """Definition of an input or output port on a node."""

    port_id: str
    name: str
    data_type: str = "float"  # float, bool, series, dataframe
    direction: str = "input"   # input, output
    optional: bool = False
    default_value: Any = None


@dataclass(slots=True)
class NodePosition:
    """Position of a node on the canvas."""

    x: float
    y: float
    width: float = 200.0
    height: float = 100.0


@dataclass(slots=True)
class VisualNode:
    """A node on the visual editor canvas."""

    node_id: str
    node_type: NodeType
    category: NodeCategory
    label: str
    inputs: tuple[PortDefinition, ...] = field(default_factory=tuple)
    outputs: tuple[PortDefinition, ...] = field(default_factory=tuple)
    parameters: dict[str, Any] = field(default_factory=dict)  # e.g., period=14 for MA
    position: NodePosition | None = None
    collapsed: bool = False
    color: str = "#2196F3"


@dataclass(slots=True)
class Connection:
    """A connection between two nodes."""

    connection_id: str
    source_node_id: str
    source_port_id: str
    target_node_id: str
    target_port_id: str
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class CanvasState:
    """State of the visual editor canvas."""

    canvas_id: str
    user_id: str
    name: str
    nodes: tuple[VisualNode, ...] = field(default_factory=tuple)
    connections: tuple[Connection, ...] = field(default_factory=tuple)
    viewport_x: float = 0.0
    viewport_y: float = 0.0
    zoom_level: float = 1.0
    grid_size: int = 20
    snap_to_grid: bool = True
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class ValidationError:
    """An error found during strategy validation."""

    error_id: str
    message: str
    severity: str = "error"  # error, warning, info
    node_id: str | None = None
    connection_id: str | None = None
    suggestion: str = ""


@dataclass(slots=True)
class ValidationResult:
    """Result of strategy validation."""

    is_valid: bool
    errors: tuple[ValidationError, ...] = field(default_factory=tuple)
    warnings: tuple[str, ...] = field(default_factory=tuple)


# ─────────────────────────────────────────────────────────────────────────────
# Node Definitions Registry
# ─────────────────────────────────────────────────────────────────────────────

NODE_DEFINITIONS: dict[NodeType, dict[str, Any]] = {
    # Source nodes
    NodeType.PRICE_DATA: {
        "category": NodeCategory.SOURCE,
        "label": "Price Data",
        "outputs": [("price", "series", False)],
        "parameters": {"field": "close"},
        "color": "#4CAF50",
    },
    NodeType.VOLUME_DATA: {
        "category": NodeCategory.SOURCE,
        "label": "Volume Data",
        "outputs": [("volume", "series", False)],
        "color": "#4CAF50",
    },
    # Indicator nodes
    NodeType.MA: {
        "category": NodeCategory.INDICATOR,
        "label": "Moving Average",
        "inputs": [("price", "series", False)],
        "outputs": [("ma", "series", False)],
        "parameters": {"period": 20},
        "color": "#2196F3",
    },
    NodeType.EMA: {
        "category": NodeCategory.INDICATOR,
        "label": "Exponential MA",
        "inputs": [("price", "series", False)],
        "outputs": [("ema", "series", False)],
        "parameters": {"period": 12},
        "color": "#2196F3",
    },
    NodeType.RSI: {
        "category": NodeCategory.INDICATOR,
        "label": "RSI",
        "inputs": [("price", "series", False)],
        "outputs": [("rsi", "series", False)],
        "parameters": {"period": 14},
        "color": "#2196F3",
    },
    NodeType.MACD: {
        "category": NodeCategory.INDICATOR,
        "label": "MACD",
        "inputs": [("price", "series", False)],
        "outputs": [("macd", "series", False), ("signal", "series", False), ("hist", "series", False)],
        "parameters": {"fast": 12, "slow": 26, "signal": 9},
        "color": "#2196F3",
    },
    NodeType.BOLLINGER: {
        "category": NodeCategory.INDICATOR,
        "label": "Bollinger Bands",
        "inputs": [("price", "series", False)],
        "outputs": [("upper", "series", False), ("middle", "series", False), ("lower", "series", False)],
        "parameters": {"period": 20, "std_dev": 2.0},
        "color": "#2196F3",
    },
    NodeType.ATR: {
        "category": NodeCategory.INDICATOR,
        "label": "ATR",
        "inputs": [("high", "series", False), ("low", "series", False), ("close", "series", False)],
        "outputs": [("atr", "series", False)],
        "parameters": {"period": 14},
        "color": "#2196F3",
    },
    # Operator nodes
    NodeType.ADD: {
        "category": NodeCategory.OPERATOR,
        "label": "Add",
        "inputs": [("a", "float", False), ("b", "float", False)],
        "outputs": [("result", "float", False)],
        "color": "#FF9800",
    },
    NodeType.SUBTRACT: {
        "category": NodeCategory.OPERATOR,
        "label": "Subtract",
        "inputs": [("a", "float", False), ("b", "float", False)],
        "outputs": [("result", "float", False)],
        "color": "#FF9800",
    },
    NodeType.MULTIPLY: {
        "category": NodeCategory.OPERATOR,
        "label": "Multiply",
        "inputs": [("a", "float", False), ("b", "float", False)],
        "outputs": [("result", "float", False)],
        "color": "#FF9800",
    },
    NodeType.DIVIDE: {
        "category": NodeCategory.OPERATOR,
        "label": "Divide",
        "inputs": [("a", "float", False), ("b", "float", False)],
        "outputs": [("result", "float", False)],
        "color": "#FF9800",
    },
    NodeType.COMPARE: {
        "category": NodeCategory.OPERATOR,
        "label": "Compare",
        "inputs": [("a", "float", False), ("b", "float", False)],
        "outputs": [("result", "bool", False)],
        "parameters": {"operator": ">"},
        "color": "#FF9800",
    },
    NodeType.CROSSOVER: {
        "category": NodeCategory.OPERATOR,
        "label": "Crossover",
        "inputs": [("a", "series", False), ("b", "series", False)],
        "outputs": [("result", "bool", False)],
        "color": "#FF9800",
    },
    NodeType.CROSSUNDER: {
        "category": NodeCategory.OPERATOR,
        "label": "Crossunder",
        "inputs": [("a", "series", False), ("b", "series", False)],
        "outputs": [("result", "bool", False)],
        "color": "#FF9800",
    },
    # Signal nodes
    NodeType.BUY_SIGNAL: {
        "category": NodeCategory.SIGNAL,
        "label": "Buy Signal",
        "inputs": [("condition", "bool", False)],
        "outputs": [],
        "color": "#4CAF50",
    },
    NodeType.SELL_SIGNAL: {
        "category": NodeCategory.SIGNAL,
        "label": "Sell Signal",
        "inputs": [("condition", "bool", False)],
        "outputs": [],
        "color": "#F44336",
    },
    NodeType.EXIT_SIGNAL: {
        "category": NodeCategory.SIGNAL,
        "label": "Exit Signal",
        "inputs": [("condition", "bool", False)],
        "outputs": [],
        "color": "#9C27B0",
    },
    # Threshold
    NodeType.THRESHOLD: {
        "category": NodeCategory.FILTER,
        "label": "Threshold",
        "inputs": [("value", "float", False)],
        "outputs": [("pass", "bool", False)],
        "parameters": {"threshold": 0, "direction": "above"},
        "color": "#607D8B",
    },
    # Order nodes
    NodeType.MARKET_ORDER: {
        "category": NodeCategory.ORDER,
        "label": "Market Order",
        "inputs": [("signal", "bool", False), ("size", "float", False)],
        "outputs": [],
        "parameters": {"quantity": 100},
        "color": "#E91E63",
    },
    # Utility nodes
    NodeType.CONSTANT: {
        "category": NodeCategory.UTILITY,
        "label": "Constant",
        "inputs": [],
        "outputs": [("value", "float", False)],
        "parameters": {"value": 0},
        "color": "#795548",
    },
}


def _create_ports_from_def(definition: dict[str, Any], direction: str) -> tuple[PortDefinition, ...]:
    """Create PortDefinitions from node definition."""
    ports = []
    port_list_key = f"{direction}s"
    for port_data in definition.get(port_list_key, []):
        if isinstance(port_data, (list, tuple)):
            name, dtype, optional = port_data[0], port_data[1], len(port_data) > 2 and port_data[2]
            port_id = f"{direction}_{name}"
            ports.append(PortDefinition(
                port_id=port_id,
                name=name,
                data_type=dtype,
                direction=direction,
                optional=optional,
            ))
    return tuple(ports)


# ─────────────────────────────────────────────────────────────────────────────
# Visual Editor Service
# ─────────────────────────────────────────────────────────────────────────────

class VisualEditorService:
    """Visual strategy editor service (VIS-01 ~ VIS-05).

    Provides:
    - Node-based strategy building canvas
    - Visual connections between nodes
    - Python/QuantScript code export
    - Strategy logic validation
    - Canvas state persistence
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._canvases: dict[str, CanvasState] = {}
        self._nodes: dict[str, VisualNode] = {}
        self._connections: dict[str, Connection] = {}

    # ── Canvas Management ──────────────────────────────────────────────────

    def create_canvas(
        self,
        user_id: str,
        name: str,
    ) -> CanvasState:
        """Create a new visual editor canvas."""
        canvas_id = f"canvas:{uuid.uuid4().hex[:12]}"
        canvas = CanvasState(
            canvas_id=canvas_id,
            user_id=user_id,
            name=name,
        )
        self._canvases[canvas_id] = canvas
        return canvas

    def get_canvas(self, canvas_id: str) -> CanvasState | None:
        """Get a canvas by ID."""
        return self._canvases.get(canvas_id)

    def get_user_canvases(self, user_id: str) -> list[CanvasState]:
        """Get all canvases for a user."""
        return [c for c in self._canvases.values() if c.user_id == user_id]

    def update_canvas_viewport(
        self,
        canvas_id: str,
        viewport_x: float | None = None,
        viewport_y: float | None = None,
        zoom_level: float | None = None,
    ) -> CanvasState | None:
        """Update canvas viewport position and zoom."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None
        if viewport_x is not None:
            canvas.viewport_x = viewport_x
        if viewport_y is not None:
            canvas.viewport_y = viewport_y
        if zoom_level is not None:
            canvas.zoom_level = max(0.1, min(zoom_level, 3.0))
        canvas.updated_at = _now()
        return canvas

    def delete_canvas(self, canvas_id: str, user_id: str) -> bool:
        """Delete a canvas (only owner)."""
        canvas = self._canvases.get(canvas_id)
        if not canvas or canvas.user_id != user_id:
            return False
        del self._canvases[canvas_id]
        return True

    # ── Node Management ────────────────────────────────────────────────────

    def add_node(
        self,
        canvas_id: str,
        node_type: NodeType,
        position: tuple[float, float],
        label: str = "",
        parameters: dict[str, Any] | None = None,
    ) -> VisualNode | None:
        """Add a node to a canvas."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None

        definition = NODE_DEFINITIONS.get(node_type, {})
        node_id = f"node:{uuid.uuid4().hex[:12]}"

        inputs = _create_ports_from_def(definition, "input")
        outputs = _create_ports_from_def(definition, "output")

        node = VisualNode(
            node_id=node_id,
            node_type=node_type,
            category=NodeCategory(definition.get("category", "utility")),
            label=label or definition.get("label", node_type.value),
            inputs=inputs,
            outputs=outputs,
            parameters=parameters or definition.get("parameters", {}),
            position=NodePosition(x=position[0], y=position[1]),
            color=definition.get("color", "#9E9E9E"),
        )

        self._nodes[node_id] = node
        canvas.nodes = canvas.nodes + (node,)
        canvas.updated_at = _now()
        return node

    def get_node(self, node_id: str) -> VisualNode | None:
        """Get a node by ID."""
        return self._nodes.get(node_id)

    def update_node(
        self,
        node_id: str,
        label: str | None = None,
        parameters: dict[str, Any] | None = None,
        position: tuple[float, float] | None = None,
        collapsed: bool | None = None,
    ) -> VisualNode | None:
        """Update a node's properties."""
        node = self._nodes.get(node_id)
        if not node:
            return None
        if label is not None:
            node.label = label
        if parameters is not None:
            node.parameters.update(parameters)
        if position is not None:
            node.position = NodePosition(x=position[0], y=position[1])
        if collapsed is not None:
            node.collapsed = collapsed
        return node

    def remove_node(self, canvas_id: str, node_id: str) -> bool:
        """Remove a node from canvas and delete its connections."""
        canvas = self._canvases.get(canvas_id)
        node = self._nodes.get(node_id)
        if not canvas or not node:
            return False

        # Remove connections involving this node
        canvas.connections = tuple(
            c for c in canvas.connections
            if c.source_node_id != node_id and c.target_node_id != node_id
        )
        self._connections = {
            cid: c for cid, c in self._connections.items()
            if c.source_node_id != node_id and c.target_node_id != node_id
        }

        # Remove node from canvas
        canvas.nodes = tuple(n for n in canvas.nodes if n.node_id != node_id)
        del self._nodes[node_id]
        canvas.updated_at = _now()
        return True

    # ── Connection Management ─────────────────────────────────────────────

    def add_connection(
        self,
        canvas_id: str,
        source_node_id: str,
        source_port_id: str,
        target_node_id: str,
        target_port_id: str,
    ) -> Connection | None:
        """Add a connection between two nodes."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None

        source_node = self._nodes.get(source_node_id)
        target_node = self._nodes.get(target_node_id)
        if not source_node or not target_node:
            return None

        # Validate port types (simplified)
        source_port = next((p for p in source_node.outputs if p.port_id == source_port_id), None)
        target_port = next((p for p in target_node.inputs if p.port_id == target_port_id), None)
        if not source_port or not target_port:
            return None

        # Check for existing connection to target port
        existing = next(
            (c for c in canvas.connections if c.target_node_id == target_node_id and c.target_port_id == target_port_id),
            None
        )
        if existing:
            # Replace existing connection
            canvas.connections = tuple(c for c in canvas.connections if c.connection_id != existing.connection_id)

        connection_id = f"conn:{uuid.uuid4().hex[:12]}"
        connection = Connection(
            connection_id=connection_id,
            source_node_id=source_node_id,
            source_port_id=source_port_id,
            target_node_id=target_node_id,
            target_port_id=target_port_id,
        )
        self._connections[connection_id] = connection
        canvas.connections = canvas.connections + (connection,)
        canvas.updated_at = _now()
        return connection

    def remove_connection(self, canvas_id: str, connection_id: str) -> bool:
        """Remove a connection."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return False
        canvas.connections = tuple(c for c in canvas.connections if c.connection_id != connection_id)
        if connection_id in self._connections:
            del self._connections[connection_id]
        canvas.updated_at = _now()
        return True

    def get_node_connections(self, canvas_id: str, node_id: str) -> list[Connection]:
        """Get all connections for a node."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return []
        return [c for c in canvas.connections if c.source_node_id == node_id or c.target_node_id == node_id]

    # ── Validation ────────────────────────────────────────────────────────

    def validate_canvas(self, canvas_id: str) -> ValidationResult:
        """Validate the canvas for errors."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return ValidationResult(is_valid=False, errors=(ValidationError(error_id="nocanvas", message="Canvas not found"),))

        errors: list[ValidationError] = []
        warnings: list[str] = []

        # Check for unconnected required inputs
        for node in canvas.nodes:
            for inp in node.inputs:
                if inp.optional:
                    continue
                connected = any(
                    c.target_node_id == node.node_id and c.target_port_id == inp.port_id
                    for c in canvas.connections
                )
                if not connected:
                    # Check if there's a default value or if it's a SOURCE node
                    if node.category != NodeCategory.SOURCE and inp.default_value is None:
                        errors.append(ValidationError(
                            error_id=f"unconnected-{node.node_id}-{inp.port_id}",
                            severity="error",
                            node_id=node.node_id,
                            message=f"Unconnected required input '{inp.name}' on node '{node.label}'",
                            suggestion="Connect this input to another node's output or mark it as optional",
                        ))

        # Check for cycles (simplified - just detect direct self-loops)
        for conn in canvas.connections:
            if conn.source_node_id == conn.target_node_id:
                errors.append(ValidationError(
                    error_id=f"selfloop-{conn.connection_id}",
                    severity="error",
                    connection_id=conn.connection_id,
                    message="Self-loop detected",
                    suggestion="Remove the connection from a node to itself",
                ))

        # Check for signals without connections to order nodes
        signal_nodes = [n for n in canvas.nodes if n.category == NodeCategory.SIGNAL]
        order_nodes = [n for n in canvas.nodes if n.category == NodeCategory.ORDER]
        if signal_nodes and not order_nodes:
            warnings.append("Strategy has signal nodes but no order execution nodes")

        # Check for orphaned nodes (no connections at all)
        for node in canvas.nodes:
            has_connection = any(
                c.source_node_id == node.node_id or c.target_node_id == node.node_id
                for c in canvas.connections
            )
            if not has_connection and node.category != NodeCategory.SOURCE:
                warnings.append(f"Node '{node.label}' has no connections")

        return ValidationResult(
            is_valid=len([e for e in errors if e.severity == "error"]) == 0,
            errors=tuple(errors),
            warnings=tuple(warnings),
        )

    # ── Code Export ───────────────────────────────────────────────────────

    def export_to_python(self, canvas_id: str) -> str | None:
        """Export canvas to Python code."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None

        lines = [
            '"""Auto-generated strategy from visual editor."""',
            "",
            "from quant_exchange import Strategy, DataSource",
            "",
            "",
            f"class {canvas.name.replace(' ', '').replace('-', '')}Strategy(Strategy):",
            "    def __init__(self):",
            f'        self.name = "{canvas.name}"',
            "        self.signals = []",
            "        super().__init__()",
            "",
            "    def on_bar(self, bar):",
            "        # Signal generation logic",
            "        pass",
            "",
        ]

        # Generate node execution order (topological sort - simplified)
        node_map = {n.node_id: n for n in canvas.nodes}
        executed: set[str] = set()

        def emit_node(node: VisualNode) -> list[str]:
            if node.node_id in executed:
                return []
            executed.add(node.node_id)
            node_lines = [f"        # Node: {node.label} ({node.node_type.value})"]

            # Emit dependencies first
            for conn in canvas.connections:
                if conn.target_node_id == node.node_id:
                    src = node_map.get(conn.source_node_id)
                    if src and src.node_id not in executed:
                        node_lines.extend(emit_node(src))

            # Generate code based on node type
            if node.node_type == NodeType.MA:
                period = node.parameters.get("period", 20)
                node_lines.append(f"        ma_{node.node_id[-6:]} = self.compute_ma(bar.close, {period})")
            elif node.node_type == NodeType.EMA:
                period = node.parameters.get("period", 12)
                node_lines.append(f"        ema_{node.node_id[-6:]} = self.compute_ema(bar.close, {period})")
            elif node.node_type == NodeType.RSI:
                period = node.parameters.get("period", 14)
                node_lines.append(f"        rsi_{node.node_id[-6:]} = self.compute_rsi(bar.close, {period})")
            elif node.node_type == NodeType.MACD:
                node_lines.append(f"        macd_{node.node_id[-6:]}, signal_{node.node_id[-6:]}, hist_{node.node_id[-6:]} = self.compute_macd(bar.close)")
            elif node.node_type == NodeType.CROSSOVER:
                node_lines.append(f"        # Crossover detection")
            elif node.node_type == NodeType.BUY_SIGNAL:
                node_lines.append(f"        # Buy signal condition")
            elif node.node_type == NodeType.SELL_SIGNAL:
                node_lines.append(f"        # Sell signal condition")
            elif node.node_type == NodeType.MARKET_ORDER:
                qty = node.parameters.get("quantity", 100)
                node_lines.append(f"        # Market order qty={qty}")

            return node_lines

        for node in canvas.nodes:
            if node.category == NodeCategory.SOURCE:
                lines.extend(emit_node(node))

        lines.append("")
        lines.append("    def get_signals(self):")
        lines.append("        return self.signals")

        return "\n".join(lines)

    def export_to_quant_script(self, canvas_id: str) -> str | None:
        """Export canvas to QuantScript DSL."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None

        lines = [
            f"# QuantScript strategy: {canvas.name}",
            f"# Generated: {_now()}",
            "",
        ]

        # Group nodes by category
        sources = [n for n in canvas.nodes if n.category == NodeCategory.SOURCE]
        indicators = [n for n in canvas.nodes if n.category == NodeCategory.INDICATOR]
        signals = [n for n in canvas.nodes if n.category == NodeCategory.SIGNAL]

        for src in sources:
            if src.node_type == NodeType.PRICE_DATA:
                lines.append(f"price = ref(close)")
            elif src.node_type == NodeType.VOLUME_DATA:
                lines.append(f"volume = ref(vol)")

        for ind in indicators:
            params_str = ", ".join(f"{k}={v}" for k, v in ind.parameters.items())
            if ind.node_type == NodeType.MA:
                lines.append(f"{ind.label.lower().replace(' ', '_')} = sma(price, {params_str})")
            elif ind.node_type == NodeType.EMA:
                lines.append(f"{ind.label.lower().replace(' ', '_')} = ema(price, {params_str})")
            elif ind.node_type == NodeType.RSI:
                lines.append(f"{ind.label.lower().replace(' ', '_')} = rsi(price, {params_str})")
            elif ind.node_type == NodeType.MACD:
                lines.append(f"{ind.label.lower().replace(' ', '_')} = macd(price, {params_str})")
            elif ind.node_type == NodeType.BOLLINGER:
                lines.append(f"{ind.label.lower().replace(' ', '_')} = bollinger(price, {params_str})")

        for sig in signals:
            if sig.node_type == NodeType.BUY_SIGNAL:
                lines.append("entry long when buy_condition")
                lines.append(f"    # {sig.label}")
            elif sig.node_type == NodeType.SELL_SIGNAL:
                lines.append("entry short when sell_condition")
                lines.append(f"    # {sig.label}")
            elif sig.node_type == NodeType.EXIT_SIGNAL:
                lines.append("exit when exit_condition")
                lines.append(f"    # {sig.label}")

        return "\n".join(lines)

    # ── Preset Templates ───────────────────────────────────────────────────

    def create_from_template(
        self,
        canvas_id: str,
        template_name: str,
    ) -> CanvasState | None:
        """Create a canvas from a preset template."""
        canvas = self._canvases.get(canvas_id)
        if not canvas:
            return None

        templates = {
            "ma_crossover": [
                (NodeType.PRICE_DATA, (100, 100)),
                (NodeType.MA, (300, 50), {"period": 20}),
                (NodeType.MA, (300, 150), {"period": 50}),
                (NodeType.CROSSOVER, (500, 100)),
                (NodeType.BUY_SIGNAL, (700, 50)),
                (NodeType.SELL_SIGNAL, (700, 150)),
                (NodeType.MARKET_ORDER, (900, 100)),
            ],
            "rsi_strategy": [
                (NodeType.PRICE_DATA, (100, 100)),
                (NodeType.RSI, (300, 100), {"period": 14}),
                (NodeType.THRESHOLD, (500, 50), {"threshold": 30, "direction": "below"}),
                (NodeType.THRESHOLD, (500, 150), {"threshold": 70, "direction": "above"}),
                (NodeType.BUY_SIGNAL, (700, 50)),
                (NodeType.SELL_SIGNAL, (700, 150)),
            ],
        }

        template = templates.get(template_name, [])
        for node_spec in template:
            node_type = node_spec[0]
            position = node_spec[1]
            params = node_spec[2] if len(node_spec) > 2 else None
            node = self.add_node(canvas_id, node_type, position, parameters=params)
            if node:
                # Auto-connect sequential nodes
                if len(canvas.nodes) > 1:
                    prev_node = canvas.nodes[-2]
                    src_port = prev_node.outputs[0] if prev_node.outputs else None
                    tgt_port = node.inputs[0] if node.inputs else None
                    if src_port and tgt_port:
                        self.add_connection(
                            canvas_id, prev_node.node_id, src_port.port_id,
                            node.node_id, tgt_port.port_id
                        )

        return canvas
