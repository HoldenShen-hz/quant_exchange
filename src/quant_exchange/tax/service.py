"""Tax compliance reporting service (TAX-01~TAX-04).

Covers:
- TAX-01: Trade lot tracking and cost basis calculation
- TAX-02: Capital gains/losses computation (FIFO/LIFO/HIFO)
- TAX-03: Tax reporting (1099, annual tax summary)
- TAX-04: Wash sale detection and adjustment
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


class CostBasisMethod(str, Enum):
    FIFO = "fifo"  # First In, First Out
    LIFO = "lifo"  # Last In, First Out
    HIFO = "hifo"  # Highest In, First Out
    SPECIFIC_ID = "specific_id"  # Specific identification


class GainType(str, Enum):
    SHORT_TERM = "short_term"  # held < 1 year
    LONG_TERM = "long_term"  # held >= 1 year


@dataclass(slots=True)
class TaxLot:
    """A tax lot for a position (one purchase)."""

    lot_id: str
    instrument_id: str
    quantity: float
    purchase_price: float  # per unit
    purchase_date: datetime
    purchase_commission: float = 0.0
    remaining_quantity: float = 0.0  # for partially closed lots


@dataclass(slots=True)
class CapitalGainEvent:
    """A realized capital gain or loss event."""

    event_id: str
    instrument_id: str
    closing_date: datetime
    quantity: float
    proceeds: float  # total sale proceeds
    cost_basis: float  # total cost basis
    gain: float  # proceeds - cost_basis
    gain_type: GainType
    holding_period_days: int
    is_wash_sale: bool = False
    wash_sale_adjustment: float = 0.0
    lot_ids: list[str] = field(default_factory=list)  # lots that were closed
    tax_year: int = 0


@dataclass(slots=True)
class TaxReport:
    """An annual tax report."""

    report_id: str
    user_id: str
    tax_year: int
    short_term_gains: float = 0.0
    short_term_losses: float = 0.0
    long_term_gains: float = 0.0
    long_term_losses: float = 0.0
    net_gains: float = 0.0
    total_commissions: float = 0.0
    wash_sale_adjustments: float = 0.0
    total_trades: int = 0
    generated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class TaxService:
    """Tax compliance reporting service (TAX-01~TAX-04)."""

    SHORT_TERM_DAYS = 365  # positions held < 1 year = short term

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._lots: dict[str, TaxLot] = {}
        self._gain_events: list[CapitalGainEvent] = []
        self._reports: dict[str, TaxReport] = {}

    # ── TAX-01: Lot Tracking ───────────────────────────────────────────────

    def add_lot(
        self,
        instrument_id: str,
        quantity: float,
        purchase_price: float,
        purchase_date: datetime,
        purchase_commission: float = 0.0,
    ) -> TaxLot:
        """Add a new tax lot (TAX-01)."""
        lot = TaxLot(
            lot_id=f"lot:{uuid.uuid4().hex[:12]}",
            instrument_id=instrument_id,
            quantity=quantity,
            purchase_price=purchase_price,
            purchase_date=purchase_date,
            purchase_commission=purchase_commission,
            remaining_quantity=quantity,
        )
        self._lots[lot.lot_id] = lot
        return lot

    def get_open_lots(self, instrument_id: str) -> list[TaxLot]:
        """Get all open (unsettled) tax lots for an instrument."""
        return [l for l in self._lots.values() if l.instrument_id == instrument_id and l.remaining_quantity > 0]

    # ── TAX-02: Capital Gains ─────────────────────────────────────────────

    def calculate_gain(
        self,
        instrument_id: str,
        quantity: float,
        sale_price: float,
        sale_date: datetime,
        sale_commission: float = 0.0,
        method: CostBasisMethod = CostBasisMethod.FIFO,
    ) -> CapitalGainEvent:
        """Calculate capital gain for a sale (TAX-02)."""
        open_lots = self.get_open_lots(instrument_id)
        if not open_lots:
            return CapitalGainEvent(
                event_id=f"gain:{uuid.uuid4().hex[:12]}",
                instrument_id=instrument_id,
                closing_date=sale_date,
                quantity=quantity,
                proceeds=sale_price * quantity - sale_commission,
                cost_basis=0.0,
                gain=0.0,
                gain_type=GainType.SHORT_TERM,
                holding_period_days=0,
            )

        # Sort lots by method
        if method == CostBasisMethod.FIFO:
            open_lots.sort(key=lambda l: l.purchase_date)
        elif method == CostBasisMethod.LIFO:
            open_lots.sort(key=lambda l: l.purchase_date, reverse=True)
        elif method == CostBasisMethod.HIFO:
            open_lots.sort(key=lambda l: l.purchase_price, reverse=True)

        total_proceeds = sale_price * quantity - sale_commission
        total_cost = 0.0
        total_quantity = quantity
        lot_ids: list[str] = []
        earliest_date = sale_date

        for lot in open_lots:
            if total_quantity <= 0:
                break
            close_qty = min(lot.remaining_quantity, total_quantity)
            lot_cost = close_qty * lot.purchase_price + (lot.purchase_commission / lot.quantity * close_qty if lot.quantity > 0 else 0)
            total_cost += lot_cost
            lot.remaining_quantity -= close_qty
            total_quantity -= close_qty
            lot_ids.append(lot.lot_id)
            if lot.purchase_date < earliest_date:
                earliest_date = lot.purchase_date

        gain = total_proceeds - total_cost
        holding_days = (sale_date - earliest_date).days
        gain_type = GainType.LONG_TERM if holding_days >= self.SHORT_TERM_DAYS else GainType.SHORT_TERM

        event = CapitalGainEvent(
            event_id=f"gain:{uuid.uuid4().hex[:12]}",
            instrument_id=instrument_id,
            closing_date=sale_date,
            quantity=quantity,
            proceeds=total_proceeds,
            cost_basis=total_cost,
            gain=gain,
            gain_type=gain_type,
            holding_period_days=holding_days,
            lot_ids=lot_ids,
            tax_year=sale_date.year,
        )
        self._gain_events.append(event)
        return event

    def get_gain_events(self, tax_year: int | None = None, user_id: str | None = None) -> list[CapitalGainEvent]:
        """Get all gain events."""
        events = list(self._gain_events)
        if tax_year:
            events = [e for e in events if e.tax_year == tax_year]
        return events

    # ── TAX-03: Tax Reports ───────────────────────────────────────────────

    def generate_tax_report(self, user_id: str, tax_year: int) -> TaxReport:
        """Generate annual tax report (TAX-03)."""
        events = [e for e in self._gain_events if e.tax_year == tax_year]

        short_term_gains = sum(e.gain for e in events if e.gain_type == GainType.SHORT_TERM and e.gain > 0)
        short_term_losses = sum(abs(e.gain) for e in events if e.gain_type == GainType.SHORT_TERM and e.gain < 0)
        long_term_gains = sum(e.gain for e in events if e.gain_type == GainType.LONG_TERM and e.gain > 0)
        long_term_losses = sum(abs(e.gain) for e in events if e.gain_type == GainType.LONG_TERM and e.gain < 0)

        net_gains = (short_term_gains - short_term_losses) + (long_term_gains - long_term_losses)

        report = TaxReport(
            report_id=f"taxr:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            tax_year=tax_year,
            short_term_gains=short_term_gains,
            short_term_losses=short_term_losses,
            long_term_gains=long_term_gains,
            long_term_losses=long_term_losses,
            net_gains=net_gains,
            wash_sale_adjustments=sum(e.wash_sale_adjustment for e in events),
            total_trades=len(events),
        )
        self._reports[report.report_id] = report
        return report

    def get_report(self, report_id: str) -> TaxReport | None:
        """Get a tax report by ID."""
        return self._reports.get(report_id)

    def export_1099_summary(self, user_id: str, tax_year: int) -> dict[str, Any]:
        """Generate 1099 summary data (TAX-03)."""
        report = self.generate_tax_report(user_id, tax_year)
        return {
            "user_id": user_id,
            "tax_year": tax_year,
            "short_term_gain_loss": report.short_term_gains - report.short_term_losses,
            "long_term_gain_loss": report.long_term_gains - report.long_term_losses,
            "total_gain_loss": report.net_gains,
            "wash_sale_adjustments": report.wash_sale_adjustments,
            "total_transactions": report.total_trades,
        }

    # ── TAX-04: Wash Sale Detection ────────────────────────────────────────

    def detect_wash_sales(self, tax_year: int) -> list[CapitalGainEvent]:
        """Detect wash sale violations (TAX-04).

        A wash sale occurs when you sell at a loss and repurchase
        the same or substantially identical security within 30 days
        before or after the sale.
        """
        events = [e for e in self._gain_events if e.tax_year == tax_year and e.gain < 0]
        wash_sales: list[CapitalGainEvent] = []

        for event in events:
            loss = abs(event.gain)
            # Find repurchases within 30 days before or after
            repurchase_window_start = event.closing_date.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=30)
            repurchase_window_end = event.closing_date.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=30)

            # Find lots purchased in the wash sale window
            repurchases = [l for l in self._lots.values() if l.instrument_id == event.instrument_id and repurchase_window_start <= l.purchase_date <= repurchase_window_end]

            if repurchases:
                total_repurchase_value = sum(l.remaining_quantity * l.purchase_price for l in repurchases)
                disallowed = min(loss, total_repurchase_value)
                event.is_wash_sale = True
                event.wash_sale_adjustment = disallowed
                wash_sales.append(event)

        return wash_sales
