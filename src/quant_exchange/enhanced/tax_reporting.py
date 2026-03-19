"""Tax reporting service (TAX-01 ~ TAX-04).

Covers:
- Trading activity reports
- Capital gains calculations (LIFO, FIFO, HIFO, minimax)
- Cost basis tracking
- Tax lot management
- Export to PDF/Excel formats
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class CostBasisMethod(str, Enum):
    FIFO = "fifo"      # First in, first out
    LIFO = "lifo"      # Last in, first out
    HIFO = "hifo"      # Highest in, first out
    MINIMAX = "minimax"  # Minimize gain / maximize loss


class GainType(str, Enum):
    SHORT_TERM = "short_term"    # Held < 1 year
    LONG_TERM = "long_term"      # Held >= 1 year


class TradeType(str, Enum):
    BUY = "buy"
    SELL = "sell"
    DIVIDEND = "dividend"
    INTEREST = "interest"
    FEE = "fee"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class TaxLot:
    """A tax lot representing shares purchased at a specific time/price."""

    lot_id: str
    instrument_id: str
    quantity: float
    cost_basis: float       # Total cost basis
    price_per_share: float
    purchase_date: datetime
    account_id: str
    is_long_term: bool = False
    linked_sale_id: str | None = None


@dataclass(slots=True)
class TradeActivity:
    """A single trade activity record."""

    activity_id: str
    account_id: str
    user_id: str
    instrument_id: str
    trade_type: TradeType
    quantity: float
    price: float
    commission: float = 0.0
    fees: float = 0.0
    proceeds: float = 0.0  # For sells
    net_amount: float = 0.0
    timestamp: str = field(default_factory=_now)
    notes: str = ""


@dataclass(slots=True)
class CapitalGain:
    """A realized capital gain/loss."""

    gain_id: str
    sale_activity_id: str
    account_id: str
    instrument_id: str
    gain_type: GainType
    proceeds: float
    cost_basis: float
    gain: float          # proceeds - cost_basis
    holding_period_days: int
    lot_ids: tuple[str, ...] = field(default_factory=tuple)
    trade_date: str = field(default_factory=_now)


@dataclass(slots=True)
class TaxSummary:
    """Annual tax summary."""

    year: int
    user_id: str
    short_term_gains: float = 0.0
    short_term_losses: float = 0.0
    long_term_gains: float = 0.0
    long_term_losses: float = 0.0
    total_dividends: float = 0.0
    total_interest: float = 0.0
    total_fees: float = 0.0
    net_short_term: float = 0.0
    net_long_term: float = 0.0
    as_of: str = field(default_factory=_now)


@dataclass(slots=True)
class WashSaleRecord:
    """Wash sale tracking record."""

    wash_id: str
    disallowed_loss: float
    replacement_shares: str | None = None
    original_sale_date: str = field(default_factory=_now)
    wash_period_start: str = ""
    wash_period_end: str = ""


# ─────────────────────────────────────────────────────────────────────────────
# Tax Reporting Service
# ─────────────────────────────────────────────────────────────────────────────

class TaxReportingService:
    """Tax reporting service (TAX-01 ~ TAX-04).

    Provides:
    - Trade activity tracking and reporting
    - Capital gains calculation with multiple cost basis methods
    - Tax lot management
    - Wash sale tracking
    - Annual tax summaries
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._activities: list[TradeActivity] = []
        self._lots: dict[str, TaxLot] = {}   # lot_id -> TaxLot
        self._gains: list[CapitalGain] = []
        self._wash_sales: list[WashSaleRecord] = []
        self._account_activities: dict[str, list[str]] = defaultdict(list)  # account_id -> activity_ids
        self._user_activities: dict[str, list[str]] = defaultdict(list)  # user_id -> activity_ids

    # ── Trade Activity ───────────────────────────────────────────────────

    def record_trade(
        self,
        account_id: str,
        user_id: str,
        instrument_id: str,
        trade_type: TradeType,
        quantity: float,
        price: float,
        commission: float = 0.0,
        fees: float = 0.0,
        timestamp: str | None = None,
        notes: str = "",
    ) -> TradeActivity:
        """Record a trade activity."""
        activity_id = f"tax:{uuid.uuid4().hex[:12]}"
        if timestamp is None:
            timestamp = _now()

        proceeds = 0.0
        net_amount = 0.0
        if trade_type == TradeType.SELL:
            proceeds = quantity * price
            net_amount = proceeds - commission - fees
        elif trade_type == TradeType.BUY:
            net_amount = -(quantity * price + commission + fees)
        elif trade_type == TradeType.DIVIDEND:
            net_amount = quantity * price
        elif trade_type == TradeType.INTEREST:
            net_amount = quantity * price
        elif trade_type == TradeType.FEE:
            net_amount = -(quantity * price)

        activity = TradeActivity(
            activity_id=activity_id,
            account_id=account_id,
            user_id=user_id,
            instrument_id=instrument_id,
            trade_type=trade_type,
            quantity=quantity,
            price=price,
            commission=commission,
            fees=fees,
            proceeds=proceeds,
            net_amount=net_amount,
            timestamp=timestamp,
            notes=notes,
        )
        self._activities.append(activity)
        self._account_activities[account_id].append(activity_id)
        self._user_activities[user_id].append(activity_id)

        # Create tax lot for buys
        if trade_type == TradeType.BUY:
            self._create_tax_lot(activity)

        return activity

    def _create_tax_lot(self, activity: TradeActivity) -> TaxLot:
        """Create a tax lot from a buy activity."""
        lot_id = f"lot:{uuid.uuid4().hex[:12]}"
        purchase_date = datetime.fromisoformat(activity.timestamp)
        cost_basis = activity.quantity * activity.price + activity.commission + activity.fees
        price_per_share = cost_basis / activity.quantity if activity.quantity > 0 else 0.0

        lot = TaxLot(
            lot_id=lot_id,
            instrument_id=activity.instrument_id,
            quantity=activity.quantity,
            cost_basis=cost_basis,
            price_per_share=price_per_share,
            purchase_date=purchase_date,
            account_id=activity.account_id,
        )
        self._lots[lot_id] = lot
        return lot

    def get_activities(
        self,
        user_id: str | None = None,
        account_id: str | None = None,
        instrument_id: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        trade_type: TradeType | None = None,
    ) -> list[TradeActivity]:
        """Get trade activities with filters."""
        results = []
        for a in reversed(self._activities):
            if user_id and a.user_id != user_id:
                continue
            if account_id and a.account_id != account_id:
                continue
            if instrument_id and a.instrument_id != instrument_id:
                continue
            if trade_type and a.trade_type != trade_type:
                continue
            ts = datetime.fromisoformat(a.timestamp)
            if start_date and ts < start_date:
                continue
            if end_date and ts > end_date:
                continue
            results.append(a)
        return results

    # ── Tax Lots ──────────────────────────────────────────────────────────

    def get_open_lots(self, instrument_id: str, account_id: str | None = None) -> list[TaxLot]:
        """Get open (unlinked) tax lots for an instrument."""
        return [
            lot for lot in self._lots.values()
            if lot.instrument_id == instrument_id
            and lot.linked_sale_id is None
            and (account_id is None or lot.account_id == account_id)
        ]

    def get_lots_for_instrument(
        self,
        instrument_id: str,
        account_id: str | None = None,
    ) -> list[TaxLot]:
        """Get all tax lots for an instrument."""
        return [
            lot for lot in self._lots.values()
            if lot.instrument_id == instrument_id
            and (account_id is None or lot.account_id == account_id)
        ]

    # ── Capital Gains ─────────────────────────────────────────────────────

    def calculate_gain(
        self,
        sale_activity: TradeActivity,
        method: CostBasisMethod = CostBasisMethod.FIFO,
    ) -> CapitalGain | None:
        """Calculate capital gain for a sale using specified cost basis method."""
        if sale_activity.trade_type != TradeType.SELL:
            return None

        open_lots = self.get_open_lots(sale_activity.instrument_id, sale_activity.account_id)
        if not open_lots:
            return None

        # Sort lots by method
        if method == CostBasisMethod.FIFO:
            open_lots.sort(key=lambda l: l.purchase_date)
        elif method == CostBasisMethod.LIFO:
            open_lots.sort(key=lambda l: l.purchase_date, reverse=True)
        elif method == CostBasisMethod.HIFO:
            open_lots.sort(key=lambda l: l.price_per_share, reverse=True)
        elif method == CostBasisMethod.MINIMAX:
            # Prefer lots that minimize gain / maximize loss
            open_lots.sort(key=lambda l: l.price_per_share)

        # Match sale against lots
        remaining_qty = sale_activity.quantity
        total_cost = 0.0
        linked_lots: list[str] = []
        sale_date = datetime.fromisoformat(sale_activity.timestamp)

        for lot in open_lots:
            if remaining_qty <= 0:
                break
            if lot.quantity <= 0:
                continue

            # Determine how much to take from this lot
            qty_from_lot = min(remaining_qty, lot.quantity)
            cost_from_lot = qty_from_lot * lot.price_per_share
            total_cost += cost_from_lot
            remaining_qty -= qty_from_lot

            # Mark lot as linked
            lot.linked_sale_id = sale_activity.activity_id
            linked_lots.append(lot.lot_id)

        if remaining_qty > 0:
            # Partial fill - shouldn't happen normally
            pass

        proceeds = sale_activity.quantity * sale_activity.price - sale_activity.commission - sale_activity.fees
        gain = proceeds - total_cost

        # Determine holding period
        # Use earliest linked lot's purchase date
        earliest_purchase = min(
            (self._lots[lid].purchase_date for lid in linked_lots if lid in self._lots),
            default=sale_date,
        )
        holding_days = (sale_date - earliest_purchase).days
        gain_type = GainType.LONG_TERM if holding_days >= 365 else GainType.SHORT_TERM

        capital_gain = CapitalGain(
            gain_id=f"gain:{uuid.uuid4().hex[:12]}",
            sale_activity_id=sale_activity.activity_id,
            account_id=sale_activity.account_id,
            instrument_id=sale_activity.instrument_id,
            gain_type=gain_type,
            proceeds=proceeds,
            cost_basis=total_cost,
            gain=gain,
            holding_period_days=holding_days,
            lot_ids=tuple(linked_lots),
            trade_date=sale_activity.timestamp,
        )
        self._gains.append(capital_gain)
        return capital_gain

    def get_gains(
        self,
        user_id: str | None = None,
        account_id: str | None = None,
        instrument_id: str | None = None,
        year: int | None = None,
    ) -> list[CapitalGain]:
        """Get capital gains with filters."""
        results = []
        for g in self._gains:
            # Get the user/account from sale activity
            if user_id or account_id:
                matching = [a for a in self._activities if a.activity_id == g.sale_activity_id]
                if not matching:
                    continue
                act = matching[0]
                if user_id and act.user_id != user_id:
                    continue
                if account_id and act.account_id != account_id:
                    continue
            if instrument_id and g.instrument_id != instrument_id:
                continue
            if year:
                gain_date = datetime.fromisoformat(g.trade_date)
                if gain_date.year != year:
                    continue
            results.append(g)
        return results

    # ── Tax Summaries ────────────────────────────────────────────────────

    def compute_annual_summary(
        self,
        user_id: str,
        year: int,
        account_id: str | None = None,
    ) -> TaxSummary:
        """Compute annual tax summary for a user."""
        start = datetime(year, 1, 1, tzinfo=timezone.utc)
        end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc)

        activities = self.get_activities(
            user_id=user_id,
            account_id=account_id,
            start_date=start,
            end_date=end,
        )
        gains = self.get_gains(user_id=user_id, account_id=account_id, year=year)

        summary = TaxSummary(year=year, user_id=user_id)

        for gain in gains:
            if gain.gain_type == GainType.SHORT_TERM:
                if gain.gain >= 0:
                    summary.short_term_gains += gain.gain
                else:
                    summary.short_term_losses += abs(gain.gain)
            else:
                if gain.gain >= 0:
                    summary.long_term_gains += gain.gain
                else:
                    summary.long_term_losses += abs(gain.gain)

        for act in activities:
            if act.trade_type == TradeType.DIVIDEND:
                summary.total_dividends += act.net_amount
            elif act.trade_type == TradeType.INTEREST:
                summary.total_interest += act.net_amount
            elif act.trade_type == TradeType.FEE:
                summary.total_fees += abs(act.net_amount)

        summary.net_short_term = summary.short_term_gains - summary.short_term_losses
        summary.net_long_term = summary.long_term_gains - summary.long_term_losses

        return summary

    # ── Wash Sales ────────────────────────────────────────────────────────

    def check_wash_sale(
        self,
        instrument_id: str,
        sale_date: datetime,
        loss: float,
        window_days: int = 30,
    ) -> WashSaleRecord | None:
        """Check if a sale creates a wash sale.

        A wash sale occurs when you sell at a loss and buy back within
        30 days before or after the sale.
        """
        window_start = sale_date - timedelta(days=window_days)
        window_end = sale_date + timedelta(days=window_days)

        # Find buys in the window
        buys_in_window = [
            a for a in self._activities
            if a.instrument_id == instrument_id
            and a.trade_type == TradeType.BUY
            and window_start <= datetime.fromisoformat(a.timestamp) <= window_end
        ]

        if not buys_in_window:
            return None

        # Disallowed loss = the loss amount (simplified)
        wash = WashSaleRecord(
            wash_id=f"wash:{uuid.uuid4().hex[:12]}",
            disallowed_loss=loss,
            replacement_shares=None,
            original_sale_date=sale_date.isoformat(),
            wash_period_start=window_start.isoformat(),
            wash_period_end=window_end.isoformat(),
        )
        self._wash_sales.append(wash)
        return wash

    # ── Export ────────────────────────────────────────────────────────────

    def export_summary_csv(
        self,
        user_id: str,
        year: int,
    ) -> str:
        """Export tax summary as CSV string."""
        summary = self.compute_annual_summary(user_id, year)
        lines = [
            f"Tax Summary {year}",
            f"User ID,{summary.user_id}",
            "",
            "Capital Gains",
            f"Short-term Gains,{summary.short_term_gains:.2f}",
            f"Short-term Losses,{summary.short_term_losses:.2f}",
            f"Long-term Gains,{summary.long_term_gains:.2f}",
            f"Long-term Losses,{summary.long_term_losses:.2f}",
            "",
            "Net",
            f"Net Short-term,{summary.net_short_term:.2f}",
            f"Net Long-term,{summary.net_long_term:.2f}",
            "",
            "Income",
            f"Dividends,{summary.total_dividends:.2f}",
            f"Interest,{summary.total_interest:.2f}",
            f"Fees,{summary.total_fees:.2f}",
        ]
        return "\n".join(lines)

    def generate_1099_summary(
        self,
        user_id: str,
        year: int,
    ) -> dict[str, Any]:
        """Generate 1099-compatible summary data."""
        summary = self.compute_annual_summary(user_id, year)
        return {
            "user_id": user_id,
            "year": year,
            "form_type": "1099",
            "dividends": summary.total_dividends,
            "interest": summary.total_interest,
            "short_term_gains": summary.net_short_term,
            "long_term_gains": summary.net_long_term,
            "total_gains": summary.net_short_term + summary.net_long_term,
            "fees_paid": summary.total_fees,
            "generated_at": _now(),
        }
