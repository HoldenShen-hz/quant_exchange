"""Trading competition service (COMP-01 ~ COMP-04).

Covers:
- Competition definitions, rules, registration, running, and ending
- Leaderboards with multiple statistical dimensions
- Reward mechanism and achievement system
- Competition integration with community, learning, and paper trading
"""

from __future__ import annotations

import uuid
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


# ─────────────────────────────────────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────────────────────────────────────

class CompetitionStatus(str, Enum):
    DRAFT = "draft"        # Being set up
    REGISTRATION = "registration"  # Open for sign-up
    RUNNING = "running"     # Live competition
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class RewardType(str, Enum):
    CASH = "cash"
    SUBSCRIPTION = "subscription"  # Free platform subscription
    BADGE = "badge"
    TROPHY = "trophy"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class CompetitionRule:
    """A rule defining competition constraints."""

    rule_id: str
    name: str
    description: str
    constraint_type: str  # "max_drawdown", "min_trades", "allowed_instruments"
    threshold: float = 0.0


@dataclass(slots=True)
class Competition:
    """A trading competition definition."""

    competition_id: str
    name: str
    description: str
    status: CompetitionStatus
    start_time: datetime
    end_time: datetime
    registration_deadline: datetime
    max_participants: int
    current_participants: int = 0
    entry门槛: str = ""  # Entry threshold description
    scoring_method: str = "total_return"  # total_return, sharpe, calmar, etc.
    rules: tuple[str, ...] = field(default_factory=tuple)
    tags: tuple[str, ...] = field(default_factory=tuple)
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class CompetitionParticipant:
    """A participant in a competition."""

    participant_id: str
    competition_id: str
    user_id: str
    username: str
    initial_equity: float
    current_equity: float
    rank: int = 0
    total_return: float = 0.0
    sharpe: float = 0.0
    max_drawdown: float = 0.0
    trade_count: int = 0
    win_rate: float = 0.0
    is_disqualified: bool = False
    disqualify_reason: str = ""
    registered_at: str = field(default_factory=_now)
    last_updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class LeaderboardEntry:
    """A single leaderboard position."""

    rank: int
    user_id: str
    username: str
    score: float
    metrics: dict[str, float]  # {total_return, sharpe, max_drawdown, ...}


@dataclass(slots=True)
class Reward:
    """A reward for a competition placement."""

    reward_id: str
    competition_id: str
    placement: int  # 1 = first place, 2 = second, etc.
    reward_type: RewardType
    value: float
    description: str
    is_claimed: bool = False
    claimed_at: str | None = None


@dataclass(slots=True)
class Achievement:
    """An achievement/badge earned by a user."""

    achievement_id: str
    user_id: str
    badge_type: str  # e.g. "first_win", "sharpe_master", "comeback_kid"
    title: str
    description: str
    icon: str = ""
    earned_at: str = field(default_factory=_now)


# ─────────────────────────────────────────────────────────────────────────────
# Competition Service
# ─────────────────────────────────────────────────────────────────────────────

class CompetitionService:
    """Trading competition service (COMP-01 ~ COMP-04).

    Provides:
    - Competition creation and lifecycle management
    - Registration and participant tracking
    - Real-time leaderboard computation
    - Reward mechanism and achievement system
    - Integration with community, learning, and paper trading
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._competitions: dict[str, Competition] = {}
        self._participants: dict[str, list[CompetitionParticipant]] = defaultdict(list)
        self._user_participations: dict[str, set[str]] = defaultdict(set)  # user_id -> competition_ids
        self._rewards: dict[str, list[Reward]] = defaultdict(list)
        self._achievements: dict[str, list[Achievement]] = defaultdict(list)
        self._equity_curves: dict[str, list[tuple[str, float]]] = defaultdict(list)  # participant_id -> [(timestamp, equity)]

    # ── Competition Lifecycle ───────────────────────────────────────────────

    def create_competition(
        self,
        name: str,
        description: str,
        start_time: datetime,
        end_time: datetime,
        registration_deadline: datetime,
        max_participants: int,
        scoring_method: str = "total_return",
        entry_threshold: str = "",
        rules: tuple[str, ...] = (),
        tags: tuple[str, ...] = (),
    ) -> Competition:
        """Create a new competition (draft state)."""
        competition_id = f"comp:{uuid.uuid4().hex[:12]}"
        competition = Competition(
            competition_id=competition_id,
            name=name,
            description=description,
            status=CompetitionStatus.DRAFT,
            start_time=start_time,
            end_time=end_time,
            registration_deadline=registration_deadline,
            max_participants=max_participants,
            entry门槛=entry_threshold,
            scoring_method=scoring_method,
            rules=rules,
            tags=tags,
        )
        self._competitions[competition_id] = competition
        return competition

    def open_registration(self, competition_id: str) -> bool:
        """Open registration for a competition (DRAFT -> REGISTRATION)."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status != CompetitionStatus.DRAFT:
            return False
        comp.status = CompetitionStatus.REGISTRATION
        return True

    def start_competition(self, competition_id: str) -> bool:
        """Start a competition (REGISTRATION -> RUNNING)."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status != CompetitionStatus.REGISTRATION:
            return False
        comp.status = CompetitionStatus.RUNNING
        return True

    def end_competition(self, competition_id: str) -> bool:
        """End a competition (RUNNING -> COMPLETED) and finalize leaderboard."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status != CompetitionStatus.RUNNING:
            return False
        comp.status = CompetitionStatus.COMPLETED
        # Finalize leaderboard
        self._finalize_leaderboard(competition_id)
        return True

    def cancel_competition(self, competition_id: str) -> bool:
        """Cancel a competition."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status in (CompetitionStatus.COMPLETED, CompetitionStatus.CANCELLED):
            return False
        comp.status = CompetitionStatus.CANCELLED
        return True

    def get_competition(self, competition_id: str) -> Competition | None:
        """Get a competition by ID."""
        return self._competitions.get(competition_id)

    def list_competitions(
        self,
        status: CompetitionStatus | None = None,
        tags: list[str] | None = None,
        limit: int = 20,
    ) -> list[Competition]:
        """List competitions with optional filters."""
        results = []
        for comp in self._competitions.values():
            if status and comp.status != status:
                continue
            if tags and not any(tag in comp.tags for tag in tags):
                continue
            results.append(comp)
        return sorted(results, key=lambda c: c.start_time, reverse=True)[:limit]

    # ── Registration ──────────────────────────────────────────────────────

    def register(
        self,
        competition_id: str,
        user_id: str,
        username: str,
        initial_equity: float = 100_000.0,
    ) -> CompetitionParticipant | None:
        """Register a user for a competition."""
        comp = self._competitions.get(competition_id)
        if not comp:
            return None
        if comp.status not in (CompetitionStatus.REGISTRATION, CompetitionStatus.DRAFT):
            return None
        if comp.current_participants >= comp.max_participants:
            return None
        if user_id in self._user_participations:
            # Already registered
            for p in self._participants[competition_id]:
                if p.user_id == user_id:
                    return p

        participant = CompetitionParticipant(
            participant_id=f"part:{uuid.uuid4().hex[:12]}",
            competition_id=competition_id,
            user_id=user_id,
            username=username,
            initial_equity=initial_equity,
            current_equity=initial_equity,
        )
        self._participants[competition_id].append(participant)
        self._user_participations[user_id].add(competition_id)
        comp.current_participants += 1
        return participant

    def unregister(self, competition_id: str, user_id: str) -> bool:
        """Unregister a user before competition starts."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status == CompetitionStatus.RUNNING:
            return False
        participants = self._participants[competition_id]
        for i, p in enumerate(participants):
            if p.user_id == user_id:
                participants.pop(i)
                self._user_participations[user_id].discard(competition_id)
                comp.current_participants = max(0, comp.current_participants - 1)
                return True
        return False

    def disqualify_participant(
        self,
        competition_id: str,
        user_id: str,
        reason: str,
    ) -> bool:
        """Disqualify a participant for rule violations."""
        for p in self._participants.get(competition_id, []):
            if p.user_id == user_id:
                p.is_disqualified = True
                p.disqualify_reason = reason
                return True
        return False

    # ── Performance Tracking ───────────────────────────────────────────────

    def record_equity(
        self,
        competition_id: str,
        user_id: str,
        equity: float,
    ) -> None:
        """Record a participant's equity at a point in time."""
        for p in self._participants.get(competition_id, []):
            if p.user_id == user_id:
                p.current_equity = equity
                p.total_return = (equity - p.initial_equity) / p.initial_equity if p.initial_equity > 0 else 0.0
                p.last_updated_at = _now()
                ts = datetime.now(timezone.utc).isoformat()
                self._equity_curves[p.participant_id].append((ts, equity))
                break

    def update_participant_metrics(
        self,
        competition_id: str,
        user_id: str,
        *,
        sharpe: float | None = None,
        max_drawdown: float | None = None,
        trade_count: int | None = None,
        win_rate: float | None = None,
    ) -> None:
        """Update a participant's performance metrics."""
        for p in self._participants.get(competition_id, []):
            if p.user_id == user_id:
                if sharpe is not None:
                    p.sharpe = sharpe
                if max_drawdown is not None:
                    p.max_drawdown = max_drawdown
                if trade_count is not None:
                    p.trade_count = trade_count
                if win_rate is not None:
                    p.win_rate = win_rate
                p.last_updated_at = _now()
                break

    def get_participant(
        self,
        competition_id: str,
        user_id: str,
    ) -> CompetitionParticipant | None:
        """Get a participant record."""
        for p in self._participants.get(competition_id, []):
            if p.user_id == user_id:
                return p
        return None

    def get_equity_curve(
        self,
        competition_id: str,
        user_id: str,
    ) -> list[tuple[str, float]]:
        """Get the equity curve for a participant."""
        for p in self._participants.get(competition_id, []):
            if p.user_id == user_id:
                return self._equity_curves.get(p.participant_id, [])
        return []

    # ── Leaderboard ───────────────────────────────────────────────────────

    def _finalize_leaderboard(self, competition_id: str) -> list[CompetitionParticipant]:
        """Finalize and rank all participants at end of competition."""
        participants = self._participants.get(competition_id, [])
        active = [p for p in participants if not p.is_disqualified]

        # Rank by scoring method
        comp = self._competitions.get(competition_id)
        scoring = comp.scoring_method if comp else "total_return"

        if scoring == "total_return":
            active.sort(key=lambda p: p.total_return, reverse=True)
        elif scoring == "sharpe":
            active.sort(key=lambda p: p.sharpe, reverse=True)
        elif scoring == "calmar":
            active.sort(key=lambda p: p.total_return / max(p.max_drawdown, 0.001), reverse=True)
        elif scoring == "win_rate":
            active.sort(key=lambda p: p.win_rate, reverse=True)

        for i, p in enumerate(active):
            p.rank = i + 1

        # Disqualified get rank after
        disqualified = [p for p in participants if p.is_disqualified]
        max_rank = len(active)
        for p in disqualified:
            p.rank = max_rank + 1

        return participants

    def get_leaderboard(
        self,
        competition_id: str,
        dimension: str = "total_return",
        limit: int = 100,
    ) -> list[LeaderboardEntry]:
        """Get the current leaderboard for a competition."""
        participants = self._participants.get(competition_id, [])
        active = [p for p in participants if not p.is_disqualified]

        # Sort by dimension
        if dimension == "total_return":
            active.sort(key=lambda p: p.total_return, reverse=True)
        elif dimension == "sharpe":
            active.sort(key=lambda p: p.sharpe, reverse=True)
        elif dimension == "max_drawdown":
            active.sort(key=lambda p: p.max_drawdown, reverse=False)  # Lower drawdown = better
        elif dimension == "win_rate":
            active.sort(key=lambda p: p.win_rate, reverse=True)
        else:
            active.sort(key=lambda p: p.total_return, reverse=True)

        return [
            LeaderboardEntry(
                rank=i + 1,
                user_id=p.user_id,
                username=p.username,
                score=getattr(p, dimension, 0.0),
                metrics={
                    "total_return": p.total_return,
                    "sharpe": p.sharpe,
                    "max_drawdown": p.max_drawdown,
                    "win_rate": p.win_rate,
                    "trade_count": p.trade_count,
                },
            )
            for i, p in enumerate(active[:limit])
        ]

    def get_cross_competition_leaderboard(
        self,
        dimension: str = "total_return",
        limit: int = 20,
    ) -> list[LeaderboardEntry]:
        """Get a cross-competition leaderboard aggregating all participants."""
        all_active = []
        for comp in self._competitions.values():
            if comp.status != CompetitionStatus.COMPLETED:
                continue
            for p in self._participants.get(comp.competition_id, []):
                if not p.is_disqualified:
                    all_active.append(p)

        if dimension == "total_return":
            all_active.sort(key=lambda p: p.total_return, reverse=True)
        elif dimension == "sharpe":
            all_active.sort(key=lambda p: p.sharpe, reverse=True)

        return [
            LeaderboardEntry(
                rank=i + 1,
                user_id=p.user_id,
                username=p.username,
                score=getattr(p, dimension, 0.0),
                metrics={"total_return": p.total_return, "sharpe": p.sharpe},
            )
            for i, p in enumerate(all_active[:limit])
        ]

    # ── Rewards ───────────────────────────────────────────────────────────

    def define_rewards(
        self,
        competition_id: str,
        rewards: list[tuple[int, RewardType, float, str]],
    ) -> list[Reward]:
        """Define rewards for top N placements.

        rewards: [(placement, reward_type, value, description), ...]
        """
        defined = []
        for placement, rtype, value, desc in rewards:
            reward = Reward(
                reward_id=f"reward:{uuid.uuid4().hex[:12]}",
                competition_id=competition_id,
                placement=placement,
                reward_type=rtype,
                value=value,
                description=desc,
            )
            self._rewards[competition_id].append(reward)
            defined.append(reward)
        return defined

    def claim_reward(self, competition_id: str, user_id: str, placement: int) -> Reward | None:
        """Claim a reward for a specific placement."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status != CompetitionStatus.COMPLETED:
            return None

        participant = self.get_participant(competition_id, user_id)
        if not participant or participant.rank != placement:
            return None

        for reward in self._rewards.get(competition_id, []):
            if reward.placement == placement and not reward.is_claimed:
                reward.is_claimed = True
                reward.claimed_at = _now()
                return reward
        return None

    def get_rewards(self, competition_id: str) -> list[Reward]:
        """Get all rewards defined for a competition."""
        return self._rewards.get(competition_id, [])

    # ── Achievements ─────────────────────────────────────────────────────

    def award_achievement(
        self,
        user_id: str,
        badge_type: str,
        title: str,
        description: str,
        icon: str = "",
    ) -> Achievement:
        """Award an achievement/badge to a user."""
        achievement = Achievement(
            achievement_id=f"ach:{uuid.uuid4().hex[:12]}",
            user_id=user_id,
            badge_type=badge_type,
            title=title,
            description=description,
            icon=icon,
        )
        self._achievements[user_id].append(achievement)
        return achievement

    def get_user_achievements(self, user_id: str) -> list[Achievement]:
        """Get all achievements for a user."""
        return self._achievements.get(user_id, [])

    def get_achievement_leaderboard(self, badge_type: str, limit: int = 20) -> list[tuple[str, int]]:
        """Get users ranked by how many achievements of a specific type they've earned."""
        counts: dict[str, int] = defaultdict(int)
        for uid, achievements in self._achievements.items():
            for ach in achievements:
                if ach.badge_type == badge_type:
                    counts[uid] += 1
        return sorted(counts.items(), key=lambda x: x[1], reverse=True)[:limit]
