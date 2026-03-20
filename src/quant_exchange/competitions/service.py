"""Competition platform service (COMP-01~COMP-04).

Covers:
- COMP-01: Competition creation and management
- COMP-02: Registration and team formation
- COMP-03: Submission tracking and leaderboard
- COMP-04: Judging and prize distribution
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class CompetitionStatus(str, Enum):
    DRAFT = "draft"
    REGISTRATION = "registration"
    IN_PROGRESS = "in_progress"
    JUDGING = "judging"
    COMPLETED = "completed"
    CANCELLED = "cancelled"


class SubmissionStatus(str, Enum):
    DRAFT = "draft"
    SUBMITTED = "submitted"
    RUNNING = "running"
    COMPLETED = "completed"
    DISQUALIFIED = "disqualified"


@dataclass(slots=True)
class Competition:
    """A trading competition."""

    competition_id: str
    name: str
    description: str
    status: CompetitionStatus
    start_time: datetime
    end_time: datetime
    registration_deadline: datetime
    max_team_size: int = 1
    min_team_size: int = 1
    max_participants: int = 100
    entry_fee: float = 0.0
    prizes: dict[str, float] = field(default_factory=dict)  # rank -> prize amount
    rules: str = ""
    evaluation_metric: str = "sharpe_ratio"  # sharpe_ratio / total_return / max_drawdown
    current_participants: int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Participant:
    """A competition participant or team."""

    participant_id: str
    competition_id: str
    user_id: str
    team_name: str
    team_members: list[str] = field(default_factory=list)
    is_individual: bool = True
    registration_time: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    final_rank: int = 0
    final_score: float = 0.0


@dataclass(slots=True)
class Submission:
    """A strategy submission to a competition."""

    submission_id: str
    competition_id: str
    participant_id: str
    strategy_name: str
    strategy_code: str = ""
    status: SubmissionStatus = SubmissionStatus.DRAFT
    metrics: dict[str, float] = field(default_factory=dict)  # evaluation metrics
    backtest_result_id: str = ""
    submitted_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    ranked_at: datetime | None = None


class CompetitionService:
    """Competition platform service (COMP-01~COMP-04)."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._competitions: dict[str, Competition] = {}
        self._participants: dict[str, Participant] = {}
        self._submissions: dict[str, Submission] = {}
        self._init_demo_data()

    def _init_demo_data(self) -> None:
        now = datetime.now(timezone.utc)
        comps = [
            Competition(
                competition_id="comp001", name="Q1 2026量化策略大赛", description="专注于趋势跟踪策略的季度竞赛，评估指标为夏普比率", status=CompetitionStatus.REGISTRATION, start_time=now + timedelta(days=14), end_time=now + timedelta(days=44), registration_deadline=now + timedelta(days=10), max_participants=50, prizes={"1": 10000.0, "2": 5000.0, "3": 2000.0}, evaluation_metric="sharpe_ratio", current_participants=12),
            Competition(
                competition_id="comp002", name="期权波动率策略挑战赛", description="期权波动率套利专项竞赛", status=CompetitionStatus.IN_PROGRESS, start_time=now - timedelta(days=10), end_time=now + timedelta(days=20), registration_deadline=now - timedelta(days=5), max_participants=30, prizes={"1": 8000.0, "2": 4000.0}, evaluation_metric="total_return", current_participants=18),
        ]
        for c in comps:
            self._competitions[c.competition_id] = c

    # ── COMP-01: Competition Management ────────────────────────────────────

    def create_competition(
        self,
        name: str,
        description: str,
        start_time: datetime,
        end_time: datetime,
        registration_deadline: datetime,
        evaluation_metric: str = "sharpe_ratio",
        max_participants: int = 100,
        max_team_size: int = 1,
        min_team_size: int = 1,
        entry_fee: float = 0.0,
        prizes: dict[str, float] | None = None,
        rules: str = "",
    ) -> Competition:
        """Create a new competition (COMP-01)."""
        comp = Competition(
            competition_id=f"comp:{uuid.uuid4().hex[:12]}",
            name=name,
            description=description,
            status=CompetitionStatus.REGISTRATION,
            start_time=start_time,
            end_time=end_time,
            registration_deadline=registration_deadline,
            evaluation_metric=evaluation_metric,
            max_participants=max_participants,
            max_team_size=max_team_size,
            min_team_size=min_team_size,
            entry_fee=entry_fee,
            prizes=prizes or {},
            rules=rules,
        )
        self._competitions[comp.competition_id] = comp
        return comp

    def get_competition(self, competition_id: str) -> Competition | None:
        """Get a competition by ID."""
        return self._competitions.get(competition_id)

    def list_competitions(
        self,
        status: CompetitionStatus | None = None,
        limit: int = 20,
    ) -> list[Competition]:
        """List competitions (COMP-01)."""
        results = list(self._competitions.values())
        if status:
            results = [c for c in results if c.status == status]
        results.sort(key=lambda c: c.start_time, reverse=True)
        return results[:limit]

    def update_status(self, competition_id: str, status: CompetitionStatus) -> bool:
        """Update competition status."""
        comp = self._competitions.get(competition_id)
        if not comp:
            return False
        comp.status = status
        return True

    # ── COMP-02: Registration ────────────────────────────────────────────

    def register(
        self,
        competition_id: str,
        user_id: str,
        team_name: str,
        team_members: list[str] | None = None,
    ) -> Participant | None:
        """Register for a competition (COMP-02)."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status != CompetitionStatus.REGISTRATION:
            return None

        if comp.current_participants >= comp.max_participants:
            return None

        members = team_members or []
        if len(members) + 1 > comp.max_team_size:
            return None
        if len(members) + 1 < comp.min_team_size:
            return None

        participant = Participant(
            participant_id=f"part:{uuid.uuid4().hex[:12]}",
            competition_id=competition_id,
            user_id=user_id,
            team_name=team_name,
            team_members=members,
            is_individual=len(members) == 0,
        )
        self._participants[participant.participant_id] = participant
        comp.current_participants += 1
        return participant

    def list_participants(self, competition_id: str) -> list[Participant]:
        """List participants in a competition."""
        return [p for p in self._participants.values() if p.competition_id == competition_id]

    # ── COMP-03: Submissions & Leaderboard ─────────────────────────────────

    def submit_strategy(
        self,
        competition_id: str,
        participant_id: str,
        strategy_name: str,
        strategy_code: str = "",
    ) -> Submission | None:
        """Submit a strategy to a competition (COMP-03)."""
        comp = self._competitions.get(competition_id)
        if not comp or comp.status not in (CompetitionStatus.IN_PROGRESS, CompetitionStatus.REGISTRATION):
            return None

        participant = self._participants.get(participant_id)
        if not participant or participant.competition_id != competition_id:
            return None

        submission = Submission(
            submission_id=f"sub:{uuid.uuid4().hex[:12]}",
            competition_id=competition_id,
            participant_id=participant_id,
            strategy_name=strategy_name,
            strategy_code=strategy_code,
            status=SubmissionStatus.SUBMITTED,
        )
        self._submissions[submission.submission_id] = submission
        return submission

    def update_submission_metrics(self, submission_id: str, metrics: dict[str, float]) -> bool:
        """Update submission evaluation metrics (COMP-03)."""
        sub = self._submissions.get(submission_id)
        if not sub:
            return False
        sub.metrics = metrics
        if sub.status == SubmissionStatus.SUBMITTED:
            sub.status = SubmissionStatus.COMPLETED
        return True

    def get_leaderboard(self, competition_id: str, limit: int = 20) -> list[dict[str, Any]]:
        """Get competition leaderboard (COMP-03)."""
        comp = self._competitions.get(competition_id)
        if not comp:
            return []

        submissions = [s for s in self._submissions.values() if s.competition_id == competition_id and s.status == SubmissionStatus.COMPLETED]

        # Sort by evaluation metric
        metric = comp.evaluation_metric
        if metric == "max_drawdown":
            submissions.sort(key=lambda s: s.metrics.get(metric, 0))  # lower is better
        else:
            submissions.sort(key=lambda s: s.metrics.get(metric, 0), reverse=True)  # higher is better

        leaderboard = []
        for rank, sub in enumerate(submissions[:limit], 1):
            participant = self._participants.get(sub.participant_id)
            leaderboard.append({
                "rank": rank,
                "submission_id": sub.submission_id,
                "participant_id": sub.participant_id,
                "team_name": participant.team_name if participant else "Unknown",
                "strategy_name": sub.strategy_name,
                "metrics": sub.metrics,
                "primary_metric": sub.metrics.get(metric, 0.0),
            })

        return leaderboard

    # ── COMP-04: Judging & Prizes ─────────────────────────────────────────

    def finalize_competition(self, competition_id: str) -> dict[str, Any]:
        """Finalize competition, calculate rankings and prizes (COMP-04)."""
        comp = self._competitions.get(competition_id)
        if not comp:
            return {}

        comp.status = CompetitionStatus.COMPLETED

        leaderboard = self.get_leaderboard(competition_id)
        prize_dist: dict[str, float] = {}

        for entry in leaderboard:
            rank = entry["rank"]
            prize = comp.prizes.get(str(rank), 0.0)
            if prize > 0:
                participant = self._participants.get(entry["participant_id"])
                if participant:
                    prize_dist[participant.user_id] = prize
                    participant.final_rank = rank
                    participant.final_score = entry["primary_metric"]

        return {
            "competition_id": competition_id,
            "total_participants": len(leaderboard),
            "prize_distribution": prize_dist,
            "leaderboard": leaderboard,
        }


from datetime import timedelta
