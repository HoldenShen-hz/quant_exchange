"""Team collaboration service (COLLAB-01~COLLAB-04).

Covers:
- COLLAB-01: Team creation and member management
- COLLAB-02: Shared workspace and strategy versioning
- COLLAB-03: Activity feed and notifications
- COLLAB-04: Access control and permissions within teams
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class MemberRole(str, Enum):
    OWNER = "owner"
    ADMIN = "admin"
    EDITOR = "editor"
    VIEWER = "viewer"


class ActivityType(str, Enum):
    STRATEGY_CREATED = "strategy_created"
    STRATEGY_UPDATED = "strategy_updated"
    STRATEGY_SHARED = "strategy_shared"
    MEMBER_JOINED = "member_joined"
    MEMBER_LEFT = "member_left"
    COMMENT_ADDED = "comment_added"
    WORKSPACE_UPDATED = "workspace_updated"


@dataclass(slots=True)
class TeamMember:
    """A member of a team."""

    member_id: str
    team_id: str
    user_id: str
    display_name: str
    role: MemberRole
    joined_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Team:
    """A collaborative team."""

    team_id: str
    name: str
    description: str
    owner_id: str
    members: list[TeamMember] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class Workspace:
    """A shared workspace within a team."""

    workspace_id: str
    team_id: str
    name: str
    description: str
    strategy_ids: list[str] = field(default_factory=list)  # shared strategies
    settings: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass(slots=True)
class ActivityLog:
    """An activity log entry in a workspace."""

    log_id: str
    team_id: str
    workspace_id: str | None
    user_id: str
    display_name: str
    activity_type: ActivityType
    description: str
    metadata: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class CollaborationService:
    """Team collaboration service (COLLAB-01~COLLAB-04)."""

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._teams: dict[str, Team] = {}
        self._workspaces: dict[str, Workspace] = {}
        self._activity_logs: list[ActivityLog] = []
        self._init_demo_data()

    def _init_demo_data(self) -> None:
        team = Team(
            team_id="team001",
            name="Alpha Quant Research",
            description="专注于量化策略研究的团队",
            owner_id="u001",
            members=[
                TeamMember(member_id="m001", team_id="team001", user_id="u001", display_name="Alice Chen", role=MemberRole.OWNER),
                TeamMember(member_id="m002", team_id="team001", user_id="u002", display_name="Bob Zhang", role=MemberRole.ADMIN),
                TeamMember(member_id="m003", team_id="team001", user_id="u003", display_name="Carol Wang", role=MemberRole.EDITOR),
            ],
        )
        self._teams["team001"] = team

        ws = Workspace(
            workspace_id="ws001",
            team_id="team001",
            name="期权策略研究",
            description="期权波动率套利策略共享空间",
            strategy_ids=["strat001", "strat002"],
        )
        self._workspaces["ws001"] = ws

    # ── COLLAB-01: Team Management ─────────────────────────────────────────

    def create_team(self, owner_id: str, name: str, description: str = "") -> Team:
        """Create a new team (COLLAB-01)."""
        team = Team(
            team_id=f"team:{uuid.uuid4().hex[:12]}",
            name=name,
            description=description,
            owner_id=owner_id,
        )
        # Add owner as member
        owner_member = TeamMember(
            member_id=f"m:{uuid.uuid4().hex[:12]}",
            team_id=team.team_id,
            user_id=owner_id,
            display_name=owner_id,
            role=MemberRole.OWNER,
        )
        team.members.append(owner_member)
        self._teams[team.team_id] = team
        return team

    def get_team(self, team_id: str) -> Team | None:
        """Get a team by ID."""
        return self._teams.get(team_id)

    def list_user_teams(self, user_id: str) -> list[Team]:
        """List all teams a user belongs to."""
        return [t for t in self._teams.values() if any(m.user_id == user_id for m in t.members)]

    def add_member(self, team_id: str, user_id: str, display_name: str, role: MemberRole) -> TeamMember | None:
        """Add a member to a team (COLLAB-01)."""
        team = self._teams.get(team_id)
        if not team:
            return None
        if any(m.user_id == user_id for m in team.members):
            return None  # already a member

        member = TeamMember(
            member_id=f"m:{uuid.uuid4().hex[:12]}",
            team_id=team_id,
            user_id=user_id,
            display_name=display_name,
            role=role,
        )
        team.members.append(member)
        self._log_activity(team_id, None, user_id, display_name, ActivityType.MEMBER_JOINED, f"{display_name} joined the team")
        return member

    def remove_member(self, team_id: str, user_id: str) -> bool:
        """Remove a member from a team."""
        team = self._teams.get(team_id)
        if not team:
            return False
        team.members = [m for m in team.members if m.user_id != user_id]
        self._log_activity(team_id, None, user_id, user_id, ActivityType.MEMBER_LEFT, f"{user_id} left the team")
        return True

    def update_member_role(self, team_id: str, user_id: str, role: MemberRole) -> bool:
        """Update a member's role (COLLAB-04)."""
        team = self._teams.get(team_id)
        if not team:
            return False
        for member in team.members:
            if member.user_id == user_id:
                member.role = role
                return True
        return False

    # ── COLLAB-02: Workspace ───────────────────────────────────────────────

    def create_workspace(
        self,
        team_id: str,
        name: str,
        description: str = "",
    ) -> Workspace | None:
        """Create a workspace within a team (COLLAB-02)."""
        team = self._teams.get(team_id)
        if not team:
            return None

        ws = Workspace(
            workspace_id=f"ws:{uuid.uuid4().hex[:12]}",
            team_id=team_id,
            name=name,
            description=description,
        )
        self._workspaces[ws.workspace_id] = ws
        self._log_activity(team_id, ws.workspace_id, team.owner_id, team.owner_id, ActivityType.WORKSPACE_UPDATED, f"Workspace '{name}' created")
        return ws

    def get_workspace(self, workspace_id: str) -> Workspace | None:
        """Get a workspace by ID."""
        return self._workspaces.get(workspace_id)

    def list_team_workspaces(self, team_id: str) -> list[Workspace]:
        """List all workspaces in a team."""
        return [w for w in self._workspaces.values() if w.team_id == team_id]

    def share_strategy_to_workspace(self, workspace_id: str, strategy_id: str) -> bool:
        """Share a strategy to a workspace (COLLAB-02)."""
        ws = self._workspaces.get(workspace_id)
        if not ws or strategy_id in ws.strategy_ids:
            return False
        ws.strategy_ids.append(strategy_id)
        ws.updated_at = datetime.now(timezone.utc)
        return True

    def update_workspace_settings(self, workspace_id: str, settings: dict[str, Any]) -> bool:
        """Update workspace settings."""
        ws = self._workspaces.get(workspace_id)
        if not ws:
            return False
        ws.settings.update(settings)
        ws.updated_at = datetime.now(timezone.utc)
        return True

    # ── COLLAB-03: Activity Feed ───────────────────────────────────────────

    def _log_activity(
        self,
        team_id: str,
        workspace_id: str | None,
        user_id: str,
        display_name: str,
        activity_type: ActivityType,
        description: str,
        metadata: dict[str, Any] | None = None,
    ) -> ActivityLog:
        """Log an activity."""
        log = ActivityLog(
            log_id=f"log:{uuid.uuid4().hex[:12]}",
            team_id=team_id,
            workspace_id=workspace_id,
            user_id=user_id,
            display_name=display_name,
            activity_type=activity_type,
            description=description,
            metadata=metadata or {},
        )
        self._activity_logs.append(log)
        return log

    def get_team_activity(self, team_id: str, limit: int = 50) -> list[ActivityLog]:
        """Get activity feed for a team (COLLAB-03)."""
        logs = [l for l in self._activity_logs if l.team_id == team_id]
        logs.sort(key=lambda l: l.created_at, reverse=True)
        return logs[:limit]

    def get_workspace_activity(self, workspace_id: str, limit: int = 30) -> list[ActivityLog]:
        """Get activity feed for a workspace."""
        logs = [l for l in self._activity_logs if l.workspace_id == workspace_id]
        logs.sort(key=lambda l: l.created_at, reverse=True)
        return logs[:limit]

    # ── COLLAB-04: Access Control ─────────────────────────────────────────

    def check_permission(self, team_id: str, user_id: str, required_role: MemberRole) -> bool:
        """Check if a user has at least the required role in a team (COLLAB-04)."""
        team = self._teams.get(team_id)
        if not team:
            return False

        role_hierarchy = {MemberRole.VIEWER: 0, MemberRole.EDITOR: 1, MemberRole.ADMIN: 2, MemberRole.OWNER: 3}

        for member in team.members:
            if member.user_id == user_id:
                return role_hierarchy.get(member.role, 0) >= role_hierarchy.get(required_role, 0)

        return False

    def get_member_permissions(self, team_id: str, user_id: str) -> dict[str, bool]:
        """Get all permissions for a user in a team (COLLAB-04)."""
        team = self._teams.get(team_id)
        if not team:
            return {}

        member = next((m for m in team.members if m.user_id == user_id), None)
        if not member:
            return {}

        return {
            "can_view": True,
            "can_edit": member.role in (MemberRole.ADMIN, MemberRole.EDITOR, MemberRole.OWNER),
            "can_manage_members": member.role in (MemberRole.ADMIN, MemberRole.OWNER),
            "can_delete": member.role == MemberRole.OWNER,
            "can_share": member.role in (MemberRole.ADMIN, MemberRole.EDITOR, MemberRole.OWNER),
        }
