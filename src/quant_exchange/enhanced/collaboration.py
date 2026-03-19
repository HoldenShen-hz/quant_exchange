"""Real-time collaboration workspace service (COLLAB-01 ~ COLLAB-04).

Covers:
- Shared strategy libraries across workspace members
- Shared watchlists with real-time updates
- Role-based permissions and edit history
- Comments, reviews, and collaborative discussions
- Multi-user concurrent editing simulation
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

class WorkspaceRole(str, Enum):
    OWNER = "owner"
    EDITOR = "editor"
    VIEWER = "viewer"
    COMMENTER = "commenter"


class EditAction(str, Enum):
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    COMMENTED = "commented"
    PERMISSION_CHANGED = "permission_changed"


# ─────────────────────────────────────────────────────────────────────────────
# Data Models
# ─────────────────────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Workspace:
    """A collaborative workspace for a group of users."""

    workspace_id: str
    name: str
    description: str
    owner_id: str
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)
    is_deleted: bool = False


@dataclass(slots=True)
class WorkspaceMember:
    """A member of a collaborative workspace."""

    workspace_id: str
    user_id: str
    role: WorkspaceRole
    joined_at: str = field(default_factory=_now)


@dataclass(slots=True)
class EditRecord:
    """Audit trail for workspace edits."""

    edit_id: str
    workspace_id: str
    user_id: str
    resource_type: str  # "strategy", "watchlist", "comment"
    resource_id: str
    action: EditAction
    before_state: dict | None
    after_state: dict | None
    edit_summary: str = ""
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class SharedItem:
    """An item shared in a workspace (strategy, watchlist, etc.)."""

    item_id: str
    workspace_id: str
    owner_id: str
    item_type: str  # "strategy", "watchlist", "factor", "dataset"
    item_ref: str  # reference to the actual item
    name: str
    shared_with: tuple[str, ...] = field(default_factory=tuple)
    permission: WorkspaceRole = WorkspaceRole.VIEWER
    version: int = 1
    created_at: str = field(default_factory=_now)
    updated_at: str = field(default_factory=_now)


@dataclass(slots=True)
class WorkspaceDiscussion:
    """A threaded discussion in a workspace."""

    discussion_id: str
    workspace_id: str
    resource_id: str | None  # Associated resource, if any
    user_id: str
    title: str
    content: str
    is_resolved: bool = False
    reply_count: int = 0
    created_at: str = field(default_factory=_now)


@dataclass(slots=True)
class DiscussionReply:
    """A reply in a workspace discussion thread."""

    reply_id: str
    discussion_id: str
    user_id: str
    content: str
    created_at: str = field(default_factory=_now)


# ─────────────────────────────────────────────────────────────────────────────
# Collaboration Service
# ─────────────────────────────────────────────────────────────────────────────

class CollaborationService:
    """Real-time collaboration workspace service (COLLAB-01 ~ COLLAB-04).

    Provides:
    - Shared workspace with role-based permissions
    - Shared strategy/watchlist/factor libraries
    - Full edit history and permission audit trail
    - Threaded discussions and reviews
    """

    def __init__(self, persistence=None) -> None:
        self.persistence = persistence
        self._workspaces: dict[str, Workspace] = {}
        self._members: dict[str, list[WorkspaceMember]] = defaultdict(list)  # workspace_id -> members
        self._shared_items: dict[str, SharedItem] = {}
        self._edit_records: list[EditRecord] = []
        self._discussions: dict[str, WorkspaceDiscussion] = {}
        _replies: dict[str, list[DiscussionReply]] = defaultdict(list)  # discussion_id -> replies
        self._pending_edits: dict[str, dict] = {}  # item_id -> pending edit

    # ── Workspace Management ─────────────────────────────────────────────────

    def create_workspace(
        self,
        name: str,
        owner_id: str,
        description: str = "",
    ) -> Workspace:
        """Create a new collaborative workspace."""
        workspace_id = f"ws:{uuid.uuid4().hex[:12]}"
        workspace = Workspace(
            workspace_id=workspace_id,
            name=name,
            description=description,
            owner_id=owner_id,
        )
        self._workspaces[workspace_id] = workspace
        self._members[workspace_id].append(
            WorkspaceMember(
                workspace_id=workspace_id,
                user_id=owner_id,
                role=WorkspaceRole.OWNER,
            )
        )
        return workspace

    def get_workspace(self, workspace_id: str) -> Workspace | None:
        """Get a workspace by ID."""
        ws = self._workspaces.get(workspace_id)
        if ws and ws.is_deleted:
            return None
        return ws

    def get_user_workspaces(self, user_id: str) -> list[Workspace]:
        """Get all workspaces a user belongs to."""
        result = []
        for ws in self._workspaces.values():
            if ws.is_deleted:
                continue
            members = self._members.get(ws.workspace_id, [])
            if any(m.user_id == user_id for m in members):
                result.append(ws)
        return result

    def delete_workspace(self, workspace_id: str, user_id: str) -> bool:
        """Delete a workspace (owner only)."""
        ws = self._workspaces.get(workspace_id)
        if not ws or ws.owner_id != user_id:
            return False
        ws.is_deleted = True
        ws.updated_at = _now()
        return True

    # ── Member Management ─────────────────────────────────────────────────

    def invite_member(
        self,
        workspace_id: str,
        user_id: str,
        role: WorkspaceRole,
        inviter_id: str,
    ) -> WorkspaceMember | None:
        """Invite a user to a workspace."""
        ws = self._workspaces.get(workspace_id)
        if not ws or ws.is_deleted:
            return None

        # Check if already a member
        for member in self._members[workspace_id]:
            if member.user_id == user_id:
                return None  # Already a member

        member = WorkspaceMember(
            workspace_id=workspace_id,
            user_id=user_id,
            role=role,
        )
        self._members[workspace_id].append(member)

        # Record edit
        self._record_edit(
            workspace_id=workspace_id,
            user_id=inviter_id,
            resource_type="member",
            resource_id=user_id,
            action=EditAction.CREATED,
            before_state=None,
            after_state={"role": role.value},
            edit_summary=f"Invited user {user_id} as {role.value}",
        )
        return member

    def update_member_role(
        self,
        workspace_id: str,
        user_id: str,
        new_role: WorkspaceRole,
        changer_id: str,
    ) -> bool:
        """Update a member's role in a workspace."""
        for member in self._members[workspace_id]:
            if member.user_id == user_id:
                old_role = member.role
                member.role = new_role
                self._record_edit(
                    workspace_id=workspace_id,
                    user_id=changer_id,
                    resource_type="member",
                    resource_id=user_id,
                    action=EditAction.PERMISSION_CHANGED,
                    before_state={"role": old_role.value},
                    after_state={"role": new_role.value},
                    edit_summary=f"Changed role from {old_role.value} to {new_role.value}",
                )
                return True
        return False

    def remove_member(self, workspace_id: str, user_id: str, remover_id: str) -> bool:
        """Remove a member from a workspace."""
        members = self._members[workspace_id]
        for i, member in enumerate(members):
            if member.user_id == user_id:
                self._record_edit(
                    workspace_id=workspace_id,
                    user_id=remover_id,
                    resource_type="member",
                    resource_id=user_id,
                    action=EditAction.DELETED,
                    before_state={"role": member.role.value},
                    after_state=None,
                    edit_summary=f"Removed user {user_id}",
                )
                members.pop(i)
                return True
        return False

    def get_members(self, workspace_id: str) -> list[WorkspaceMember]:
        """Get all members of a workspace."""
        return self._members.get(workspace_id, [])

    def get_user_role(self, workspace_id: str, user_id: str) -> WorkspaceRole | None:
        """Get a user's role in a workspace."""
        for member in self._members.get(workspace_id, []):
            if member.user_id == user_id:
                return member.role
        return None

    # ── Shared Items ─────────────────────────────────────────────────────

    def share_item(
        self,
        workspace_id: str,
        owner_id: str,
        item_type: str,
        item_ref: str,
        name: str,
        permission: WorkspaceRole = WorkspaceRole.VIEWER,
    ) -> SharedItem:
        """Share an item (strategy, watchlist, factor, dataset) in a workspace."""
        item_id = f"item:{uuid.uuid4().hex[:12]}"
        item = SharedItem(
            item_id=item_id,
            workspace_id=workspace_id,
            owner_id=owner_id,
            item_type=item_type,
            item_ref=item_ref,
            name=name,
            permission=permission,
        )
        self._shared_items[item_id] = item
        self._record_edit(
            workspace_id=workspace_id,
            user_id=owner_id,
            resource_type=item_type,
            resource_id=item_id,
            action=EditAction.CREATED,
            before_state=None,
            after_state=asdict(item),
            edit_summary=f"Shared {item_type}: {name}",
        )
        return item

    def update_shared_item(
        self,
        item_id: str,
        user_id: str,
        changes: dict,
    ) -> SharedItem | None:
        """Update a shared item (editor/owner only)."""
        item = self._shared_items.get(item_id)
        if not item:
            return None

        # Check permission
        role = self.get_user_role(item.workspace_id, user_id)
        if role not in (WorkspaceRole.OWNER, WorkspaceRole.EDITOR):
            return None

        before = asdict(item)
        for key, value in changes.items():
            if hasattr(item, key):
                setattr(item, key, value)
        item.version += 1
        item.updated_at = _now()

        self._record_edit(
            workspace_id=item.workspace_id,
            user_id=user_id,
            resource_type=item.item_type,
            resource_id=item_id,
            action=EditAction.UPDATED,
            before_state=before,
            after_state=asdict(item),
            edit_summary=f"Updated {item.item_type}: {item.name}",
        )
        return item

    def delete_shared_item(self, item_id: str, user_id: str) -> bool:
        """Delete a shared item from a workspace."""
        item = self._shared_items.get(item_id)
        if not item:
            return False

        role = self.get_user_role(item.workspace_id, user_id)
        if role not in (WorkspaceRole.OWNER, WorkspaceRole.EDITOR):
            return False

        self._record_edit(
            workspace_id=item.workspace_id,
            user_id=user_id,
            resource_type=item.item_type,
            resource_id=item_id,
            action=EditAction.DELETED,
            before_state=asdict(item),
            after_state=None,
            edit_summary=f"Deleted {item.item_type}: {item.name}",
        )
        del self._shared_items[item_id]
        return True

    def get_shared_items(
        self,
        workspace_id: str,
        item_type: str | None = None,
    ) -> list[SharedItem]:
        """Get all shared items in a workspace."""
        return [
            item for item in self._shared_items.values()
            if item.workspace_id == workspace_id
            and (item_type is None or item.item_type == item_type)
        ]

    # ── Edit History ─────────────────────────────────────────────────────

    def _record_edit(
        self,
        workspace_id: str,
        user_id: str,
        resource_type: str,
        resource_id: str,
        action: EditAction,
        before_state: dict | None,
        after_state: dict | None,
        edit_summary: str,
    ) -> None:
        """Record an edit in the workspace audit trail."""
        record = EditRecord(
            edit_id=f"edit:{uuid.uuid4().hex[:12]}",
            workspace_id=workspace_id,
            user_id=user_id,
            resource_type=resource_type,
            resource_id=resource_id,
            action=action,
            before_state=before_state,
            after_state=after_state,
            edit_summary=edit_summary,
        )
        self._edit_records.append(record)

    def get_edit_history(
        self,
        workspace_id: str,
        resource_id: str | None = None,
        user_id: str | None = None,
        limit: int = 100,
    ) -> list[EditRecord]:
        """Get edit history for a workspace, optionally filtered."""
        results = []
        for record in reversed(self._edit_records):
            if record.workspace_id != workspace_id:
                continue
            if resource_id and record.resource_id != resource_id:
                continue
            if user_id and record.user_id != user_id:
                continue
            results.append(record)
            if len(results) >= limit:
                break
        return results

    # ── Discussions ─────────────────────────────────────────────────────

    def create_discussion(
        self,
        workspace_id: str,
        user_id: str,
        title: str,
        content: str,
        resource_id: str | None = None,
    ) -> WorkspaceDiscussion:
        """Create a discussion thread in a workspace."""
        discussion_id = f"disc:{uuid.uuid4().hex[:12]}"
        discussion = WorkspaceDiscussion(
            discussion_id=discussion_id,
            workspace_id=workspace_id,
            resource_id=resource_id,
            user_id=user_id,
            title=title,
            content=content,
        )
        self._discussions[discussion_id] = discussion
        self._record_edit(
            workspace_id=workspace_id,
            user_id=user_id,
            resource_type="discussion",
            resource_id=discussion_id,
            action=EditAction.CREATED,
            before_state=None,
            after_state={"title": title},
            edit_summary=f"Created discussion: {title}",
        )
        return discussion

    def add_reply(
        self,
        discussion_id: str,
        user_id: str,
        content: str,
    ) -> DiscussionReply | None:
        """Add a reply to a discussion thread."""
        discussion = self._discussions.get(discussion_id)
        if not discussion:
            return None

        reply = DiscussionReply(
            reply_id=f"reply:{uuid.uuid4().hex[:12]}",
            discussion_id=discussion_id,
            user_id=user_id,
            content=content,
        )
        discussion.reply_count += 1

        # Store in instance variable
        if not hasattr(self, "_replies"):
            self._replies: dict[str, list[DiscussionReply]] = defaultdict(list)
        self._replies[discussion_id].append(reply)

        self._record_edit(
            workspace_id=discussion.workspace_id,
            user_id=user_id,
            resource_type="discussion_reply",
            resource_id=reply.reply_id,
            action=EditAction.COMMENTED,
            before_state=None,
            after_state={"content": content[:50]},
            edit_summary=f"Replied to discussion {discussion_id}",
        )
        return reply

    def get_discussion(self, discussion_id: str) -> WorkspaceDiscussion | None:
        """Get a discussion by ID."""
        return self._discussions.get(discussion_id)

    def get_workspace_discussions(
        self,
        workspace_id: str,
        resource_id: str | None = None,
    ) -> list[WorkspaceDiscussion]:
        """Get all discussions in a workspace."""
        results = []
        for disc in self._discussions.values():
            if disc.workspace_id != workspace_id:
                continue
            if resource_id and disc.resource_id != resource_id:
                continue
            results.append(disc)
        return sorted(results, key=lambda d: d.created_at, reverse=True)

    def resolve_discussion(self, discussion_id: str, user_id: str) -> bool:
        """Mark a discussion as resolved."""
        disc = self._discussions.get(discussion_id)
        if not disc:
            return False
        disc.is_resolved = True
        return True

    def get_replies(self, discussion_id: str) -> list[DiscussionReply]:
        """Get all replies for a discussion."""
        return getattr(self, "_replies", {}).get(discussion_id, [])
