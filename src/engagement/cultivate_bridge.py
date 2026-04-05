"""Bridge to cultivate's relationship network intelligence DB.

Reads cultivate's SQLite database directly via raw SQL — no cultivate package
import required. Presence works fine when cultivate is unavailable (all methods
return None/empty defaults).

Write surface: record_mention_event() forwards mentions to cultivate's events
and interactions tables so cultivate's analysis pipeline stays up to date.
"""

import json
import sqlite3
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from uuid import uuid4

# Stage names for display
_STAGE_NAMES = {
    0: "Observation",
    1: "Ambient",
    2: "Light",
    3: "Active",
    4: "Relationship",
    5: "Alliance",
}

_TIER_NAMES = {
    1: "Inner Circle",
    2: "Key Network",
    3: "Active Network",
    4: "Wider Network",
}


@dataclass
class PersonContext:
    """Relationship context for a person, sourced from cultivate."""

    x_handle: str
    display_name: str
    bio: Optional[str]
    relationship_strength: Optional[float]
    engagement_stage: Optional[int]
    dunbar_tier: Optional[int]
    authenticity_score: Optional[float]
    content_quality_score: Optional[float]
    content_relevance_score: Optional[float]
    recent_interactions: list[dict] = field(default_factory=list)
    is_known: bool = True

    @property
    def stage_name(self) -> str:
        return _STAGE_NAMES.get(self.engagement_stage, "Unknown")

    @property
    def tier_name(self) -> str:
        return _TIER_NAMES.get(self.dunbar_tier, "Unknown")

    def to_json(self) -> str:
        """Serialize for storage in presence's reply_queue."""
        return json.dumps({
            "x_handle": self.x_handle,
            "display_name": self.display_name,
            "bio": self.bio,
            "relationship_strength": self.relationship_strength,
            "engagement_stage": self.engagement_stage,
            "dunbar_tier": self.dunbar_tier,
            "authenticity_score": self.authenticity_score,
            "content_quality_score": self.content_quality_score,
            "content_relevance_score": self.content_relevance_score,
            "recent_interactions": self.recent_interactions,
            "is_known": self.is_known,
        })

    @classmethod
    def from_json(cls, data: str) -> "PersonContext":
        """Deserialize from reply_queue storage."""
        d = json.loads(data)
        return cls(**d)


@dataclass
class ProactiveAction:
    """A cultivate-suggested engagement action."""

    action_id: str
    action_type: str
    target_handle: str
    target_person_id: str
    description: str
    payload: Optional[dict] = None
    person_context: Optional[PersonContext] = None


class CultivateBridge:
    """Read-only bridge to cultivate's SQLite database.

    Uses raw SQL queries — does not import cultivate's package.
    """

    # Required tables to validate schema
    _REQUIRED_TABLES = {"people", "interactions", "events", "actions", "meta"}

    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self._self_person_id: Optional[str] = None

    @classmethod
    def try_connect(
        cls, db_path: str = "~/.cultivate/cultivate.db"
    ) -> Optional["CultivateBridge"]:
        """Attempt to connect to cultivate's DB. Returns None if unavailable."""
        path = Path(db_path).expanduser()
        if not path.exists():
            return None

        try:
            conn = sqlite3.connect(str(path))
            conn.row_factory = sqlite3.Row

            # Validate schema has expected tables
            cursor = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
            tables = {row["name"] for row in cursor.fetchall()}
            if not cls._REQUIRED_TABLES.issubset(tables):
                conn.close()
                return None

            return cls(conn)
        except (sqlite3.Error, OSError):
            return None

    @property
    def self_person_id(self) -> Optional[str]:
        """Cached lookup of the self person's ID in cultivate."""
        if self._self_person_id is None:
            row = self.conn.execute(
                "SELECT id FROM people WHERE is_self = 1"
            ).fetchone()
            if row:
                self._self_person_id = row["id"]
        return self._self_person_id

    def get_person_context(self, x_handle: str) -> Optional[PersonContext]:
        """Look up relationship context for a person by X handle."""
        handle = x_handle.lstrip("@")
        row = self.conn.execute(
            "SELECT * FROM people WHERE x_handle = ?", (handle,)
        ).fetchone()
        if not row:
            return None
        return self._build_person_context(row)

    def get_person_context_by_x_id(self, x_user_id: str) -> Optional[PersonContext]:
        """Look up relationship context by X user ID."""
        row = self.conn.execute(
            "SELECT * FROM people WHERE x_user_id = ?", (x_user_id,)
        ).fetchone()
        if not row:
            return None
        return self._build_person_context(row)

    def _build_person_context(self, person_row: sqlite3.Row) -> PersonContext:
        """Build PersonContext from a people table row + recent interactions."""
        person_id = person_row["id"]
        self_id = self.self_person_id

        interactions = []
        if self_id and person_id != self_id:
            rows = self.conn.execute(
                """SELECT interaction_type, content_snippet, occurred_at,
                          actor_person_id, target_person_id
                   FROM interactions
                   WHERE (actor_person_id = ? AND target_person_id = ?)
                      OR (actor_person_id = ? AND target_person_id = ?)
                   ORDER BY occurred_at DESC
                   LIMIT 10""",
                (person_id, self_id, self_id, person_id),
            ).fetchall()

            for ix in rows:
                if ix["actor_person_id"] == self_id:
                    direction = "me \u2192 them"
                else:
                    direction = "them \u2192 me"
                interactions.append({
                    "type": ix["interaction_type"],
                    "snippet": ix["content_snippet"] or "",
                    "date": ix["occurred_at"],
                    "direction": direction,
                })

        return PersonContext(
            x_handle=person_row["x_handle"],
            display_name=person_row["display_name"],
            bio=person_row["bio"],
            relationship_strength=person_row["relationship_strength"],
            engagement_stage=person_row["engagement_stage"],
            dunbar_tier=person_row["dunbar_tier"],
            authenticity_score=person_row["authenticity_score"],
            content_quality_score=person_row["content_quality_score"],
            content_relevance_score=person_row["content_relevance_score"],
            recent_interactions=interactions,
            is_known=True,
        )

    def get_pending_proactive_actions(
        self, limit: int = 20
    ) -> list[ProactiveAction]:
        """Get cultivate's suggested engagement actions with person context."""
        rows = self.conn.execute(
            """SELECT a.id, a.action_type, a.target_person_id, a.description,
                      a.payload,
                      p.x_handle, p.display_name, p.bio,
                      p.relationship_strength, p.engagement_stage, p.dunbar_tier,
                      p.authenticity_score, p.content_quality_score,
                      p.content_relevance_score
               FROM actions a
               JOIN people p ON a.target_person_id = p.id
               WHERE a.status = 'suggested'
               ORDER BY a.created_at
               LIMIT ?""",
            (limit,),
        ).fetchall()

        actions = []
        for row in rows:
            payload = None
            if row["payload"]:
                try:
                    payload = json.loads(row["payload"])
                except json.JSONDecodeError:
                    pass

            person_ctx = PersonContext(
                x_handle=row["x_handle"],
                display_name=row["display_name"],
                bio=row["bio"],
                relationship_strength=row["relationship_strength"],
                engagement_stage=row["engagement_stage"],
                dunbar_tier=row["dunbar_tier"],
                authenticity_score=row["authenticity_score"],
                content_quality_score=row["content_quality_score"],
                content_relevance_score=row["content_relevance_score"],
                is_known=True,
            )

            actions.append(ProactiveAction(
                action_id=row["id"],
                action_type=row["action_type"],
                target_handle=row["x_handle"],
                target_person_id=row["target_person_id"],
                description=row["description"],
                payload=payload,
                person_context=person_ctx,
            ))

        return actions

    def record_mention_event(
        self,
        tweet_id: str,
        author_x_id: str,
        author_handle: str,
        text: str,
        created_at: str,
    ) -> None:
        """Forward a mention to cultivate's events + interactions tables."""
        now = datetime.now(timezone.utc).isoformat()
        event_id = uuid4().hex

        # Insert event
        self.conn.execute(
            """INSERT INTO events (id, event_type, source, payload, detected_at, processed)
               VALUES (?, 'mention', 'presence_poll', ?, ?, 0)""",
            (
                event_id,
                json.dumps({
                    "tweet_id": tweet_id,
                    "author_id": author_x_id,
                    "author_handle": author_handle,
                    "text": text,
                    "created_at": created_at,
                }),
                now,
            ),
        )

        # If author is known in cultivate, also insert interaction
        self_id = self.self_person_id
        if self_id:
            author_row = self.conn.execute(
                "SELECT id FROM people WHERE x_user_id = ?", (author_x_id,)
            ).fetchone()
            if author_row:
                # Check for duplicate (same tweet + type)
                existing = self.conn.execute(
                    "SELECT id FROM interactions WHERE x_tweet_id = ? AND interaction_type = ?",
                    (tweet_id, "mention"),
                ).fetchone()
                if not existing:
                    self.conn.execute(
                        """INSERT INTO interactions
                           (id, actor_person_id, target_person_id, interaction_type,
                            x_tweet_id, content_snippet, occurred_at, ingested_at)
                           VALUES (?, ?, ?, 'mention', ?, ?, ?, ?)""",
                        (
                            uuid4().hex,
                            author_row["id"],
                            self_id,
                            tweet_id,
                            text[:280] if text else None,
                            created_at or now,
                            now,
                        ),
                    )

        self.conn.commit()

    def mark_action_completed(self, action_id: str) -> None:
        """Mark a cultivate action as completed."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE actions SET status = 'completed', completed_at = ? WHERE id = ?",
            (now, action_id),
        )
        self.conn.commit()

    def mark_action_dismissed(self, action_id: str) -> None:
        """Mark a cultivate action as dismissed."""
        now = datetime.now(timezone.utc).isoformat()
        self.conn.execute(
            "UPDATE actions SET status = 'dismissed', completed_at = ? WHERE id = ?",
            (now, action_id),
        )
        self.conn.commit()

    def close(self) -> None:
        if self.conn:
            self.conn.close()
            self.conn = None
