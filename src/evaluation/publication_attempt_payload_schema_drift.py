"""Report schema drift in recent publication attempt payload metadata."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
import json
import sqlite3
from typing import Any, Mapping


DEFAULT_DAYS = 7
DEFAULT_COMMON_KEY_RATIO = 0.8
DEFAULT_RARE_KEY_RATIO = 0.2


@dataclass(frozen=True)
class MalformedPublicationAttemptPayload:
    """A payload row that could not be decoded as JSON."""

    attempt_id: int
    platform: str
    status: str
    attempted_at: str | None
    error: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class PublicationAttemptPayloadKeyObservation:
    """Payload key drift observations for one platform/status bucket."""

    platform: str
    status: str
    payload_count: int
    common_keys: tuple[str, ...]
    missing_common_keys: tuple[str, ...]
    rare_keys: tuple[str, ...]
    observed_keys: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        for key in ("common_keys", "missing_common_keys", "observed_keys", "rare_keys"):
            payload[key] = list(payload[key])
        return payload


@dataclass(frozen=True)
class PublicationAttemptPayloadSchemaDriftReport:
    """Read-only publication attempt payload schema drift report."""

    artifact_type: str
    generated_at: str
    filters: dict[str, Any]
    totals: dict[str, Any]
    observations: tuple[PublicationAttemptPayloadKeyObservation, ...]
    malformed_payloads: tuple[MalformedPublicationAttemptPayload, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": self.artifact_type,
            "filters": dict(self.filters),
            "generated_at": self.generated_at,
            "malformed_payloads": [item.to_dict() for item in self.malformed_payloads],
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted((self.missing_columns or {}).items())
            },
            "missing_tables": list(self.missing_tables),
            "observations": [item.to_dict() for item in self.observations],
            "totals": dict(sorted(self.totals.items())),
        }


def build_publication_attempt_payload_schema_drift_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    common_key_ratio: float = DEFAULT_COMMON_KEY_RATIO,
    rare_key_ratio: float = DEFAULT_RARE_KEY_RATIO,
    now: datetime | None = None,
) -> PublicationAttemptPayloadSchemaDriftReport:
    """Inspect recent attempt payload JSON and flag platform/status key drift."""
    if days <= 0:
        raise ValueError("days must be positive")
    if not 0 < common_key_ratio <= 1:
        raise ValueError("common_key_ratio must be between 0 and 1")
    if not 0 <= rare_key_ratio < 1:
        raise ValueError("rare_key_ratio must be at least 0 and less than 1")

    generated_at = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = (generated_at - timedelta(days=days)).isoformat()
    conn = _connection(db_or_conn)
    schema = _schema(conn)
    required = {"id", "platform", "attempted_at", "success", "response_metadata"}
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = {}
    rows: list[Mapping[str, Any]] = []
    if "publication_attempts" not in schema:
        missing_tables = ("publication_attempts",)
    else:
        missing = tuple(sorted(required - schema["publication_attempts"]))
        if missing:
            missing_columns["publication_attempts"] = missing
        else:
            rows = _load_rows(conn, cutoff)

    observations, malformed = _analyze_rows(
        rows,
        common_key_ratio=common_key_ratio,
        rare_key_ratio=rare_key_ratio,
    )
    return PublicationAttemptPayloadSchemaDriftReport(
        artifact_type="publication_attempt_payload_schema_drift",
        generated_at=generated_at.isoformat(),
        filters={
            "common_key_ratio": common_key_ratio,
            "days": days,
            "rare_key_ratio": rare_key_ratio,
        },
        totals={
            "bucket_count": len(observations),
            "malformed_payload_count": len(malformed),
            "payload_count": sum(item.payload_count for item in observations),
            "row_count": len(rows),
        },
        observations=tuple(observations),
        malformed_payloads=tuple(malformed),
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def format_publication_attempt_payload_schema_drift_json(
    report: PublicationAttemptPayloadSchemaDriftReport,
) -> str:
    """Serialize the report as deterministic JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_publication_attempt_payload_schema_drift_text(
    report: PublicationAttemptPayloadSchemaDriftReport,
) -> str:
    """Render a concise text report."""
    lines = [
        "Publication Attempt Payload Schema Drift",
        f"Generated: {report.generated_at}",
        f"Window: {report.filters['days']} days",
        (
            f"Buckets: {report.totals['bucket_count']} "
            f"payloads={report.totals['payload_count']} "
            f"malformed={report.totals['malformed_payload_count']}"
        ),
    ]
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        lines.append(
            "Missing columns: "
            + "; ".join(
                f"{table}({', '.join(columns)})"
                for table, columns in sorted(report.missing_columns.items())
            )
        )
    if not report.observations and not report.malformed_payloads:
        lines.append("No payload schema drift observations found.")
        return "\n".join(lines)
    for item in report.observations:
        lines.append(
            f"- platform={item.platform} status={item.status} payloads={item.payload_count} "
            f"missing_common={','.join(item.missing_common_keys) or '-'} "
            f"rare={','.join(item.rare_keys) or '-'}"
        )
    if report.malformed_payloads:
        lines.append("Malformed payloads:")
        for item in report.malformed_payloads:
            lines.append(
                f"- attempt={item.attempt_id} platform={item.platform} "
                f"status={item.status} at={item.attempted_at or '-'} error={item.error}"
            )
    return "\n".join(lines)


def _load_rows(conn: sqlite3.Connection, cutoff: str) -> list[sqlite3.Row]:
    return conn.execute(
        """SELECT id, platform, attempted_at, success, response_metadata
           FROM publication_attempts
           WHERE attempted_at >= ?
           ORDER BY platform ASC, success ASC, attempted_at ASC, id ASC""",
        (cutoff,),
    ).fetchall()


def _analyze_rows(
    rows: list[Mapping[str, Any]],
    *,
    common_key_ratio: float,
    rare_key_ratio: float,
) -> tuple[list[PublicationAttemptPayloadKeyObservation], list[MalformedPublicationAttemptPayload]]:
    grouped: dict[tuple[str, str], list[set[str]]] = defaultdict(list)
    malformed: list[MalformedPublicationAttemptPayload] = []
    for raw_row in rows:
        row = dict(raw_row)
        platform = _label(row.get("platform"))
        status = "success" if bool(row.get("success")) else "failure"
        decoded, error = _decode(row.get("response_metadata"))
        if error:
            malformed.append(
                MalformedPublicationAttemptPayload(
                    attempt_id=int(row["id"]),
                    platform=platform,
                    status=status,
                    attempted_at=row.get("attempted_at"),
                    error=error,
                )
            )
            continue
        if decoded is None:
            continue
        grouped[(platform, status)].append(set(decoded))

    observations: list[PublicationAttemptPayloadKeyObservation] = []
    for (platform, status), key_sets in grouped.items():
        counts = Counter(key for keys in key_sets for key in keys)
        total = len(key_sets)
        common = {
            key for key, count in counts.items() if count / total >= common_key_ratio
        }
        rare = tuple(sorted(key for key, count in counts.items() if count / total <= rare_key_ratio))
        missing = tuple(
            sorted(
                key
                for key in common
                if any(key not in keys for keys in key_sets)
            )
        )
        observations.append(
            PublicationAttemptPayloadKeyObservation(
                platform=platform,
                status=status,
                payload_count=total,
                common_keys=tuple(sorted(common)),
                missing_common_keys=missing,
                rare_keys=rare,
                observed_keys=tuple(sorted(counts)),
            )
        )
    observations.sort(key=lambda item: (item.platform, item.status))
    malformed.sort(key=lambda item: (item.platform, item.status, item.attempted_at or "", item.attempt_id))
    return observations, malformed


def _decode(value: Any) -> tuple[tuple[str, ...] | None, str | None]:
    if value in (None, ""):
        return None, None
    if isinstance(value, Mapping):
        payload = value
    else:
        try:
            payload = json.loads(str(value))
        except json.JSONDecodeError as exc:
            return None, exc.msg
    if not isinstance(payload, Mapping):
        return None, "payload is not an object"
    return tuple(str(key) for key in payload), None


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type = 'table'").fetchall()
    return {
        row[0]: {column[1] for column in conn.execute(f"PRAGMA table_info({row[0]})")}
        for row in rows
    }


def _label(value: Any) -> str:
    return str(value or "unknown").strip().casefold() or "unknown"


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
