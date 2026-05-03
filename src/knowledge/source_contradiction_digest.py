"""Digest potential contradictions across curated knowledge sources."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import json
import re
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_LIMIT = 20

_WORD_RE = re.compile(r"[a-z0-9][a-z0-9.+#-]*")
_SENTENCE_RE = re.compile(r"(?<=[.!?])\s+|\n+")
_NUMBER_RE = re.compile(
    r"""
    (?P<value>
        \$\s*\d+(?:[,.]\d+)*(?:\.\d+)?
        |
        \b\d+(?:[,.]\d+)*(?:\.\d+)?
        |
        \b(?:zero|one|two|three|four|five|six|seven|eight|nine|ten)\b
    )
    \s*
    (?P<unit>
        %|x|percent|percentage|times|ms|s|sec|secs|seconds?|minutes?|hours?|
        days?|weeks?|months?|years?|versions?|repos?|repositories|commits?|
        files?|tests?|errors?|requests?|users?|tokens?|lines?
    )?
    """,
    re.IGNORECASE | re.VERBOSE,
)
_VERSION_RE = re.compile(
    r"\b(?P<name>[A-Z][A-Za-z0-9.+#-]{1,30})\s+"
    r"(?P<version>v?\d+(?:\.\d+){1,3}(?:-[A-Za-z0-9.]+)?)\b"
)
_NEGATION_RE = re.compile(
    r"\b(?:cannot|can't|does\s+not|doesn't|do\s+not|don't|did\s+not|didn't|"
    r"is\s+not|isn't|are\s+not|aren't|was\s+not|wasn't|were\s+not|weren't|"
    r"has\s+not|hasn't|have\s+not|haven't|had\s+not|hadn't|never|no|not|"
    r"without|unsupported|unavailable|disabled|blocked|fails?|failed)\b",
    re.IGNORECASE,
)
_AFFIRMATION_RE = re.compile(
    r"\b(?:can|does|do|did|is|are|was|were|has|have|had|supports?|supported|"
    r"available|enabled|allows?|allowed|works?|worked|succeeds?|succeeded)\b",
    re.IGNORECASE,
)
_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "because",
    "before",
    "being",
    "between",
    "but",
    "can",
    "cannot",
    "content",
    "curated",
    "did",
    "does",
    "from",
    "had",
    "has",
    "have",
    "insight",
    "into",
    "its",
    "knowledge",
    "more",
    "new",
    "not",
    "now",
    "only",
    "over",
    "says",
    "source",
    "that",
    "the",
    "this",
    "through",
    "today",
    "using",
    "was",
    "were",
    "when",
    "with",
}
_NUMBER_WORDS = {
    "zero": Decimal("0"),
    "one": Decimal("1"),
    "two": Decimal("2"),
    "three": Decimal("3"),
    "four": Decimal("4"),
    "five": Decimal("5"),
    "six": Decimal("6"),
    "seven": Decimal("7"),
    "eight": Decimal("8"),
    "nine": Decimal("9"),
    "ten": Decimal("10"),
}


@dataclass(frozen=True)
class SourceContradictionPair:
    left_source_id: int
    right_source_id: int
    left_source_type: str | None
    right_source_type: str | None
    left_excerpt: str
    right_excerpt: str
    topic: str
    conflict_type: str
    confidence_score: float
    left_source_trust: float | None = None
    right_source_trust: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "confidence_score": self.confidence_score,
            "conflict_type": self.conflict_type,
            "left_excerpt": self.left_excerpt,
            "left_source_id": self.left_source_id,
            "left_source_trust": self.left_source_trust,
            "left_source_type": self.left_source_type,
            "right_excerpt": self.right_excerpt,
            "right_source_id": self.right_source_id,
            "right_source_trust": self.right_source_trust,
            "right_source_type": self.right_source_type,
            "topic": self.topic,
        }


@dataclass(frozen=True)
class SourceContradictionDigestReport:
    generated_at: str
    filters: dict[str, Any]
    total_rows: int
    pair_count: int
    pairs: list[SourceContradictionPair]
    metadata: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "artifact_type": "source_contradiction_digest",
            "filters": self.filters,
            "generated_at": self.generated_at,
            "metadata": self.metadata,
            "pair_count": self.pair_count,
            "pairs": [pair.to_dict() for pair in self.pairs],
            "total_rows": self.total_rows,
        }


@dataclass(frozen=True)
class _Claim:
    kind: str
    value: str
    comparable: Decimal | str
    label: str | None
    unit: str | None
    sentence: str
    terms: frozenset[str]


def build_source_contradiction_digest_report(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    source_type: str | None = None,
    limit: int = DEFAULT_LIMIT,
    now: datetime | None = None,
) -> SourceContradictionDigestReport:
    """Return ranked contradiction candidates among recent curated knowledge rows."""
    if days < 1:
        raise ValueError("days must be at least 1")
    if limit < 1:
        raise ValueError("limit must be at least 1")

    conn = _connection(db_or_conn)
    generated_at = _parse_now(now)
    metadata = _schema_metadata(conn)
    filters = {"days": days, "limit": limit, "source_type": source_type}
    if not metadata["availability"]["knowledge"]:
        return SourceContradictionDigestReport(
            generated_at=generated_at.isoformat(),
            filters=filters,
            total_rows=0,
            pair_count=0,
            pairs=[],
            metadata=metadata,
        )

    rows = _fetch_rows(conn, columns=set(metadata["columns"]["knowledge"]), days=days, source_type=source_type, now=generated_at)
    candidates: list[SourceContradictionPair] = []
    for index, left in enumerate(rows):
        for right in rows[index + 1 :]:
            if _same_source(left, right):
                continue
            topic = _shared_topic(left, right)
            if not topic:
                continue
            pair = _detect_pair_conflict(left, right, topic=topic, now=generated_at)
            if pair is not None:
                candidates.append(pair)

    candidates.sort(
        key=lambda pair: (
            -pair.confidence_score,
            pair.conflict_type,
            pair.left_source_id,
            pair.right_source_id,
        )
    )
    pairs = candidates[:limit]
    return SourceContradictionDigestReport(
        generated_at=generated_at.isoformat(),
        filters=filters,
        total_rows=len(rows),
        pair_count=len(pairs),
        pairs=pairs,
        metadata=metadata,
    )


def _connection(db_or_conn: Any) -> sqlite3.Connection:
    return getattr(db_or_conn, "conn", db_or_conn)


def format_source_contradiction_digest_json(report: SourceContradictionDigestReport) -> str:
    """Format a digest report as stable JSON."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_source_contradiction_digest_text(report: SourceContradictionDigestReport) -> str:
    """Format a digest report for terminal review."""
    lines = ["Source Contradiction Digest"]
    lines.append(f"Rows scanned: {report.total_rows}")
    lines.append(f"Candidate pairs: {report.pair_count}")
    missing_tables = [
        name for name, available in report.metadata.get("availability", {}).items() if not available
    ]
    if missing_tables:
        lines.append(f"Missing tables: {', '.join(missing_tables)}")
    missing_columns = report.metadata.get("missing_columns", {}).get("knowledge", ())
    if missing_columns:
        lines.append(f"Missing optional knowledge columns: {', '.join(missing_columns)}")
    if not report.pairs:
        lines.append("No likely source contradictions found.")
        return "\n".join(lines)

    for pair in report.pairs:
        lines.append("")
        lines.append(
            f"- {pair.conflict_type} topic={pair.topic} score={pair.confidence_score:.3f} "
            f"#{pair.left_source_id}({pair.left_source_type or 'unknown'}) "
            f"vs #{pair.right_source_id}({pair.right_source_type or 'unknown'})"
        )
        lines.append(f"  left: {pair.left_excerpt}")
        lines.append(f"  right: {pair.right_excerpt}")
    return "\n".join(lines)


def _schema_metadata(conn: sqlite3.Connection) -> dict[str, Any]:
    exists = _table_exists(conn, "knowledge")
    columns = _table_columns(conn, "knowledge") if exists else set()
    optional = (
        "source_id",
        "source_url",
        "author",
        "insight",
        "title",
        "metadata",
        "published_at",
        "ingested_at",
        "created_at",
        "approved",
        "source_trust",
        "trust_score",
        "quality_score",
        "relevance_score",
        "confidence",
    )
    return {
        "availability": {"knowledge": exists},
        "columns": {"knowledge": sorted(columns)},
        "missing_columns": {
            "knowledge": tuple(column for column in optional if column not in columns)
        },
    }


def _table_exists(conn: sqlite3.Connection, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (name,),
    ).fetchone()
    return row is not None


def _table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}


def _fetch_rows(
    conn: sqlite3.Connection,
    *,
    columns: set[str],
    days: int,
    source_type: str | None,
    now: datetime,
) -> list[dict[str, Any]]:
    required = {"id", "source_type", "content"}
    if not required.issubset(columns):
        return []

    timestamp_expr = _timestamp_expression(columns)
    where = ["source_type LIKE 'curated_%'"]
    params: list[Any] = []
    if source_type:
        where.append("source_type = ?")
        params.append(source_type)
    if "approved" in columns:
        where.append("COALESCE(approved, 0) = 1")
    if timestamp_expr != "NULL":
        where.append(f"{timestamp_expr} >= ?")
        params.append((now - timedelta(days=days)).isoformat())

    select_columns = [
        "id",
        "source_type",
        _optional_column(columns, "source_id"),
        _optional_column(columns, "source_url"),
        _optional_column(columns, "author"),
        "content",
        _optional_column(columns, "insight"),
        _optional_column(columns, "title"),
        _optional_column(columns, "metadata"),
        _optional_column(columns, "source_trust"),
        _optional_column(columns, "trust_score"),
        _optional_column(columns, "quality_score"),
        _optional_column(columns, "relevance_score"),
        _optional_column(columns, "confidence"),
        f"{timestamp_expr} AS item_timestamp",
    ]
    cursor = conn.execute(
        f"""SELECT {', '.join(select_columns)}
            FROM knowledge
            WHERE {' AND '.join(where)}
            ORDER BY id""",
        params,
    )
    names = [description[0] for description in cursor.description]
    return [_prepare_row(dict(zip(names, row))) for row in cursor.fetchall()]


def _optional_column(columns: set[str], column: str) -> str:
    return column if column in columns else f"NULL AS {column}"


def _timestamp_expression(columns: set[str]) -> str:
    parts = [column for column in ("published_at", "ingested_at", "created_at") if column in columns]
    if not parts:
        return "NULL"
    if len(parts) == 1:
        return parts[0]
    return f"COALESCE({', '.join(parts)})"


def _prepare_row(row: dict[str, Any]) -> dict[str, Any]:
    row["metadata_dict"] = _parse_metadata(row.get("metadata"))
    row["topic_labels"] = _topic_labels(row)
    row["topic_terms"] = _topic_terms(row)
    row["claims"] = _extract_claims(_claim_text(row))
    row["trust"] = _source_trust(row)
    row["timestamp"] = _parse_datetime(row.get("item_timestamp"))
    return row


def _parse_metadata(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _topic_terms(row: dict[str, Any]) -> frozenset[str]:
    metadata = row.get("metadata_dict") or {}
    parts: list[str] = []
    for key in ("topic", "topics", "tags", "title", "name"):
        value = metadata.get(key)
        if isinstance(value, list):
            parts.extend(str(item) for item in value)
        elif value:
            parts.append(str(value))
    parts.extend(str(row.get(column) or "") for column in ("title", "insight", "content"))
    tokens = [
        token.strip(".+-")
        for token in _WORD_RE.findall(" ".join(parts).lower())
        if token not in _STOPWORDS
    ]
    return frozenset(token for token in tokens if token and not token.replace(".", "").isdigit())


def _topic_labels(row: dict[str, Any]) -> frozenset[str]:
    metadata = row.get("metadata_dict") or {}
    labels: list[str] = []
    for key in ("topic", "topics", "tags"):
        value = metadata.get(key)
        if isinstance(value, list):
            labels.extend(str(item) for item in value)
        elif value:
            labels.append(str(value))
    return frozenset(" ".join(label.lower().split()) for label in labels if str(label).strip())


def _shared_topic(left: dict[str, Any], right: dict[str, Any]) -> str | None:
    shared_labels = sorted(left.get("topic_labels", frozenset()) & right.get("topic_labels", frozenset()))
    if shared_labels:
        return shared_labels[0]
    shared = sorted((left["topic_terms"] & right["topic_terms"]) - _STOPWORDS)
    if len(shared) < 2:
        return None
    preferred = [token for token in shared if len(token) > 2]
    if len(preferred) < 2:
        return None
    return " ".join(preferred[:4])


def _claim_text(row: dict[str, Any]) -> str:
    return " ".join(str(row.get(column) or "") for column in ("title", "insight", "content"))


def _sentences(text: str) -> list[str]:
    return [sentence.strip() for sentence in _SENTENCE_RE.split(text or "") if sentence.strip()]


def _extract_claims(text: str) -> list[_Claim]:
    claims: list[_Claim] = []
    for sentence in _sentences(text):
        terms = frozenset(
            token
            for token in _WORD_RE.findall(sentence.lower())
            if token not in _STOPWORDS and not token.replace(".", "").isdigit()
        )
        for match in _VERSION_RE.finditer(sentence):
            name = match.group("name").lower()
            version = match.group("version").lower().lstrip("v")
            claims.append(
                _Claim(
                    kind="version",
                    value=f"{name} {version}",
                    comparable=f"{name} {version}",
                    label=name,
                    unit=None,
                    sentence=sentence,
                    terms=terms,
                )
            )
        for match in _NUMBER_RE.finditer(sentence):
            parsed = _parse_number(match.group("value"))
            if parsed is None:
                continue
            unit = _normalize_unit(match.group("unit"))
            claims.append(
                _Claim(
                    kind="numeric",
                    value=f"{_format_decimal(parsed)}{unit or ''}",
                    comparable=parsed,
                    label=None,
                    unit=unit,
                    sentence=sentence,
                    terms=terms,
                )
            )
    return claims


def _parse_number(value: str) -> Decimal | None:
    word = value.strip().lower()
    if word in _NUMBER_WORDS:
        return _NUMBER_WORDS[word]
    try:
        return Decimal(word.replace("$", "").replace(",", "").strip())
    except (InvalidOperation, ValueError):
        return None


def _format_decimal(value: Decimal) -> str:
    normalized = value.normalize()
    if normalized == normalized.to_integral():
        return str(normalized.quantize(Decimal("1")))
    return format(normalized, "f")


def _normalize_unit(unit: str | None) -> str | None:
    if not unit:
        return None
    lowered = unit.lower()
    if lowered in {"percent", "percentage"}:
        return "%"
    if lowered in {"sec", "secs", "second", "seconds"}:
        return "s"
    return lowered.rstrip("s")


def _detect_pair_conflict(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    topic: str,
    now: datetime,
) -> SourceContradictionPair | None:
    numeric = _numeric_or_version_conflict(left, right, topic=topic, now=now)
    if numeric is not None:
        return numeric
    return _negation_conflict(left, right, topic=topic, now=now)


def _numeric_or_version_conflict(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    topic: str,
    now: datetime,
) -> SourceContradictionPair | None:
    for left_claim in left["claims"]:
        for right_claim in right["claims"]:
            if left_claim.kind != right_claim.kind:
                continue
            if left_claim.kind == "version" and left_claim.label != right_claim.label:
                continue
            if left_claim.kind == "numeric" and left_claim.unit != right_claim.unit:
                continue
            if left_claim.comparable == right_claim.comparable:
                continue
            if len((left_claim.terms & right_claim.terms) - _STOPWORDS) < 2:
                continue
            return _pair(
                left,
                right,
                topic=topic,
                conflict_type=left_claim.kind,
                left_excerpt=left_claim.sentence,
                right_excerpt=right_claim.sentence,
                now=now,
                base=0.74 if left_claim.kind == "numeric" else 0.8,
            )
    return None


def _negation_conflict(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    topic: str,
    now: datetime,
) -> SourceContradictionPair | None:
    shared = left["topic_terms"] & right["topic_terms"]
    for left_sentence in _sentences(_claim_text(left)):
        left_terms = _sentence_terms(left_sentence)
        if len(left_terms & shared) < 2:
            continue
        left_negated = _has_negation(left_sentence)
        left_affirmed = _has_affirmation(left_sentence)
        for right_sentence in _sentences(_claim_text(right)):
            right_terms = _sentence_terms(right_sentence)
            if len(left_terms & right_terms & shared) < 2:
                continue
            right_negated = _has_negation(right_sentence)
            right_affirmed = _has_affirmation(right_sentence)
            if left_negated == right_negated:
                continue
            if not ((left_negated and right_affirmed) or (right_negated and left_affirmed)):
                continue
            return _pair(
                left,
                right,
                topic=topic,
                conflict_type="negation",
                left_excerpt=left_sentence,
                right_excerpt=right_sentence,
                now=now,
                base=0.68,
            )
    return None


def _sentence_terms(sentence: str) -> frozenset[str]:
    return frozenset(
        token
        for token in _WORD_RE.findall(sentence.lower())
        if token not in _STOPWORDS and not token.replace(".", "").isdigit()
    )


def _has_negation(sentence: str) -> bool:
    return _NEGATION_RE.search(sentence) is not None


def _has_affirmation(sentence: str) -> bool:
    return _AFFIRMATION_RE.search(sentence) is not None


def _pair(
    left: dict[str, Any],
    right: dict[str, Any],
    *,
    topic: str,
    conflict_type: str,
    left_excerpt: str,
    right_excerpt: str,
    now: datetime,
    base: float,
) -> SourceContradictionPair:
    return SourceContradictionPair(
        left_source_id=int(left["id"]),
        right_source_id=int(right["id"]),
        left_source_type=left.get("source_type"),
        right_source_type=right.get("source_type"),
        left_excerpt=_shorten(left_excerpt, 220),
        right_excerpt=_shorten(right_excerpt, 220),
        topic=topic,
        conflict_type=conflict_type,
        confidence_score=_confidence_score(left, right, now=now, base=base),
        left_source_trust=left.get("trust"),
        right_source_trust=right.get("trust"),
    )


def _source_trust(row: dict[str, Any]) -> float | None:
    metadata = row.get("metadata_dict") or {}
    for key in ("source_trust", "trust_score", "quality_score", "relevance_score", "confidence"):
        value = row.get(key)
        if value is None:
            value = metadata.get(key)
        parsed = _bounded_float(value)
        if parsed is not None:
            return parsed
    tier = str(metadata.get("source_tier") or metadata.get("tier") or "").lower()
    if tier == "gold":
        return 1.0
    if tier == "silver":
        return 0.75
    if tier == "bronze":
        return 0.5
    return None


def _bounded_float(value: Any) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if parsed > 1:
        parsed = parsed / 100
    return max(0.0, min(1.0, parsed))


def _confidence_score(left: dict[str, Any], right: dict[str, Any], *, now: datetime, base: float) -> float:
    trust_values = [value for value in (left.get("trust"), right.get("trust")) if value is not None]
    trust_bonus = (sum(trust_values) / len(trust_values) * 0.12) if trust_values else 0.0
    recencies = [_recency_score(row.get("timestamp"), now) for row in (left, right)]
    recency_bonus = (sum(recencies) / len(recencies)) * 0.14
    return round(min(0.99, base + trust_bonus + recency_bonus), 3)


def _recency_score(value: datetime | None, now: datetime) -> float:
    if value is None:
        return 0.0
    age_days = max(0.0, (now - value).total_seconds() / 86400)
    return max(0.0, 1.0 - min(age_days, DEFAULT_DAYS) / DEFAULT_DAYS)


def _same_source(left: dict[str, Any], right: dict[str, Any]) -> bool:
    for key in ("source_url", "source_id"):
        left_value = str(left.get(key) or "").strip().lower()
        right_value = str(right.get(key) or "").strip().lower()
        if left_value and left_value == right_value:
            return True
    return False


def _parse_now(now: datetime | None) -> datetime:
    parsed = now or datetime.now(timezone.utc)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _shorten(text: str, limit: int) -> str:
    normalized = " ".join((text or "").split())
    if len(normalized) <= limit:
        return normalized
    return normalized[: limit - 1].rstrip() + "..."
