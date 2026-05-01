"""Mine recurring audience FAQ ideas from reply_queue."""

from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import json
import re
import sqlite3
from typing import Any

from synthesis.content_gaps import classify_source_topics


DEFAULT_DAYS = 60
DEFAULT_MIN_COUNT = 2
DEFAULT_LIMIT = 20
SOURCE_NAME = "reply_faq_miner"

_QUESTION_INTENTS = {"question", "bug_report"}
_EXCLUDED_INTENTS = {"spam", "appreciation"}
_EXCLUDED_FLAGS = {"spam", "low_value", "low-value", "generic", "sycophantic"}
_LOW_SIGNAL_TEXT = {
    "thanks",
    "thank you",
    "thx",
    "great post",
    "love this",
    "nice",
    "awesome",
    "cool",
    "same",
    "+1",
}
_STOPWORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "can",
    "could",
    "did",
    "do",
    "does",
    "for",
    "from",
    "how",
    "i",
    "in",
    "is",
    "it",
    "my",
    "of",
    "on",
    "or",
    "should",
    "that",
    "the",
    "this",
    "to",
    "was",
    "what",
    "when",
    "where",
    "which",
    "why",
    "with",
    "would",
    "you",
    "your",
}
_SYNONYMS = {
    "db": "database",
    "dbs": "database",
    "pytest": "test",
    "testing": "test",
    "tests": "test",
    "migrate": "migration",
    "migrations": "migration",
    "fixture": "fixtures",
    "bug": "bug",
    "bugs": "bug",
    "broken": "break",
    "breaks": "break",
    "error": "failure",
    "errors": "failure",
    "failures": "failure",
}


def build_reply_faq_miner(
    db_or_conn: Any,
    *,
    days: int = DEFAULT_DAYS,
    min_count: int = DEFAULT_MIN_COUNT,
    limit: int = DEFAULT_LIMIT,
    apply: bool = False,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Return ranked FAQ clusters, optionally seeding content_ideas."""
    if days <= 0:
        raise ValueError("days must be positive")
    if min_count <= 0:
        raise ValueError("min_count must be positive")
    if limit < 0:
        raise ValueError("limit must be non-negative")

    conn = getattr(db_or_conn, "conn", db_or_conn)
    schema = _schema(conn)
    now = _ensure_utc(now or datetime.now(timezone.utc))
    cutoff = now - timedelta(days=days)
    filters = {
        "days": days,
        "min_count": min_count,
        "limit": limit,
        "apply": apply,
        "cutoff": cutoff.isoformat(),
    }
    if limit == 0 or "reply_queue" not in schema:
        return _empty_report(now, filters)

    rows = _load_reply_rows(conn, schema, cutoff=cutoff)
    eligible = [_prepare_row(row, now=now) for row in rows]
    eligible = [row for row in eligible if row is not None]
    clusters = _cluster_rows(eligible)
    candidates = [
        _build_cluster(cluster, now=now)
        for cluster in clusters
        if len(cluster) >= min_count
    ]
    candidates.sort(key=_cluster_sort_key)
    candidates = candidates[:limit]

    seed_results = _apply_candidates(db_or_conn, conn, candidates, apply=apply)
    for candidate, result in zip(candidates, seed_results, strict=False):
        candidate["seed_status"] = result["status"]
        candidate["idea_id"] = result["idea_id"]
        candidate["seed_reason"] = result["reason"]

    status_counts = Counter(result["status"] for result in seed_results)
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "scanned_reply_count": len(rows),
            "eligible_reply_count": len(eligible),
            "cluster_count": len(candidates),
            "created_count": status_counts.get("created", 0),
            "skipped_count": status_counts.get("skipped", 0),
            "candidate_count": status_counts.get("candidate", 0),
        },
        "clusters": candidates,
    }


def format_reply_faq_miner_json(report: dict[str, Any]) -> str:
    """Render the miner report as deterministic JSON."""
    return json.dumps(report, indent=2, sort_keys=True)


def format_reply_faq_miner_text(report: dict[str, Any]) -> str:
    """Render a compact operator-facing FAQ miner report."""
    filters = report["filters"]
    summary = report["summary"]
    lines = [
        "Reply FAQ miner",
        f"Generated: {report['generated_at']}",
        (
            f"Filters: days={filters['days']} min_count={filters['min_count']} "
            f"limit={filters['limit']} apply={filters['apply']}"
        ),
        (
            "Totals: "
            f"scanned={summary['scanned_reply_count']} "
            f"eligible={summary['eligible_reply_count']} "
            f"clusters={summary['cluster_count']} "
            f"created={summary['created_count']} "
            f"skipped={summary['skipped_count']}"
        ),
        "",
    ]
    if not report["clusters"]:
        lines.append("No recurring reply FAQ clusters found.")
        return "\n".join(lines)

    lines.append("Clusters")
    lines.append("  Score  Count  Authors  Status     Topic / representative question")
    for cluster in report["clusters"]:
        lines.append(
            f"  {cluster['score']:<5.1f}  "
            f"{cluster['reply_count']:<5}  "
            f"{cluster['author_count']:<7}  "
            f"{cluster['seed_status']:<9}  "
            f"{_clip(cluster['topic'], 18)} / {_clip(cluster['representative_question'], 78)}"
        )
        if cluster.get("answer_excerpt"):
            lines.append(f"         answer: {_clip(cluster['answer_excerpt'], 96)}")
        lines.append(f"         note: {_clip(cluster['suggested_content_idea_note'], 106)}")
    return "\n".join(lines)


def _load_reply_rows(
    conn: sqlite3.Connection,
    schema: dict[str, set[str]],
    *,
    cutoff: datetime,
) -> list[dict[str, Any]]:
    columns = schema["reply_queue"]
    required = {"id", "inbound_text"}
    if not required.issubset(columns):
        return []
    select = {
        "id": "rq.id",
        "inbound_text": "rq.inbound_text",
        "draft_text": _column_expr(columns, "draft_text", "NULL", alias="rq"),
        "intent": _column_expr(columns, "intent", "'other'", alias="rq"),
        "status": _column_expr(columns, "status", "''", alias="rq"),
        "priority": _column_expr(columns, "priority", "'normal'", alias="rq"),
        "inbound_author_handle": _column_expr(columns, "inbound_author_handle", "NULL", alias="rq"),
        "inbound_author_id": _column_expr(columns, "inbound_author_id", "NULL", alias="rq"),
        "inbound_tweet_id": _column_expr(columns, "inbound_tweet_id", "NULL", alias="rq"),
        "platform": _column_expr(columns, "platform", "'x'", alias="rq"),
        "inbound_url": _column_expr(columns, "inbound_url", "NULL", alias="rq"),
        "quality_score": _column_expr(columns, "quality_score", "NULL", alias="rq"),
        "quality_flags": _column_expr(columns, "quality_flags", "NULL", alias="rq"),
        "detected_at": _column_expr(columns, "detected_at", "NULL", alias="rq"),
        "reviewed_at": _column_expr(columns, "reviewed_at", "NULL", alias="rq"),
        "posted_at": _column_expr(columns, "posted_at", "NULL", alias="rq"),
    }
    params: list[Any] = [cutoff.isoformat()]
    filters = ["rq.inbound_text IS NOT NULL"]
    if "detected_at" in columns:
        filters.append("(rq.detected_at IS NULL OR datetime(rq.detected_at) >= datetime(?))")
    else:
        params = []
    return [
        dict(row)
        for row in conn.execute(
            f"""SELECT
                   {select['id']} AS id,
                   {select['inbound_text']} AS inbound_text,
                   {select['draft_text']} AS draft_text,
                   {select['intent']} AS intent,
                   {select['status']} AS status,
                   {select['priority']} AS priority,
                   {select['inbound_author_handle']} AS inbound_author_handle,
                   {select['inbound_author_id']} AS inbound_author_id,
                   {select['inbound_tweet_id']} AS inbound_tweet_id,
                   {select['platform']} AS platform,
                   {select['inbound_url']} AS inbound_url,
                   {select['quality_score']} AS quality_score,
                   {select['quality_flags']} AS quality_flags,
                   {select['detected_at']} AS detected_at,
                   {select['reviewed_at']} AS reviewed_at,
                   {select['posted_at']} AS posted_at
               FROM reply_queue rq
               WHERE {' AND '.join(filters)}
               ORDER BY datetime({select['detected_at']}) DESC, rq.id ASC""",
            params,
        ).fetchall()
    ]


def _prepare_row(row: dict[str, Any], *, now: datetime) -> dict[str, Any] | None:
    intent = str(row.get("intent") or "").strip().lower()
    text = _compact(row.get("inbound_text"))
    if not text:
        return None
    if intent in _EXCLUDED_INTENTS:
        return None
    if intent not in _QUESTION_INTENTS:
        return None
    if _is_low_signal_text(text):
        return None
    if _quality_flags(row) & _EXCLUDED_FLAGS:
        return None
    score = row.get("quality_score")
    if score is not None:
        try:
            if float(score) < 4.0:
                return None
        except (TypeError, ValueError):
            pass

    tokens = _signal_tokens(text)
    if len(tokens) < 2:
        return None
    detected_at = _parse_datetime(row.get("detected_at")) or now
    item = dict(row)
    item["normalized_text"] = _normalize_text(text)
    item["tokens"] = tokens
    item["token_set"] = set(tokens)
    item["base_fingerprint"] = _fingerprint(tokens)
    item["detected_at_dt"] = detected_at
    return item


def _cluster_rows(rows: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    clusters: list[list[dict[str, Any]]] = []
    for row in rows:
        best_index = None
        best_overlap = 0.0
        for index, cluster in enumerate(clusters):
            overlap = max(_token_overlap(row["token_set"], other["token_set"]) for other in cluster)
            same_fingerprint = row["base_fingerprint"] == cluster[0]["base_fingerprint"]
            if same_fingerprint or overlap >= 0.4:
                if overlap > best_overlap or best_index is None:
                    best_index = index
                    best_overlap = overlap
        if best_index is None:
            clusters.append([row])
        else:
            clusters[best_index].append(row)
    return clusters


def _build_cluster(rows: list[dict[str, Any]], *, now: datetime) -> dict[str, Any]:
    rows = sorted(rows, key=lambda row: (row["detected_at_dt"], int(row["id"])), reverse=True)
    representative = _representative_row(rows)
    reply_ids = sorted(int(row["id"]) for row in rows)
    authors = {
        str(row.get("inbound_author_id") or row.get("inbound_author_handle") or f"reply:{row['id']}")
        for row in rows
    }
    latest_at = max(row["detected_at_dt"] for row in rows)
    recency_days = max(0.0, (now - latest_at).total_seconds() / 86400)
    answer_row = next(
        (
            row
            for row in rows
            if str(row.get("status") or "").lower() in {"approved", "posted"}
            and _compact(row.get("draft_text"))
        ),
        None,
    )
    topic = _topic_for_rows(rows)
    fingerprint = _cluster_fingerprint(rows)
    score = (len(rows) * 10.0) + (len(authors) * 3.0) + max(0.0, 8.0 - recency_days)
    note = (
        f"Create an FAQ explainer about {topic}: "
        f"{_shorten(representative.get('inbound_text'), 150)} "
        f"({len(rows)} recurring replies from {len(authors)} author"
        f"{'' if len(authors) == 1 else 's'})."
    )
    return {
        "cluster_fingerprint": fingerprint,
        "representative_question": _compact(representative.get("inbound_text")),
        "answer_excerpt": _shorten(answer_row.get("draft_text"), 220) if answer_row else None,
        "reply_ids": reply_ids,
        "reply_count": len(rows),
        "author_count": len(authors),
        "authors": sorted(authors),
        "intent_counts": dict(sorted(Counter(str(row.get("intent") or "other") for row in rows).items())),
        "topic": topic,
        "latest_detected_at": latest_at.isoformat(),
        "score": round(score, 2),
        "suggested_content_idea_note": note,
        "source_metadata": {
            "source": SOURCE_NAME,
            "cluster_fingerprint": fingerprint,
            "reply_ids": reply_ids,
            "reply_count": len(rows),
            "author_count": len(authors),
            "representative_question": _compact(representative.get("inbound_text")),
            "answer_excerpt": _shorten(answer_row.get("draft_text"), 220) if answer_row else None,
            "topic": topic,
            "latest_detected_at": latest_at.isoformat(),
            "intents": sorted({str(row.get("intent") or "other") for row in rows}),
            "source_ids": [_reply_identity(row) for row in rows],
        },
    }


def _apply_candidates(
    db_or_conn: Any,
    conn: sqlite3.Connection,
    candidates: list[dict[str, Any]],
    *,
    apply: bool,
) -> list[dict[str, Any]]:
    results = []
    for candidate in candidates:
        duplicate = _find_existing_seed(conn, candidate["cluster_fingerprint"])
        if duplicate is not None:
            results.append(
                {
                    "status": "skipped",
                    "idea_id": int(duplicate["id"]),
                    "reason": f"{duplicate['status']} content idea duplicate",
                }
            )
            continue
        if not apply:
            results.append({"status": "candidate", "idea_id": None, "reason": "dry run"})
            continue
        add_idea = getattr(db_or_conn, "add_content_idea", None)
        metadata = candidate["source_metadata"]
        if add_idea is not None:
            idea_id = add_idea(
                note=candidate["suggested_content_idea_note"],
                topic=candidate["topic"],
                priority="high" if candidate["reply_count"] >= 3 else "normal",
                source=SOURCE_NAME,
                source_metadata=metadata,
            )
        else:
            cursor = conn.execute(
                """INSERT INTO content_ideas
                   (note, topic, priority, status, source, source_metadata)
                   VALUES (?, ?, ?, 'open', ?, ?)""",
                (
                    candidate["suggested_content_idea_note"],
                    candidate["topic"],
                    "high" if candidate["reply_count"] >= 3 else "normal",
                    SOURCE_NAME,
                    json.dumps(metadata, sort_keys=True),
                ),
            )
            conn.commit()
            idea_id = cursor.lastrowid
        results.append({"status": "created", "idea_id": int(idea_id), "reason": "created"})
    return results


def _find_existing_seed(
    conn: sqlite3.Connection,
    cluster_fingerprint: str,
) -> dict[str, Any] | None:
    if "content_ideas" not in _schema(conn):
        return None
    rows = conn.execute(
        """SELECT * FROM content_ideas
           WHERE source = ?
             AND status IN ('open', 'promoted')
             AND source_metadata IS NOT NULL
           ORDER BY created_at ASC, id ASC""",
        (SOURCE_NAME,),
    ).fetchall()
    for row in rows:
        item = dict(row)
        try:
            metadata = json.loads(item.get("source_metadata") or "{}")
        except (TypeError, ValueError):
            continue
        if metadata.get("cluster_fingerprint") == cluster_fingerprint:
            return item
    return None


def _representative_row(rows: list[dict[str, Any]]) -> dict[str, Any]:
    def key(row: dict[str, Any]) -> tuple[float, int, int, datetime, int]:
        overlap = sum(
            _token_overlap(row["token_set"], other["token_set"])
            for other in rows
            if other["id"] != row["id"]
        )
        has_expanded_terms = int("database" in _normalize_text(row.get("inbound_text")))
        return (
            overlap,
            len(row["token_set"]),
            has_expanded_terms,
            row["detected_at_dt"],
            -int(row["id"]),
        )

    return max(rows, key=key)


def _topic_for_rows(rows: list[dict[str, Any]]) -> str:
    text = " ".join(
        _compact(f"{row.get('inbound_text') or ''} {row.get('draft_text') or ''}")
        for row in rows
    )
    topics = classify_source_topics(text)
    return topics[0] if topics else "reply-faq"


def _normalize_text(text: str) -> str:
    text = str(text or "").lower()
    text = re.sub(r"https?://\S+|@\w+", " ", text)
    text = re.sub(r"[^a-z0-9+#.\s']+", " ", text)
    return _compact(text)


def _signal_tokens(text: str) -> list[str]:
    tokens = []
    for token in re.findall(r"[a-z0-9+#.']+", _normalize_text(text)):
        token = token.strip("'")
        if not token or token in _STOPWORDS or len(token) <= 1:
            continue
        token = _SYNONYMS.get(token, token)
        token = _stem(token)
        tokens.append(_SYNONYMS.get(token, token))
    return tokens


def _stem(token: str) -> str:
    if len(token) > 5 and token.endswith("ing"):
        return token[:-3]
    if len(token) > 4 and token.endswith("ed"):
        return token[:-2]
    if len(token) > 4 and token.endswith("s") and not token.endswith("ss"):
        return token[:-1]
    return token


def _fingerprint(tokens: list[str]) -> str:
    material = " ".join(sorted(set(tokens))[:10])
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _cluster_fingerprint(rows: list[dict[str, Any]]) -> str:
    token_counts = Counter(token for row in rows for token in set(row["tokens"]))
    tokens = [token for token, _count in token_counts.most_common(10)]
    material = " ".join(sorted(tokens))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:16]


def _token_overlap(left: set[str], right: set[str]) -> float:
    if not left or not right:
        return 0.0
    return len(left & right) / len(left | right)


def _quality_flags(row: dict[str, Any]) -> set[str]:
    raw = row.get("quality_flags")
    if not raw:
        return set()
    try:
        parsed = json.loads(raw)
    except (TypeError, ValueError):
        parsed = [raw]
    if not isinstance(parsed, list):
        parsed = [parsed]
    flags = set()
    for item in parsed:
        value = str(item or "").strip().lower()
        if value:
            flags.add(value)
            flags.update(part for part in re.split(r"[:\s]+", value) if part)
    return flags


def _is_low_signal_text(text: str) -> bool:
    normalized = _normalize_text(text).strip()
    if normalized in _LOW_SIGNAL_TEXT:
        return True
    return (
        "?" not in text
        and len(_signal_tokens(text)) <= 2
        and any(value in normalized for value in _LOW_SIGNAL_TEXT)
    )


def _reply_identity(row: dict[str, Any]) -> str:
    return f"{row.get('platform') or 'x'}:{row.get('inbound_tweet_id') or row.get('id')}"


def _cluster_sort_key(cluster: dict[str, Any]) -> tuple[float, int, float, str]:
    latest = _parse_datetime(cluster["latest_detected_at"])
    latest_ts = latest.timestamp() if latest is not None else 0.0
    return (
        -float(cluster["score"]),
        -int(cluster["reply_count"]),
        -latest_ts,
        cluster["cluster_fingerprint"],
    )


def _empty_report(now: datetime, filters: dict[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": now.isoformat(),
        "filters": filters,
        "summary": {
            "scanned_reply_count": 0,
            "eligible_reply_count": 0,
            "cluster_count": 0,
            "created_count": 0,
            "skipped_count": 0,
            "candidate_count": 0,
        },
        "clusters": [],
    }


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        name = row[0]
        schema[name] = {item[1] for item in conn.execute(f"PRAGMA table_info({name})")}
    return schema


def _column_expr(
    columns: set[str],
    column: str,
    default: str = "NULL",
    *,
    alias: str,
) -> str:
    return f"{alias}.{column}" if column in columns else default


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _compact(text: Any) -> str:
    return re.sub(r"\s+", " ", str(text or "")).strip()


def _shorten(text: Any, width: int = 100) -> str:
    value = _compact(text)
    if len(value) <= width:
        return value
    return value[: max(0, width - 3)].rstrip() + "..."


def _clip(text: Any, width: int) -> str:
    return _shorten(text, width)
