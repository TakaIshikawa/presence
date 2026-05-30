"""Report published posts with overused or repetitive hashtags."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import timedelta
from typing import Any

from ._batch_report_common import *


ARTIFACT_TYPE = "published_post_hashtag_overuse"
DEFAULT_LIMIT = 100
DEFAULT_WINDOW_DAYS = 30
DEFAULT_MAX_HASHTAGS_PER_POST = 5
DEFAULT_REPEATED_SET_THRESHOLD = 2
DEFAULT_MIN_DIVERSITY_RATIO = 0.5
HASHTAG_RE = re.compile(r"(?<![\w&])#([A-Za-z][A-Za-z0-9_]*)")
URL_RE = re.compile(r"https?://[^\s<>)\"']+|www\.[^\s<>)\"']+", re.I)


def build_published_post_hashtag_overuse_report(
    rows: list[dict[str, Any]],
    *,
    window_days: int = DEFAULT_WINDOW_DAYS,
    max_hashtags_per_post: int = DEFAULT_MAX_HASHTAGS_PER_POST,
    repeated_set_threshold: int = DEFAULT_REPEATED_SET_THRESHOLD,
    min_diversity_ratio: float = DEFAULT_MIN_DIVERSITY_RATIO,
    limit: int = DEFAULT_LIMIT,
    now=None,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
) -> dict[str, Any]:
    positive("window_days", window_days)
    positive("max_hashtags_per_post", max_hashtags_per_post)
    positive("repeated_set_threshold", repeated_set_threshold)
    positive("limit", limit)
    bounded_share("min_diversity_ratio", min_diversity_ratio)
    gen = now_value(now)
    cutoff = gen - timedelta(days=window_days)
    posts = [_post(row) for row in rows]
    recent = [p for p in posts if p["published_at_dt"] is None or p["published_at_dt"] >= cutoff]
    issues: list[dict[str, Any]] = []
    for post in recent:
        if len(post["hashtags"]) > max_hashtags_per_post:
            issues.append(
                _issue(
                    post,
                    "per_post_overuse",
                    "high" if len(post["hashtags"]) >= max_hashtags_per_post * 2 else "medium",
                    f"{len(post['hashtags'])} hashtags exceeds limit {max_hashtags_per_post}",
                    "Reduce the hashtag list to the few strongest topic or campaign tags.",
                )
            )
    by_set: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
    for post in recent:
        if post["canonical_set"]:
            by_set[post["canonical_set"]].append(post)
    for tag_set, grouped in by_set.items():
        if len(grouped) >= repeated_set_threshold:
            evidence = f"same set {' '.join('#' + t for t in tag_set)} used on {len(grouped)} posts"
            for post in grouped:
                issues.append(
                    _issue(
                        post,
                        "repeated_hashtag_set",
                        "medium",
                        evidence,
                        "Vary hashtags so each post reflects its specific topic and audience.",
                    )
                )
    total_tags = sum(len(p["canonical_hashtags"]) for p in recent)
    unique_tags = {tag for p in recent for tag in p["canonical_hashtags"]}
    diversity_ratio = round(len(unique_tags) / total_tags, 4) if total_tags else 1.0
    if recent and total_tags and diversity_ratio < min_diversity_ratio:
        top = ", ".join(f"#{tag}x{count}" for tag, count in Counter(tag for p in recent for tag in p["canonical_hashtags"]).most_common(5))
        for post in recent:
            if post["canonical_hashtags"]:
                issues.append(
                    _issue(
                        post,
                        "low_diversity_window",
                        "low",
                        f"window diversity {diversity_ratio} below {min_diversity_ratio}; top tags: {top}",
                        "Broaden the hashtag mix across the rolling window.",
                    )
                )
    issues.sort(key=lambda i: (-{"high": 3, "medium": 2, "low": 1}[i["severity"]], clean(i["post_id"]), i["issue_type"]))
    shown = issues[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {
            "window_days": window_days,
            "max_hashtags_per_post": max_hashtags_per_post,
            "repeated_set_threshold": repeated_set_threshold,
            "min_diversity_ratio": min_diversity_ratio,
            "limit": limit,
        },
        "summary": {
            "post_count": len(recent),
            "issue_count": len(issues),
            "shown_count": len(shown),
            "unique_hashtag_count": len(unique_tags),
            "total_hashtag_count": total_tags,
            "diversity_ratio": diversity_ratio,
            "issue_counts": dict(sorted(Counter(i["issue_type"] for i in issues).items())),
        },
        "issues": shown,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items()) if v},
        "empty_state": empty_state(issues, "No published post hashtag overuse findings.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_published_post_hashtag_overuse_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = "content_publications" if "content_publications" in sch else "generated_content" if "generated_content" in sch else None
    if not table:
        return build_published_post_hashtag_overuse_report([], missing_tables=["content_publications|generated_content"], **kwargs)
    cols = sch[table]
    body_cols = {"content", "body", "text", "caption", "post_text"}
    if not body_cols & cols:
        return build_published_post_hashtag_overuse_report([], missing_columns={table: ["content|body|text|caption|post_text"]}, **kwargs)
    if table == "content_publications":
        rows = load_table(conn, table, cols, {"post_id": ("id", "post_id", "content_id"), "text": tuple(body_cols), "published_at": ("published_at", "created_at"), "status": ("status",), "platform": ("platform",)})
        rows = [r for r in rows if lower(r.get("status"), "published") == "published"]
    else:
        rows = load_table(conn, table, cols, {"post_id": ("id", "post_id"), "text": tuple(body_cols), "published_at": ("published_at", "created_at"), "status": ("status", "outcome"), "published": ("published",), "platform": ("platform", "content_type")})
        rows = [r for r in rows if str(r.get("published") or "1") != "0" and lower(r.get("status"), "published") in {"published", "success", ""}]
    return build_published_post_hashtag_overuse_report(rows, **kwargs)


def extract_hashtags(text: Any) -> list[str]:
    scrubbed = URL_RE.sub(" ", clean(text))
    return ["#" + match.group(1) for match in HASHTAG_RE.finditer(scrubbed)]


def format_published_post_hashtag_overuse_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_published_post_hashtag_overuse_text(report: dict[str, Any]) -> str:
    s = report["summary"]
    lines = ["Published Post Hashtag Overuse", f"Generated: {report['generated_at']}", f"Totals: posts={s['post_count']} issues={s['issue_count']} unique_tags={s['unique_hashtag_count']} diversity={s['diversity_ratio']}"]
    if report["missing_tables"]:
        lines.append("Missing tables: " + ", ".join(report["missing_tables"]))
    if report["missing_columns"]:
        lines.append("Missing columns: " + flatten_missing(report["missing_columns"]))
    if not report["issues"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "post_id | severity | issue_type | hashtags | evidence | recommendation"]
    for item in report["issues"]:
        lines.append(f"{item['post_id']} | {item['severity']} | {item['issue_type']} | {' '.join(item['hashtags']) or '-'} | {item['evidence']} | {item['recommendation']}")
    return "\n".join(lines)


def _post(row: dict[str, Any]) -> dict[str, Any]:
    hashtags = extract_hashtags(row.get("text") or row.get("content") or row.get("body") or row.get("caption") or row.get("post_text"))
    canonical = [tag[1:].lower() for tag in hashtags]
    return {
        "post_id": clean(row.get("post_id") or row.get("id"), "unknown"),
        "platform": clean(row.get("platform")),
        "published_at": clean(row.get("published_at") or row.get("created_at")),
        "published_at_dt": dt(row.get("published_at") or row.get("created_at")),
        "hashtags": hashtags,
        "canonical_hashtags": canonical,
        "canonical_set": tuple(sorted(set(canonical))),
    }


def _issue(post: dict[str, Any], issue_type: str, severity: str, evidence: str, recommendation: str) -> dict[str, Any]:
    return {"post_id": post["post_id"], "platform": post["platform"], "published_at": post["published_at"], "issue_type": issue_type, "hashtags": post["hashtags"], "canonical_hashtags": post["canonical_hashtags"], "evidence": evidence, "severity": severity, "recommendation": recommendation}
