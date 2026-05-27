"""Find GitHub issue or PR comments that need follow-up but lack downstream action."""
from __future__ import annotations
from datetime import datetime
from typing import Any
from ._batch_report_common import *

ARTIFACT_TYPE = "github_activity_issue_comment_followup_gaps"
DEFAULT_LIMIT = 50
CUES = {"question": re.compile(r"\?|\bhow\b|\bwhy\b|\bcan you\b", re.I), "mention": re.compile(r"@[a-z0-9_-]+", re.I), "todo": re.compile(r"\b(todo|follow up|action item)\b", re.I), "bug": re.compile(r"\bbug|regression|broken|fails?\b", re.I), "release_note": re.compile(r"\brelease note|changelog\b", re.I), "feedback": re.compile(r"\bfeedback|request|suggestion\b", re.I)}


def build_github_activity_issue_comment_followup_gaps_report(comments: list[dict[str, Any]], followups: list[dict[str, Any]] | None = None, *, now: datetime | None = None, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None):
    positive("limit", limit); gen = now_value(now)
    hay = " ".join(clean(f.get("comment_id") or f.get("source_comment_id") or f.get("source_id")) + " " + clean(f.get("body") or f.get("title") or f.get("source_url")) for f in (followups or []))
    findings = []
    for c in comments:
        body = clean(c.get("body") or c.get("comment_body"))
        cues = [name for name, rx in CUES.items() if rx.search(body)]
        cid = clean(c.get("comment_id") or c.get("id"))
        if not cues: continue
        if cid and cid in hay: continue
        created = dt(c.get("created_at") or c.get("updated_at"))
        age = (gen - created).days if created else None
        findings.append({"repository": clean(c.get("repository") or c.get("repo")), "issue_pr": clean(c.get("issue_number") or c.get("pr_number") or c.get("number")), "comment_id": cid, "age_days": age, "missing_followup_reason": "cue_without_downstream_followup:" + ",".join(cues), "recommended_action": "create planned topic, reply draft, content idea, or resolved follow-up action"})
    findings.sort(key=lambda f: (-(f["age_days"] or 0), f["repository"], f["issue_pr"], f["comment_id"]))
    shown = findings[:limit]
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"limit": limit}, "summary": {"comment_count": len(comments), "finding_count": len(findings), "shown": len(shown)}, "comment_followup_gaps": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "empty_state": empty_state(findings, "No GitHub issue comment follow-up gaps found.", schema_gap=bool(missing_tables or missing_columns))}


def build_github_activity_issue_comment_followup_gaps_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; comments = []; followups = []
    table = "github_issue_comments" if "github_issue_comments" in s else ("github_activity" if "github_activity" in s else None)
    if not table: mt.append("github_issue_comments|github_activity")
    else:
        c = s[table]
        if not ({"body", "comment_body"} & c): mc[table] = ["body|comment_body"]
        comments = load_table(conn, table, c, {"comment_id": ("comment_id", "id"), "repository": ("repository", "repo"), "issue_number": ("issue_number", "pr_number", "number"), "body": ("body", "comment_body"), "created_at": ("created_at", "updated_at")})
    for t in [x for x in ("planned_topics", "content_ideas", "generated_content", "reply_queue", "proactive_actions", "strategic_actions") if x in s]:
        c = s[t]; followups += load_table(conn, t, c, {"comment_id": ("comment_id", "source_comment_id", "source_id"), "title": ("title", "name"), "body": ("body", "text", "description"), "source_url": ("source_url", "url")})
    return build_github_activity_issue_comment_followup_gaps_report(comments, followups, missing_tables=mt, missing_columns=mc, **kw)


def format_github_activity_issue_comment_followup_gaps_json(r): return json_dumps(r)
def format_github_activity_issue_comment_followup_gaps_text(r):
    s = r["summary"]; lines = ["GitHub Activity Issue Comment Followup Gaps", f"Generated: {r['generated_at']}", f"Totals: comments={s['comment_count']} findings={s['finding_count']} shown={s['shown']}"]
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["comment_followup_gaps"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "repository | issue_pr | comment_id | age_days | reason"]
    for f in r["comment_followup_gaps"]: lines.append(f"{f['repository']} | {f['issue_pr']} | {f['comment_id']} | {f['age_days']} | {f['missing_followup_reason']}")
    return "\n".join(lines)
