"""Find destination domains whose newsletter click share has decayed."""
from __future__ import annotations
from collections import defaultdict
from typing import Any
from urllib.parse import urlparse
from ._batch_report_common import *

ARTIFACT_TYPE = "newsletter_click_destination_decay"
DEFAULT_RECENT_DAYS = 14
DEFAULT_BASELINE_DAYS = 60
DEFAULT_MIN_BASELINE_CLICKS = 10
DEFAULT_MIN_SHARE_DROP = 0.1
DEFAULT_LIMIT = 50


def _destination_domain(url: Any) -> str | None:
    text = clean(url)
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return None
    host = parsed.hostname.lower().strip(".")
    return host[4:] if host.startswith("www.") else host


def build_newsletter_click_destination_decay_report(
    rows: list[dict[str, Any]],
    *,
    recent_days: int = DEFAULT_RECENT_DAYS,
    baseline_days: int = DEFAULT_BASELINE_DAYS,
    min_baseline_clicks: int = DEFAULT_MIN_BASELINE_CLICKS,
    min_share_drop: float = DEFAULT_MIN_SHARE_DROP,
    domain: str | list[str] | tuple[str, ...] | None = None,
    limit: int = DEFAULT_LIMIT,
    missing_tables: list[str] | None = None,
    missing_columns: dict[str, list[str]] | None = None,
    now=None,
) -> dict[str, Any]:
    positive("recent_days", recent_days)
    positive("baseline_days", baseline_days)
    positive("min_baseline_clicks", min_baseline_clicks)
    non_negative("min_share_drop", min_share_drop)
    positive("limit", limit)
    gen = now_value(now)
    recent_start = gen - timedelta(days=recent_days)
    baseline_start = recent_start - timedelta(days=baseline_days)
    wanted = {d for d in (_destination_domain(f"https://{x}") or clean(x).lower().removeprefix("www.") for x in ([domain] if isinstance(domain, str) else domain or [])) if d}
    counts = {"baseline": defaultdict(int), "recent": defaultdict(int)}
    newest: dict[str, datetime] = {}
    skipped_url_count = 0
    click_rows_scanned = 0
    for row in rows:
        clicked = dt(row.get("clicked_at") or row.get("last_clicked_at") or row.get("date") or row.get("created_at"))
        if clicked is None or clicked < baseline_start or clicked > gen:
            continue
        dom = _destination_domain(row.get("url") or row.get("link_url") or row.get("destination_url"))
        if dom is None:
            skipped_url_count += 1
            continue
        if wanted and dom not in wanted:
            continue
        click_rows_scanned += 1
        clicks = to_int(row.get("click_count") or row.get("clicks") or row.get("unique_clicks") or 1, 1)
        bucket = "recent" if clicked >= recent_start else "baseline"
        counts[bucket][dom] += clicks
        if dom not in newest or clicked > newest[dom]:
            newest[dom] = clicked
    baseline_total = sum(counts["baseline"].values())
    recent_total = sum(counts["recent"].values())
    findings = []
    for dom in sorted(set(counts["baseline"]) | set(counts["recent"])):
        base = counts["baseline"][dom]
        recent = counts["recent"][dom]
        if base < min_baseline_clicks or baseline_total <= 0:
            continue
        baseline_share = base / baseline_total
        recent_share = recent / recent_total if recent_total else 0.0
        share_delta = recent_share - baseline_share
        decay_ratio = recent_share / baseline_share if baseline_share else 0.0
        if baseline_share - recent_share >= min_share_drop:
            findings.append(
                {
                    "domain": dom,
                    "baseline_clicks": base,
                    "recent_clicks": recent,
                    "baseline_share": round(baseline_share, 4),
                    "recent_share": round(recent_share, 4),
                    "share_delta": round(share_delta, 4),
                    "decay_ratio": round(decay_ratio, 4),
                    "newest_click_at": newest[dom].isoformat() if dom in newest else None,
                }
            )
    findings.sort(key=lambda i: (i["decay_ratio"], i["share_delta"], -i["baseline_clicks"], i["domain"]))
    findings = findings[:limit]
    return {
        "artifact_type": ARTIFACT_TYPE,
        "generated_at": gen.isoformat(),
        "filters": {
            "recent_days": recent_days,
            "baseline_days": baseline_days,
            "min_baseline_clicks": min_baseline_clicks,
            "min_share_drop": min_share_drop,
            "domain": sorted(wanted),
            "limit": limit,
        },
        "summary": {
            "click_rows_scanned": click_rows_scanned,
            "baseline_clicks": baseline_total,
            "recent_clicks": recent_total,
            "decay_count": len(findings),
            "skipped_url_count": skipped_url_count,
        },
        "decays": findings,
        "missing_tables": sorted(missing_tables or []),
        "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())},
        "empty_state": empty_state(findings, "No newsletter click destination decay found.", schema_gap=bool(missing_tables or missing_columns)),
    }


def build_newsletter_click_destination_decay_report_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn)
    sch = schema(conn)
    table = "newsletter_link_clicks" if "newsletter_link_clicks" in sch else "newsletter_clicks" if "newsletter_clicks" in sch else None
    if not table:
        return build_newsletter_click_destination_decay_report([], missing_tables=["newsletter_link_clicks"], **kwargs)
    rows = load_table(
        conn,
        table,
        sch[table],
        {
            "url": ("link_url", "url", "destination_url"),
            "click_count": ("click_count", "clicks", "unique_clicks"),
            "clicked_at": ("clicked_at", "last_clicked_at", "date", "created_at"),
        },
    )
    return build_newsletter_click_destination_decay_report(rows, **kwargs)


def format_newsletter_click_destination_decay_json(report: dict[str, Any]) -> str:
    return json_dumps(report)


def format_newsletter_click_destination_decay_text(report: dict[str, Any]) -> str:
    lines = [
        "Newsletter Click Destination Decay",
        f"Generated: {report['generated_at']}",
        f"Totals: baseline_clicks={report['summary']['baseline_clicks']} recent_clicks={report['summary']['recent_clicks']} decay={report['summary']['decay_count']} skipped_urls={report['summary']['skipped_url_count']}",
    ]
    if not report["decays"]:
        lines.append(report["empty_state"]["message"])
        return "\n".join(lines)
    lines += ["", "domain | baseline_clicks | recent_clicks | baseline_share | recent_share | share_delta | decay_ratio | newest_click_at"]
    lines += [
        f"{i['domain']} | {i['baseline_clicks']} | {i['recent_clicks']} | {i['baseline_share']} | {i['recent_share']} | {i['share_delta']} | {i['decay_ratio']} | {i['newest_click_at']}"
        for i in report["decays"]
    ]
    return "\n".join(lines)
