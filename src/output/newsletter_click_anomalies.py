"""Detect anomalous newsletter link-click distributions."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import json
import sqlite3
from typing import Any


DEFAULT_DAYS = 90
DEFAULT_DOMINANCE_THRESHOLD = 0.8


@dataclass(frozen=True)
class NewsletterClickAnomaly:
    """One anomalous link or send-level click condition."""

    send_id: int
    issue_id: str
    subject: str
    sent_at: str
    link_url: str | None
    click_count: int
    share: float | None
    reason: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        if self.share is not None:
            payload["share"] = round(self.share, 6)
        return payload


@dataclass(frozen=True)
class NewsletterClickSendSummary:
    """Click distribution summary for one newsletter send."""

    send_id: int
    issue_id: str
    subject: str
    sent_at: str
    total_clicks: int
    tracked_links: int
    anomalies: tuple[NewsletterClickAnomaly, ...] = field(default_factory=tuple)

    @property
    def anomalous(self) -> bool:
        return bool(self.anomalies)

    def to_dict(self) -> dict[str, Any]:
        return {
            "anomalies": [item.to_dict() for item in self.anomalies],
            "anomalous": self.anomalous,
            "issue_id": self.issue_id,
            "send_id": self.send_id,
            "sent_at": self.sent_at,
            "subject": self.subject,
            "total_clicks": self.total_clicks,
            "tracked_links": self.tracked_links,
        }


@dataclass(frozen=True)
class NewsletterClickAnomalyReport:
    """Anomaly report for recent newsletter sends."""

    days: int
    dominance_threshold: float
    send_id: int | None
    total_sends_inspected: int
    anomalous_sends: int
    total_clicks: int
    sends: tuple[NewsletterClickSendSummary, ...]
    anomalies: tuple[NewsletterClickAnomaly, ...]
    missing_tables: tuple[str, ...] = ()
    missing_columns: dict[str, tuple[str, ...]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "anomalies": [item.to_dict() for item in self.anomalies],
            "anomalous_sends": self.anomalous_sends,
            "artifact_type": "newsletter_click_anomalies",
            "days": self.days,
            "dominance_threshold": self.dominance_threshold,
            "missing_columns": {
                table: list(columns)
                for table, columns in sorted(self.missing_columns.items())
            },
            "missing_tables": list(self.missing_tables),
            "send_id": self.send_id,
            "sends": [item.to_dict() for item in self.sends],
            "total_clicks": self.total_clicks,
            "total_sends_inspected": self.total_sends_inspected,
        }


def build_newsletter_click_anomaly_report(
    db: Any,
    *,
    days: int = DEFAULT_DAYS,
    dominance_threshold: float = DEFAULT_DOMINANCE_THRESHOLD,
    send_id: int | None = None,
) -> NewsletterClickAnomalyReport:
    """Build a recent newsletter click-distribution anomaly report."""
    period_days = int(days)
    if period_days <= 0:
        raise ValueError("days must be positive")
    threshold = float(dominance_threshold)
    if threshold <= 0 or threshold > 1:
        raise ValueError("dominance_threshold must be greater than 0 and at most 1")
    scoped_send_id = None if send_id is None else int(send_id)
    if scoped_send_id is not None and scoped_send_id <= 0:
        raise ValueError("send_id must be positive")

    conn = _connection(db)
    schema = _schema(conn)
    missing_tables = tuple(
        table
        for table in ("newsletter_sends", "newsletter_link_clicks")
        if table not in schema
    )
    missing_columns: dict[str, tuple[str, ...]] = {}
    required = {
        "newsletter_sends": {"id", "issue_id", "subject", "sent_at"},
        "newsletter_link_clicks": {
            "id",
            "newsletter_send_id",
            "issue_id",
            "link_url",
            "clicks",
            "fetched_at",
        },
    }
    for table, columns in required.items():
        if table in schema:
            missing = tuple(sorted(columns - schema[table]))
            if missing:
                missing_columns[table] = missing
    if missing_tables or missing_columns:
        return NewsletterClickAnomalyReport(
            days=period_days,
            dominance_threshold=threshold,
            send_id=scoped_send_id,
            total_sends_inspected=0,
            anomalous_sends=0,
            total_clicks=0,
            sends=(),
            anomalies=(),
            missing_tables=missing_tables,
            missing_columns=missing_columns,
        )

    send_rows = _load_sends(conn, days=period_days, send_id=scoped_send_id)
    click_rows = _load_latest_clicks(conn, send_ids=[int(row["id"]) for row in send_rows])
    clicks_by_send: dict[int, list[dict[str, Any]]] = {}
    for row in click_rows:
        clicks_by_send.setdefault(int(row["newsletter_send_id"]), []).append(row)

    send_summaries: list[NewsletterClickSendSummary] = []
    anomalies: list[NewsletterClickAnomaly] = []
    total_clicks = 0
    for send in send_rows:
        current_send_id = int(send["id"])
        rows = clicks_by_send.get(current_send_id, [])
        send_total = sum(int(row["clicks"] or 0) for row in rows)
        total_clicks += send_total
        send_anomalies = _send_anomalies(
            send=send,
            click_rows=rows,
            total_clicks=send_total,
            threshold=threshold,
        )
        anomalies.extend(send_anomalies)
        send_summaries.append(
            NewsletterClickSendSummary(
                send_id=current_send_id,
                issue_id=send["issue_id"] or "",
                subject=send["subject"] or "",
                sent_at=send["sent_at"] or "",
                total_clicks=send_total,
                tracked_links=len(rows),
                anomalies=tuple(send_anomalies),
            )
        )

    return NewsletterClickAnomalyReport(
        days=period_days,
        dominance_threshold=threshold,
        send_id=scoped_send_id,
        total_sends_inspected=len(send_summaries),
        anomalous_sends=sum(1 for send in send_summaries if send.anomalous),
        total_clicks=total_clicks,
        sends=tuple(send_summaries),
        anomalies=tuple(anomalies),
    )


def format_newsletter_click_anomaly_json(report: NewsletterClickAnomalyReport) -> str:
    """Render deterministic JSON for automation."""
    return json.dumps(report.to_dict(), indent=2, sort_keys=True)


def format_newsletter_click_anomaly_text(report: NewsletterClickAnomalyReport) -> str:
    """Render a compact human-readable anomaly report."""
    lines = [
        "Newsletter Click Anomalies",
        f"Period: last {report.days} days",
        f"Dominance threshold: {report.dominance_threshold:.0%}",
        (
            "Summary: "
            f"{report.total_sends_inspected} sends inspected, "
            f"{report.anomalous_sends} anomalous sends, "
            f"{report.total_clicks} total clicks"
        ),
    ]
    if report.send_id is not None:
        lines.append(f"Send filter: {report.send_id}")
    if report.missing_tables:
        lines.append("Missing tables: " + ", ".join(report.missing_tables))
    if report.missing_columns:
        formatted = [
            f"{table}.{column}"
            for table, columns in sorted(report.missing_columns.items())
            for column in columns
        ]
        lines.append("Missing columns: " + ", ".join(formatted))
    if not report.anomalies:
        lines.append("No anomalies found.")
        return "\n".join(lines)

    lines.append("")
    lines.append("Anomalies:")
    for item in report.anomalies:
        subject = item.subject or "untitled"
        link_url = item.link_url or "n/a"
        share = "n/a" if item.share is None else f"{item.share:.0%}"
        lines.append(
            f"- send {item.send_id} ({subject}); link {link_url}; "
            f"{item.click_count} clicks; share {share}; reason {item.reason}"
        )
    return "\n".join(lines)


def _send_anomalies(
    *,
    send: sqlite3.Row,
    click_rows: list[dict[str, Any]],
    total_clicks: int,
    threshold: float,
) -> list[NewsletterClickAnomaly]:
    send_id = int(send["id"])
    base = {
        "send_id": send_id,
        "issue_id": send["issue_id"] or "",
        "subject": send["subject"] or "",
        "sent_at": send["sent_at"] or "",
    }
    if not click_rows:
        return [
            NewsletterClickAnomaly(
                **base,
                link_url=None,
                click_count=0,
                share=None,
                reason="no_click_rows",
            )
        ]

    anomalies: list[NewsletterClickAnomaly] = []
    for row in sorted(click_rows, key=lambda item: (str(item["link_url"] or ""), int(item["id"]))):
        clicks = int(row["clicks"] or 0)
        share = clicks / total_clicks if total_clicks > 0 else 0.0
        if clicks == 0:
            anomalies.append(
                NewsletterClickAnomaly(
                    **base,
                    link_url=row["link_url"] or "",
                    click_count=clicks,
                    share=share,
                    reason="zero_click_link",
                )
            )
        if total_clicks > 0 and share >= threshold:
            anomalies.append(
                NewsletterClickAnomaly(
                    **base,
                    link_url=row["link_url"] or "",
                    click_count=clicks,
                    share=share,
                    reason="dominant_link",
                )
            )
    return anomalies


def _load_sends(
    conn: sqlite3.Connection,
    *,
    days: int,
    send_id: int | None,
) -> list[sqlite3.Row]:
    filters = ["datetime(sent_at) >= datetime('now', ?)"]
    params: list[Any] = [f"-{days} days"]
    if send_id is not None:
        filters.append("id = ?")
        params.append(send_id)
    cursor = conn.execute(
        f"""SELECT id, issue_id, subject, sent_at
            FROM newsletter_sends
            WHERE {" AND ".join(filters)}
            ORDER BY datetime(sent_at) DESC, id DESC""",
        params,
    )
    return list(cursor.fetchall())


def _load_latest_clicks(
    conn: sqlite3.Connection,
    *,
    send_ids: list[int],
) -> list[dict[str, Any]]:
    if not send_ids:
        return []
    placeholders = ",".join("?" for _ in send_ids)
    cursor = conn.execute(
        f"""SELECT nlc.id,
                   nlc.newsletter_send_id,
                   nlc.issue_id,
                   nlc.link_url,
                   nlc.clicks,
                   nlc.fetched_at
            FROM newsletter_link_clicks nlc
            WHERE nlc.newsletter_send_id IN ({placeholders})
              AND nlc.id = (
                  SELECT latest.id
                  FROM newsletter_link_clicks latest
                  WHERE latest.newsletter_send_id = nlc.newsletter_send_id
                    AND latest.issue_id = nlc.issue_id
                    AND latest.link_url = nlc.link_url
                  ORDER BY datetime(latest.fetched_at) DESC, latest.id DESC
                  LIMIT 1
              )
            ORDER BY nlc.newsletter_send_id ASC, nlc.link_url ASC""",
        tuple(send_ids),
    )
    return [dict(row) for row in cursor.fetchall()]


def _connection(db: Any) -> sqlite3.Connection:
    return db.conn if hasattr(db, "conn") else db


def _schema(conn: sqlite3.Connection) -> dict[str, set[str]]:
    rows = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    schema: dict[str, set[str]] = {}
    for row in rows:
        table = row["name"] if isinstance(row, sqlite3.Row) else row[0]
        schema[str(table)] = {
            column[1] for column in conn.execute(f"PRAGMA table_info({table})")
        }
    return schema
