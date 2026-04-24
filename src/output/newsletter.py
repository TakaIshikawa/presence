"""Newsletter assembly and Buttondown delivery."""

import json
import logging
import re
import requests
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Optional
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

from evaluation.newsletter_subject_performance import score_subject_performance
from storage.db import Database

logger = logging.getLogger(__name__)

if not hasattr(requests, "RequestException"):
    requests.RequestException = Exception

if not hasattr(requests, "Session"):
    class _MissingRequestsSession:
        def __init__(self, *args, **kwargs):
            raise RuntimeError("requests.Session is unavailable")

    requests.Session = _MissingRequestsSession


@dataclass
class NewsletterResult:
    success: bool
    issue_id: Optional[str] = None
    url: Optional[str] = None
    error: Optional[str] = None


@dataclass
class NewsletterContent:
    subject: str
    body_markdown: str
    source_content_ids: list[int] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)
    subject_candidates: list["NewsletterSubjectCandidate"] = field(default_factory=list)


@dataclass
class NewsletterSubjectCandidate:
    subject: str
    score: float
    rationale: str = ""
    source: str = "heuristic"
    metadata: dict = field(default_factory=dict)


@dataclass
class NewsletterMetrics:
    issue_id: str
    opens: int = 0
    clicks: int = 0
    unsubscribes: int = 0
    link_clicks: list["NewsletterLinkClick"] = field(default_factory=list)


@dataclass
class NewsletterLinkClick:
    url: str
    clicks: int = 0
    unique_clicks: Optional[int] = None
    raw_url: Optional[str] = None
    raw_metrics: dict = field(default_factory=dict)


@dataclass
class NewsletterSubscriberMetrics:
    subscriber_count: int
    active_subscriber_count: Optional[int] = None
    unsubscribes: Optional[int] = None
    churn_rate: Optional[float] = None
    new_subscribers: Optional[int] = None
    net_subscriber_change: Optional[int] = None
    raw_metrics: dict = field(default_factory=dict)


TRACKING_QUERY_PARAMS = {
    "bd_analytics",
    "bd_tracking",
    "ck_subscriber_id",
    "content_id",
    "dclid",
    "fbclid",
    "gclid",
    "igshid",
    "li_fat_id",
    "mc_cid",
    "mc_eid",
    "mkt_tok",
    "msclkid",
    "oly_anon_id",
    "oly_enc_id",
    "vero_conv",
    "vero_id",
    "wickedid",
    "yclid",
}


def newsletter_subject_candidate_to_dict(
    candidate: NewsletterSubjectCandidate,
) -> dict:
    """Serialize a subject candidate for preview/review output."""
    return {
        "subject": candidate.subject,
        "score": candidate.score,
        "rationale": candidate.rationale,
        "source": candidate.source,
        "metadata": candidate.metadata,
    }


def select_newsletter_subject(content: NewsletterContent) -> str:
    """Select the subject that would be used for the assembled newsletter."""
    candidates = content.subject_candidates or []
    if candidates:
        return candidates[0].subject
    return content.subject


def newsletter_preview_payload(
    content: NewsletterContent,
    week_start: datetime,
    week_end: datetime,
    *,
    subject: str = "",
    utm_metadata: Optional[dict] = None,
    message: str = "",
) -> dict:
    """Build a JSON-serializable newsletter review payload."""
    selected_subject = subject or select_newsletter_subject(content)
    candidates = [
        newsletter_subject_candidate_to_dict(candidate)
        for candidate in (content.subject_candidates or [])
    ]
    metadata = dict(content.metadata or {})
    return {
        "subject": selected_subject,
        "body_markdown": content.body_markdown,
        "source_content_ids": list(content.source_content_ids or []),
        "metadata": metadata,
        "utm_metadata": dict(utm_metadata or metadata),
        "subject_candidates": candidates,
        "week_range": {
            "start": week_start.isoformat(),
            "end": week_end.isoformat(),
        },
        "message": message,
    }


def format_newsletter_preview_json(payload: dict) -> str:
    """Format a newsletter preview payload as JSON."""
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def format_newsletter_preview_markdown(payload: dict) -> str:
    """Format a newsletter preview payload as Markdown."""
    subject = payload.get("subject") or ""
    message = payload.get("message") or ""
    source_ids = ", ".join(
        str(item) for item in (payload.get("source_content_ids") or [])
    ) or "None"
    candidate_lines = [
        (
            f"- {candidate['subject']} "
            f"(score: {candidate['score']}, source: {candidate['source']})"
            + (f" - {candidate['rationale']}" if candidate.get("rationale") else "")
        )
        for candidate in (payload.get("subject_candidates") or [])
    ] or ["- None"]
    metadata_json = format_newsletter_preview_json(
        payload.get("utm_metadata") or {}
    ).strip()
    body = payload.get("body_markdown") or ""
    if not body.strip():
        body = "_No newsletter content for this date range._"

    parts = [
        "# Newsletter Preview",
        "## Week Range",
        f"- Start: {payload['week_range']['start']}",
        f"- End: {payload['week_range']['end']}",
        "## Subject",
        subject or "None",
    ]
    if message:
        parts.extend(["## Message", message])
    parts.extend(
        [
            "## Source Content IDs",
            source_ids,
            "## UTM Metadata",
            f"```json\n{metadata_json}\n```",
            "## Subject Candidates",
            "\n".join(candidate_lines),
            "## Body",
            body,
        ]
    )
    return "\n\n".join(parts) + "\n"


class NewsletterAssembler:
    """Assembles newsletter content from the week's published posts."""

    def __init__(
        self,
        db: Database,
        site_url: str = "https://takaishikawa.com",
        utm_source: str = "",
        utm_medium: str = "",
        utm_campaign_template: str = "",
        repeat_lookback_weeks: int = 8,
    ):
        self.db = db
        self.site_url = site_url.rstrip("/")
        self.utm_source = utm_source
        self.utm_medium = utm_medium
        self.utm_campaign_template = utm_campaign_template
        self.repeat_lookback_weeks = max(int(repeat_lookback_weeks or 0), 0)

    def assemble(
        self, week_start: datetime, week_end: datetime
    ) -> NewsletterContent:
        """Gather this week's blog post + top threads + top posts into a newsletter."""
        content_ids = []
        sections = []
        subject_context = {
            "blog_titles": [],
            "thread_hooks": [],
            "post_hooks": [],
            "content_types": [],
        }
        source_preferences = self._load_resonant_source_preferences()
        utm_campaign = self._build_utm_campaign(week_start, week_end)
        suppressed_content_ids = self._load_recent_newsletter_content_ids(week_end)
        suppressed_ids = set(suppressed_content_ids)
        metadata = {}
        if suppressed_content_ids:
            metadata["suppressed_content_ids"] = suppressed_content_ids
            metadata["repeat_lookback_weeks"] = self.repeat_lookback_weeks
        query_week_end = self._query_week_end(week_end)

        # 1. Blog post (if published this week)
        blog_posts = self.db.get_published_content_in_range(
            "blog_post", week_start, query_week_end
        )
        blog_posts = self._suppress_recent_content(blog_posts, suppressed_ids)
        blog_posts = self._sort_by_source_preferences(blog_posts, source_preferences)
        if blog_posts:
            post = blog_posts[0]
            content_ids.append(post["id"])
            title = self._extract_blog_title(post["content"])
            excerpt = self._extract_blog_excerpt(post["content"], max_lines=3)
            url = post.get("published_url", "")
            url = self._rewrite_internal_link(url, post["id"], utm_campaign)
            subject_context["blog_titles"].append(title)
            subject_context["content_types"].append("blog_post")
            sections.append((
                "blog_post",
                f"## This Week's Post\n\n"
                f"**[{title}]({url})**\n\n"
                f"{excerpt}\n\n"
                f"[Read the full post]({url})",
            ))

        # 2. Top threads (up to 2)
        threads = self.db.get_published_content_in_range(
            "x_thread", week_start, query_week_end
        )
        threads = self._suppress_recent_content(threads, suppressed_ids)
        threads = self._sort_by_source_preferences(threads, source_preferences)
        if threads:
            thread_items = []
            for thread in threads[:2]:
                content_ids.append(thread["id"])
                first_tweet = self._extract_first_tweet(thread["content"])
                url = thread.get("published_url", "")
                url = self._rewrite_internal_link(url, thread["id"], utm_campaign)
                link = f" ([thread]({url}))" if url else ""
                thread_items.append(f"- {first_tweet}{link}")
                subject_context["thread_hooks"].append(first_tweet)
                subject_context["content_types"].append("x_thread")
            sections.append(
                ("x_thread", "## Threads\n\n" + "\n\n".join(thread_items))
            )

        # 3. Top posts by engagement (up to 3)
        posts = self.db.get_published_content_in_range(
            "x_post", week_start, query_week_end
        )
        posts = self._suppress_recent_content(posts, suppressed_ids)
        posts = self._sort_by_source_preferences(posts, source_preferences)
        if posts:
            post_items = []
            for post in posts[:3]:
                content_ids.append(post["id"])
                url = post.get("published_url", "")
                url = self._rewrite_internal_link(url, post["id"], utm_campaign)
                link = f" ([link]({url}))" if url else ""
                post_items.append(f"> {post['content']}{link}")
                subject_context["post_hooks"].append(post["content"])
                subject_context["content_types"].append("x_post")
            sections.append(
                ("x_post", "## Posts\n\n" + "\n\n".join(post_items))
            )

        if not sections:
            return NewsletterContent(
                subject="",
                body_markdown="",
                source_content_ids=[],
                metadata=metadata,
            )

        # Compose the full newsletter
        date_str = week_end.strftime("%B %d, %Y")
        subject = f"Building with AI — Week of {week_start.strftime('%b %d')}"
        subject_candidates = self.generate_subject_candidates(
            week_start,
            week_end,
            subject_context=subject_context,
            fallback_subject=subject,
        )

        body = f"# Weekly Digest\n\n{date_str}\n\n"
        ordered_sections = self._order_sections(sections, source_preferences)
        body += "\n\n---\n\n".join(section for _, section in ordered_sections)
        body += (
            "\n\n---\n\n"
            f"*Shipped from [takaishikawa.com]({self.site_url})*"
        )

        if utm_campaign:
            metadata["utm_campaign"] = utm_campaign

        return NewsletterContent(
            subject=subject,
            body_markdown=body,
            source_content_ids=content_ids,
            metadata=metadata,
            subject_candidates=subject_candidates,
        )

    def generate_subject_candidates(
        self,
        week_start: datetime,
        week_end: datetime,
        subject_context: Optional[dict] = None,
        fallback_subject: str = "",
        subject_history: Optional[list[dict]] = None,
    ) -> list[NewsletterSubjectCandidate]:
        """Create scored subject candidates for the assembled issue."""
        fallback_subject = fallback_subject or (
            f"Building with AI — Week of {week_start.strftime('%b %d')}"
        )
        subject_context = subject_context or {}
        blog_titles = subject_context.get("blog_titles") or []
        thread_hooks = subject_context.get("thread_hooks") or []
        post_hooks = subject_context.get("post_hooks") or []
        content_types = subject_context.get("content_types") or []
        subject_history = (
            subject_history
            if subject_history is not None
            else self._load_subject_performance_history()
        )
        history_profile = self._build_subject_history_profile(subject_history)

        candidates = [fallback_subject]
        if blog_titles:
            candidates.append(blog_titles[0])
            candidates.append(f"This week: {blog_titles[0]}")

        top_hook = self._first_meaningful_phrase(thread_hooks + post_hooks)
        if top_hook:
            candidates.append(f"This week: {top_hook}")

        mix_label = self._subject_mix_label(content_types)
        if mix_label:
            candidates.append(
                f"{mix_label} from the week of {week_start.strftime('%b %d')}"
            )

        candidates.append(f"Weekly Digest — {week_end.strftime('%b %d, %Y')}")

        seen = set()
        scored = []
        for candidate in candidates:
            normalized = self._normalize_subject(candidate)
            key = normalized.lower()
            if not normalized or key in seen:
                continue
            seen.add(key)
            score, rationale = self._score_subject_candidate(
                normalized,
                fallback_subject=fallback_subject,
                context_terms=blog_titles + thread_hooks + post_hooks,
            )
            history_bonus, history_rationale, history_metadata = (
                self._score_subject_history(
                    normalized,
                    history_profile,
                )
            )
            if history_bonus:
                score = round(score + history_bonus, 2)
            if history_rationale:
                rationale = (
                    f"{rationale}, {history_rationale}"
                    if rationale
                    else history_rationale
                )
            scored.append(
                NewsletterSubjectCandidate(
                    subject=normalized,
                    score=score,
                    rationale=rationale,
                    metadata={
                        "week_start": week_start.strftime("%Y-%m-%d"),
                        "week_end": week_end.strftime("%Y-%m-%d"),
                        **history_metadata,
                    },
                )
            )

        return sorted(scored, key=lambda item: (-item.score, item.subject.lower()))

    def _load_subject_performance_history(self, days: int = 180) -> list[dict]:
        """Load prior subject performance rows when the database supports it."""
        getter = getattr(self.db, "get_newsletter_subject_performance", None)
        if not callable(getter):
            return []
        try:
            return getter(days=days)
        except Exception as e:
            logger.debug("Newsletter subject history lookup failed: %s", e)
            return []

    @staticmethod
    def _build_subject_history_profile(rows: list[dict]) -> dict:
        """Convert historical subject rows into reusable token weights."""
        if not rows:
            return {}

        scored_rows = []
        for row in rows:
            subject = (row.get("subject") or "").strip()
            if not subject:
                continue
            open_rate = row.get("open_rate")
            click_rate = row.get("click_rate")
            unsubscribes = int(row.get("unsubscribes") or 0)
            subscriber_count = int(row.get("subscriber_count") or 0)
            scored_rows.append(
                {
                    "subject": subject,
                    "performance_score": score_subject_performance(
                        float(open_rate) if open_rate is not None else None,
                        float(click_rate) if click_rate is not None else None,
                        unsubscribes=unsubscribes,
                        subscriber_count=subscriber_count,
                    ),
                    "sent_at": row.get("sent_at") or "",
                    "open_rate": row.get("open_rate"),
                    "click_rate": row.get("click_rate"),
                }
            )

        if not scored_rows:
            return {}

        performance_scores = [row["performance_score"] for row in scored_rows]
        baseline = median(performance_scores)
        spread = max(max(performance_scores) - min(performance_scores), 1.0)

        token_weights = defaultdict(float)
        subject_signals = []
        for row in scored_rows:
            performance = row["performance_score"]
            if performance <= baseline:
                continue
            normalized = min((performance - baseline) / spread, 1.0)
            recency = NewsletterAssembler._history_recency_weight(row["sent_at"])
            weight = round(max(normalized, 0.0) * recency, 4)
            if weight <= 0:
                continue
            tokens = NewsletterAssembler._subject_tokens(row["subject"])
            if not tokens:
                continue
            subject_signals.append(
                {
                    "subject": row["subject"],
                    "performance_score": performance,
                    "weight": weight,
                    "tokens": tokens,
                }
            )
            for token in tokens:
                token_weights[token] += weight

        return {
            "baseline": baseline,
            "spread": spread,
            "token_weights": dict(token_weights),
            "subjects": subject_signals,
        }

    @staticmethod
    def _score_subject_history(subject: str, profile: dict) -> tuple[float, str, dict]:
        """Return a small history bonus plus explanation metadata for a subject."""
        if not profile:
            return 0.0, "", {}

        token_weights = profile.get("token_weights") or {}
        if not token_weights:
            return 0.0, "", {}

        tokens = NewsletterAssembler._subject_tokens(subject)
        if not tokens:
            return 0.0, "", {}

        matched_tokens = []
        raw_bonus = 0.0
        for token in tokens:
            token_weight = float(token_weights.get(token, 0.0))
            if token_weight <= 0:
                continue
            matched_tokens.append((token, round(token_weight, 4)))
            raw_bonus += token_weight

        if not matched_tokens:
            return 0.0, "", {}

        bonus = min(2.5, round(0.35 + raw_bonus * 1.5, 2))
        matched_subjects = []
        for signal in (profile.get("subjects") or [])[:5]:
            signal_tokens = set(signal.get("tokens") or [])
            if signal_tokens.intersection({token for token, _ in matched_tokens}):
                matched_subjects.append(
                    {
                        "subject": signal.get("subject", ""),
                        "performance_score": signal.get("performance_score", 0.0),
                        "weight": signal.get("weight", 0.0),
                    }
                )
        rationale = (
            "history match: "
            + ", ".join(token for token, _ in matched_tokens[:3])
        )
        metadata = {
            "history": {
                "bonus": bonus,
                "matched_tokens": [token for token, _ in matched_tokens],
                "matched_subjects": matched_subjects[:3],
                "baseline_performance": profile.get("baseline"),
                "profiled_subjects": len(profile.get("subjects") or []),
            }
        }
        return bonus, rationale, metadata

    @staticmethod
    def _history_recency_weight(sent_at: str) -> float:
        """Favor more recent performance signals without excluding older wins."""
        if not sent_at:
            return 0.7
        try:
            parsed = datetime.fromisoformat(sent_at)
        except (TypeError, ValueError):
            return 0.7
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        age_days = max((datetime.now(timezone.utc) - parsed).days, 0)
        return max(0.35, 1.0 - (age_days / 365.0))

    @staticmethod
    def _subject_tokens(subject: str) -> list[str]:
        """Extract reusable tokens from a subject line."""
        stopwords = {
            "ai",
            "and",
            "from",
            "into",
            "notes",
            "note",
            "post",
            "this",
            "the",
            "week",
            "weekly",
            "with",
            "your",
            "digest",
            "update",
            "issue",
            "building",
        }
        tokens = []
        for token in re.findall(r"[A-Za-z][A-Za-z0-9'-]{1,}", subject.lower()):
            cleaned = token.strip("'-.")
            if len(cleaned) < 2 or cleaned in stopwords:
                continue
            tokens.append(cleaned)
        return tokens

    @staticmethod
    def _normalize_subject(subject: str, max_length: int = 90) -> str:
        """Trim generated subject text to a Buttondown-friendly one-liner."""
        subject = re.sub(r"\s+", " ", subject or "").strip(" -:\n\t")
        if len(subject) <= max_length:
            return subject
        shortened = subject[:max_length].rsplit(" ", 1)[0].strip(" -:")
        return shortened or subject[:max_length].strip()

    @staticmethod
    def _first_meaningful_phrase(items: list[str]) -> str:
        """Return a compact phrase from the first current issue hook."""
        for item in items:
            text = re.sub(r"https?://\S+", "", item or "")
            text = re.sub(r"[*_`>#\[\]()]", "", text)
            text = re.sub(r"\s+", " ", text).strip()
            if text:
                return text.split(". ")[0].strip()
        return ""

    @staticmethod
    def _subject_mix_label(content_types: list[str]) -> str:
        labels = []
        if "blog_post" in content_types:
            labels.append("post")
        if "x_thread" in content_types:
            labels.append("threads")
        if "x_post" in content_types:
            labels.append("notes")
        if not labels:
            return ""
        if len(labels) == 1:
            return labels[0].title()
        return ", ".join(labels[:-1]).title() + f" and {labels[-1]}"

    @staticmethod
    def _score_subject_candidate(
        subject: str, fallback_subject: str, context_terms: list[str]
    ) -> tuple[float, str]:
        """Score subject lines for specificity, readability, and email fit."""
        score = 5.0
        reasons = []
        length = len(subject)

        if 28 <= length <= 70:
            score += 1.5
            reasons.append("clear length")
        elif length < 20:
            score -= 1.0
            reasons.append("short")
        elif length > 80:
            score -= 1.0
            reasons.append("long")

        lower_subject = subject.lower()
        context_words = {
            word.lower()
            for term in context_terms
            for word in re.findall(r"[A-Za-z][A-Za-z0-9'-]{3,}", term or "")
        }
        if context_words and any(word in lower_subject for word in context_words):
            score += 1.5
            reasons.append("issue-specific")
        if "weekly digest" in lower_subject or "week of" in lower_subject:
            score += 0.5
            reasons.append("recognizable series")
        if subject == fallback_subject:
            score += 0.25
            reasons.append("default format")
        if re.search(r"\b(ai|agents?|shipping|building|post|threads?|notes?)\b", lower_subject):
            score += 0.5
            reasons.append("topical")
        if subject.count("!") > 0 or subject.isupper():
            score -= 1.0
            reasons.append("salesy")

        return round(max(score, 0.0), 2), ", ".join(reasons) or "baseline"

    def _build_utm_campaign(
        self, week_start: datetime, week_end: datetime
    ) -> str:
        """Render configured UTM campaign template for this issue."""
        if not (
            self.utm_source
            and self.utm_medium
            and self.utm_campaign_template
        ):
            return ""
        context = {
            "week_start": week_start.strftime("%Y-%m-%d"),
            "week_end": week_end.strftime("%Y-%m-%d"),
            "week_start_compact": week_start.strftime("%Y%m%d"),
            "week_end_compact": week_end.strftime("%Y%m%d"),
        }
        try:
            return self.utm_campaign_template.format(**context)
        except (KeyError, ValueError) as e:
            logger.debug(f"Newsletter UTM campaign template failed: {e}")
            return self.utm_campaign_template

    def _query_week_end(self, week_end: datetime) -> datetime:
        """Allow near-current delayed sends to include content published since week_end."""
        now = datetime.now(timezone.utc)
        normalized_week_end = week_end
        if normalized_week_end.tzinfo is None:
            normalized_week_end = normalized_week_end.replace(tzinfo=timezone.utc)
        if now > normalized_week_end and now - normalized_week_end <= timedelta(days=3):
            return now
        return week_end

    def _rewrite_internal_link(
        self, url: str, content_id: int, utm_campaign: str
    ) -> str:
        """Add UTM/click attribution to internal links only."""
        if not url or not utm_campaign:
            return url

        parsed_url = urlparse(url)
        parsed_site = urlparse(self.site_url)
        if parsed_url.netloc and parsed_url.netloc != parsed_site.netloc:
            return url
        if parsed_url.scheme and parsed_url.scheme not in ("http", "https"):
            return url

        query = dict(parse_qsl(parsed_url.query, keep_blank_values=True))
        query.update({
            "utm_source": self.utm_source,
            "utm_medium": self.utm_medium,
            "utm_campaign": utm_campaign,
            "content_id": str(content_id),
        })
        return urlunparse(parsed_url._replace(query=urlencode(query)))

    def _load_resonant_source_preferences(self) -> list[dict]:
        """Load content type/format patterns from prior resonant sends."""
        getter = getattr(self.db, "get_resonant_newsletter_source_patterns", None)
        if getter is None:
            return []
        try:
            return getter()
        except Exception as e:
            logger.debug(f"Newsletter source preference lookup failed: {e}")
            return []

    def _load_recent_newsletter_content_ids(self, week_end: datetime) -> list[int]:
        """Load content IDs featured in recent newsletter sends."""
        if self.repeat_lookback_weeks <= 0:
            return []

        getter = getattr(self.db, "get_recent_newsletter_source_content_ids", None)
        try:
            if callable(getter):
                return getter(weeks=self.repeat_lookback_weeks, before=week_end)

            conn = getattr(self.db, "conn", None)
            if conn is None:
                return []
            cutoff = week_end - timedelta(weeks=self.repeat_lookback_weeks)
            cursor = conn.execute(
                """SELECT source_content_ids
                   FROM newsletter_sends
                   WHERE source_content_ids IS NOT NULL
                     AND datetime(sent_at) >= datetime(?)
                     AND datetime(sent_at) < datetime(?)
                   ORDER BY sent_at DESC""",
                (cutoff.isoformat(), week_end.isoformat()),
            )
        except Exception as e:
            logger.debug("Newsletter repeat suppression lookup failed: %s", e)
            return []

        content_ids = []
        seen = set()
        for row in cursor.fetchall():
            try:
                raw_ids = json.loads(row["source_content_ids"] or "[]")
            except (TypeError, json.JSONDecodeError):
                continue
            for raw_id in raw_ids:
                try:
                    content_id = int(raw_id)
                except (TypeError, ValueError):
                    continue
                if content_id not in seen:
                    seen.add(content_id)
                    content_ids.append(content_id)
        return content_ids

    @staticmethod
    def _suppress_recent_content(
        items: list[dict], suppressed_ids: set[int]
    ) -> list[dict]:
        """Remove items already featured in recent newsletters."""
        if not suppressed_ids:
            return items
        return [item for item in items if int(item.get("id", 0)) not in suppressed_ids]

    @staticmethod
    def _sort_by_source_preferences(
        items: list[dict], preferences: list[dict]
    ) -> list[dict]:
        """Prefer current content matching prior resonant type/format patterns."""
        if not preferences:
            return items

        format_rank = {}
        type_rank = {}
        for index, pref in enumerate(preferences):
            content_type = pref.get("content_type")
            content_format = pref.get("content_format")
            if content_type and content_type not in type_rank:
                type_rank[content_type] = index
            if content_type and content_format:
                format_rank[(content_type, content_format)] = index

        def preference_key(item: dict) -> tuple[int, int]:
            content_type = item.get("content_type")
            content_format = item.get("content_format")
            return (
                format_rank.get((content_type, content_format), len(preferences)),
                type_rank.get(content_type, len(preferences)),
            )

        return sorted(items, key=preference_key)

    @staticmethod
    def _order_sections(
        sections: list[tuple[str, str]], preferences: list[dict]
    ) -> list[tuple[str, str]]:
        """Order newsletter sections by prior resonant content-type mix."""
        if not preferences:
            return sections

        default_order = {"blog_post": 0, "x_thread": 1, "x_post": 2}
        type_rank = {}
        for index, pref in enumerate(preferences):
            content_type = pref.get("content_type")
            if content_type and content_type not in type_rank:
                type_rank[content_type] = index

        return sorted(
            sections,
            key=lambda section: (
                type_rank.get(section[0], len(preferences)),
                default_order.get(section[0], len(default_order)),
            ),
        )

    @staticmethod
    def _extract_blog_title(content: str) -> str:
        """Extract title from blog post content."""
        match = re.search(r"^TITLE:\s*(.+)$", content, re.MULTILINE)
        return match.group(1).strip() if match else "This Week's Post"

    @staticmethod
    def _extract_blog_excerpt(content: str, max_lines: int = 3) -> str:
        """Extract first few non-title, non-header lines as excerpt."""
        lines = content.split("\n")
        excerpt_lines = []
        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("TITLE:") or stripped.startswith("#"):
                continue
            excerpt_lines.append(stripped)
            if len(excerpt_lines) >= max_lines:
                break
        return " ".join(excerpt_lines)

    @staticmethod
    def _extract_first_tweet(content: str) -> str:
        """Extract the first tweet from a thread."""
        match = re.search(
            r"TWEET\s+1:\s*\n(.+?)(?:\n\s*\nTWEET\s+\d+:|$)",
            content, re.DOTALL
        )
        if match:
            return match.group(1).strip()
        # Fallback: first non-empty line
        for line in content.split("\n"):
            stripped = line.strip()
            if stripped and not stripped.startswith("TWEET"):
                return stripped
        return content[:100]


class ButtondownClient:
    """Buttondown API client for sending newsletter emails."""

    BASE_URL = "https://api.buttondown.com/v1"

    def __init__(self, api_key: str, timeout: int = 30):
        self.api_key = api_key
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers["Authorization"] = f"Token {api_key}"

    def send(
        self, subject: str, body: str, publish: bool = True
    ) -> NewsletterResult:
        """Send a newsletter issue via Buttondown API."""
        status = "published" if publish else "draft"
        try:
            response = self.session.post(
                f"{self.BASE_URL}/emails",
                json={
                    "subject": subject,
                    "body": body,
                    "status": status,
                },
                timeout=self.timeout,
            )
            if response.status_code in (200, 201):
                data = response.json()
                return NewsletterResult(
                    success=True,
                    issue_id=data.get("id", ""),
                    url=data.get("absolute_url", ""),
                )
            else:
                return NewsletterResult(
                    success=False,
                    error=f"HTTP {response.status_code}: {response.text[:200]}",
                )
        except requests.RequestException as e:
            return NewsletterResult(success=False, error=str(e))

    def get_subscriber_count(self) -> int:
        """Get current subscriber count."""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/subscribers",
                params={"type": "regular"},
                timeout=self.timeout,
            )
            if response.status_code == 200:
                return response.json().get("count", 0)
        except requests.RequestException as e:
            logger.debug(f"Subscriber count fetch failed: {e}")
        return 0

    def get_subscriber_metrics(self) -> Optional[NewsletterSubscriberMetrics]:
        """Fetch aggregate newsletter subscriber metrics when available."""
        try:
            active_response = self.session.get(
                f"{self.BASE_URL}/subscribers",
                params={"type": "regular"},
                timeout=self.timeout,
            )
            if active_response.status_code != 200:
                logger.warning(
                    "Subscriber metrics fetch failed: HTTP %s",
                    active_response.status_code,
                )
                return None

            active_data = active_response.json()
            raw_metrics = dict(active_data) if isinstance(active_data, dict) else {}
            active_count = self._extract_int(
                raw_metrics,
                "active_subscriber_count",
                "active_subscribers",
                "regular_subscribers",
                "subscriber_count",
                "count",
            )
            subscriber_count = self._extract_int(
                raw_metrics,
                "subscriber_count",
                "total_subscribers",
                "count",
            )

            unsubscribes = self._extract_int(
                raw_metrics,
                "unsubscribes",
                "unsubscriptions",
                "unsubscribed_count",
                "unsubscribed_subscribers",
            )
            if unsubscribes is None:
                unsubscribes = self._fetch_subscriber_type_count("unsubscribed")
                if unsubscribes is not None:
                    raw_metrics["unsubscribed_count"] = unsubscribes

            churn_rate = self._extract_float(
                raw_metrics,
                "churn_rate",
                "unsubscribe_rate",
                "unsubscription_rate",
            )
            new_subscribers = self._extract_int(
                raw_metrics,
                "new_subscribers",
                "subscribers_added",
                "new_subscriber_count",
            )
            net_change = self._extract_int(
                raw_metrics,
                "net_subscriber_change",
                "net_subscribers",
                "subscriber_delta",
            )

            if subscriber_count is None:
                subscriber_count = active_count or 0

            return NewsletterSubscriberMetrics(
                subscriber_count=subscriber_count,
                active_subscriber_count=active_count,
                unsubscribes=unsubscribes,
                churn_rate=churn_rate,
                new_subscribers=new_subscribers,
                net_subscriber_change=net_change,
                raw_metrics=raw_metrics,
            )
        except (ValueError, TypeError, requests.RequestException) as e:
            logger.warning("Subscriber metrics fetch failed: %s", e)
            return None

    def _fetch_subscriber_type_count(self, subscriber_type: str) -> Optional[int]:
        """Fetch a Buttondown subscriber count for one subscriber type."""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/subscribers",
                params={"type": subscriber_type},
                timeout=self.timeout,
            )
            if response.status_code != 200:
                return None
            data = response.json()
            if not isinstance(data, dict):
                return None
            return self._extract_int(data, "count")
        except (ValueError, TypeError, requests.RequestException):
            return None

    @staticmethod
    def _extract_int(data: dict, *keys: str) -> Optional[int]:
        for key in keys:
            value = data.get(key)
            if value is None:
                continue
            try:
                return int(value)
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _extract_float(data: dict, *keys: str) -> Optional[float]:
        for key in keys:
            value = data.get(key)
            if value is None:
                continue
            try:
                return float(value)
            except (TypeError, ValueError):
                continue
        return None

    def get_email_analytics(self, issue_id: str) -> Optional[NewsletterMetrics]:
        """Fetch aggregate and link-level analytics for a Buttondown email issue."""
        try:
            response = self.session.get(
                f"{self.BASE_URL}/emails/{issue_id}/analytics",
                timeout=self.timeout,
            )
            if response.status_code == 200:
                data = response.json()
                return NewsletterMetrics(
                    issue_id=issue_id,
                    opens=int(data.get("opens") or 0),
                    clicks=int(data.get("clicks") or 0),
                    unsubscribes=int(data.get("unsubscriptions") or 0),
                    link_clicks=self._parse_link_clicks(data),
                )
        except (ValueError, TypeError, requests.RequestException):
            pass
        return None

    @classmethod
    def _parse_link_clicks(cls, data: dict) -> list[NewsletterLinkClick]:
        """Normalize Buttondown per-link analytics from known response shapes."""
        candidates = []
        for key in (
            "links",
            "link_clicks",
            "clicks_by_link",
            "clicks_by_url",
            "url_clicks",
        ):
            value = data.get(key)
            if isinstance(value, list):
                candidates.extend(item for item in value if isinstance(item, dict))
            elif isinstance(value, dict):
                for url, clicks in value.items():
                    if isinstance(clicks, dict):
                        item = dict(clicks)
                        item.setdefault("url", url)
                        candidates.append(item)
                    else:
                        candidates.append({"url": url, "clicks": clicks})

        merged: dict[str, NewsletterLinkClick] = {}
        for item in candidates:
            raw_url = cls._extract_link_url(item)
            if not raw_url:
                continue
            normalized_url = normalize_newsletter_link_url(raw_url)
            if not normalized_url:
                continue
            clicks = cls._extract_int(
                item,
                "clicks",
                "click_count",
                "total_clicks",
                "count",
            ) or 0
            unique_clicks = cls._extract_int(
                item,
                "unique_clicks",
                "unique_click_count",
                "unique_count",
            )
            existing = merged.get(normalized_url)
            if existing:
                existing.clicks += clicks
                if unique_clicks is not None:
                    existing.unique_clicks = (existing.unique_clicks or 0) + unique_clicks
                existing.raw_metrics.setdefault("sources", []).append(item)
            else:
                merged[normalized_url] = NewsletterLinkClick(
                    url=normalized_url,
                    clicks=clicks,
                    unique_clicks=unique_clicks,
                    raw_url=raw_url,
                    raw_metrics={"sources": [item]},
                )

        return list(merged.values())

    @staticmethod
    def _extract_link_url(item: dict) -> str:
        for key in ("url", "href", "target", "destination", "link"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, dict):
                nested = ButtondownClient._extract_link_url(value)
                if nested:
                    return nested
        return ""


def normalize_newsletter_link_url(url: str) -> str:
    """Strip common tracking params while preserving the destination URL."""
    url = (url or "").strip()
    if not url:
        return ""

    parsed = urlparse(url)
    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not _is_tracking_query_param(key)
    ]
    netloc = parsed.netloc.lower()
    if parsed.scheme == "http" and netloc.endswith(":80"):
        netloc = netloc[:-3]
    elif parsed.scheme == "https" and netloc.endswith(":443"):
        netloc = netloc[:-4]

    return urlunparse(
        parsed._replace(
            scheme=parsed.scheme.lower(),
            netloc=netloc,
            query=urlencode(query, doseq=True),
        )
    )


def _is_tracking_query_param(key: str) -> bool:
    normalized = (key or "").lower()
    return normalized.startswith("utm_") or normalized in TRACKING_QUERY_PARAMS
