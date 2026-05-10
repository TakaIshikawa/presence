"""Pack WebFetch redirect chain and URL validation analyzer.

Evaluates how well Claude Code sessions handle WebFetch redirects and URL
validation, including proper redirect following, URL validity, and appropriate
tool selection (gh CLI vs WebFetch for GitHub URLs).

Metrics:
- redirect_follow_rate: % of redirects properly followed up
- url_validity_rate: % of WebFetch URLs that are valid/appropriate
- tool_selection_score: Penalty for using WebFetch on GitHub when gh CLI is better
- overall_score: Weighted composite (0-1)
- issues: List of detected problems
"""

from __future__ import annotations

import re
from typing import Any, Mapping


class PackWebFetchRedirectAnalyzer:
    """Analyzes WebFetch redirect handling and URL validation across pack sessions."""

    GITHUB_URL_PATTERN = re.compile(r"https?://(?:api\.)?github\.com/")

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze WebFetch usage patterns across pack records.

        Args:
            records: List of pack dictionaries with sessions/messages/tool_calls.

        Returns:
            Dict with redirect_follow_rate, url_validity_rate,
            tool_selection_score, overall_score, and issues.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of pack dictionaries")

        total_webfetch_calls = 0
        redirect_responses = 0
        redirects_followed = 0
        github_url_webfetch = 0
        malformed_urls = 0
        issues: list[str] = []

        for record in records:
            if not isinstance(record, Mapping):
                continue

            sessions = record.get("sessions")
            if not isinstance(sessions, list):
                continue

            for session in sessions:
                if not isinstance(session, Mapping):
                    continue

                messages = session.get("messages")
                if not isinstance(messages, list):
                    continue

                # Process messages sequentially to detect redirect follow-through
                for i, message in enumerate(messages):
                    if not isinstance(message, Mapping):
                        continue

                    tool_calls = message.get("tool_calls")
                    if not isinstance(tool_calls, list):
                        continue

                    for tc in tool_calls:
                        if not isinstance(tc, Mapping):
                            continue
                        if tc.get("tool_name") != "WebFetch":
                            continue

                        total_webfetch_calls += 1
                        url = tc.get("url", "")

                        # Check URL validity
                        if not self._is_valid_url(url):
                            malformed_urls += 1

                        # Check GitHub URL usage
                        if self.GITHUB_URL_PATTERN.search(url):
                            github_url_webfetch += 1

                        # Check if this was a redirect response
                        result = tc.get("result", "")
                        if self._is_redirect_response(result):
                            redirect_responses += 1
                            # Check if next message follows up with redirect URL
                            if self._redirect_was_followed(messages, i, result):
                                redirects_followed += 1

        # Handle no WebFetch calls — neutral
        if total_webfetch_calls == 0:
            return {
                "redirect_follow_rate": 1.0,
                "url_validity_rate": 1.0,
                "tool_selection_score": 1.0,
                "overall_score": 1.0,
                "issues": [],
            }

        # Calculate metrics
        redirect_follow_rate = (
            redirects_followed / redirect_responses
            if redirect_responses > 0
            else 1.0
        )

        valid_urls = total_webfetch_calls - malformed_urls
        url_validity_rate = valid_urls / total_webfetch_calls

        # Tool selection: penalize GitHub URL usage via WebFetch
        non_github_calls = total_webfetch_calls - github_url_webfetch
        tool_selection_score = non_github_calls / total_webfetch_calls

        # Overall weighted score
        overall_score = (
            0.4 * redirect_follow_rate
            + 0.3 * url_validity_rate
            + 0.3 * tool_selection_score
        )

        # Generate issues
        if redirect_responses > redirects_followed:
            missed = redirect_responses - redirects_followed
            issues.append(
                f"{missed} redirect(s) not followed with subsequent WebFetch call."
            )
        if github_url_webfetch > 0:
            issues.append(
                f"{github_url_webfetch} GitHub URL(s) fetched via WebFetch instead of gh CLI."
            )
        if malformed_urls > 0:
            issues.append(f"{malformed_urls} malformed URL(s) detected.")

        return {
            "redirect_follow_rate": round(redirect_follow_rate, 4),
            "url_validity_rate": round(url_validity_rate, 4),
            "tool_selection_score": round(tool_selection_score, 4),
            "overall_score": round(overall_score, 4),
            "issues": issues,
        }

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is minimally valid."""
        if not isinstance(url, str):
            return False
        return bool(re.match(r"https?://[^\s]+", url))

    def _is_redirect_response(self, result: str) -> bool:
        """Check if tool result indicates a redirect."""
        if not isinstance(result, str):
            return False
        return "redirect" in result.lower() and "different host" in result.lower()

    def _redirect_was_followed(
        self, messages: list, current_idx: int, redirect_result: str
    ) -> bool:
        """Check if a redirect was followed in subsequent messages."""
        # Look at next few messages for a WebFetch call
        for j in range(current_idx + 1, min(current_idx + 3, len(messages))):
            msg = messages[j]
            if not isinstance(msg, Mapping):
                continue
            tool_calls = msg.get("tool_calls")
            if not isinstance(tool_calls, list):
                continue
            for tc in tool_calls:
                if isinstance(tc, Mapping) and tc.get("tool_name") == "WebFetch":
                    return True
        return False
