"""Session prompt template adherence analyzer.

Examines assistant responses and tool parameters to detect prompt template
usage patterns including heredoc markers, template variables, commit message
format adherence, PR description template usage, prompt version references,
and format inconsistencies across similar operations.

Detection dimensions:
1. Template markers: heredoc (EOF, HEREDOC), template variables ({variable}),
   consistent formatting patterns in Task/Bash tool parameters.
2. Commit message format: Co-Authored-By pattern, conventional commit format,
   HEREDOC usage in git commit calls.
3. PR description templates: ## Summary, ## Test plan, standard PR body
   sections in gh pr create calls.
4. Prompt version references: explicit version strings (v1, v2, prompt_v3)
   in tool parameters or assistant responses.
5. Format inconsistency: turns where similar operations use different
   formatting approaches (e.g., one commit uses HEREDOC, another inline -m).
"""

from __future__ import annotations

import re
from typing import Any


_HEREDOC_RE = re.compile(r"<<\s*['\"]?(EOF|HEREDOC|END)\b", re.IGNORECASE)
_TEMPLATE_VAR_RE = re.compile(r"\{[a-zA-Z_][a-zA-Z0-9_]*\}")
_VERSION_REF_RE = re.compile(r"\bv\d+\b|\bprompt_v\d+\b", re.IGNORECASE)
_CONVENTIONAL_COMMIT_RE = re.compile(
    r"^(feat|fix|chore|docs|style|refactor|perf|test|ci|build|revert)"
    r"(\([^)]+\))?:\s",
)
_CO_AUTHORED_RE = re.compile(r"Co-Authored-By:", re.IGNORECASE)
_GIT_COMMIT_RE = re.compile(r"\bgit\s+commit\b")
_GH_PR_CREATE_RE = re.compile(r"\bgh\s+pr\s+create\b")
_PR_SUMMARY_RE = re.compile(r"##\s*Summary", re.IGNORECASE)
_PR_TEST_PLAN_RE = re.compile(r"##\s*Test\s*[Pp]lan", re.IGNORECASE)


def _empty_result() -> dict[str, Any]:
    return {
        "total_template_operations": 0,
        "heredoc_usage_count": 0,
        "template_variable_count": 0,
        "commit_format_adherence": 0,
        "total_commits": 0,
        "pr_template_usage": 0,
        "total_prs": 0,
        "version_references": 0,
        "format_inconsistencies": 0,
        "template_adherence_score": 1.0,
    }


def analyze_session_prompt_template_adherence(records: object) -> dict[str, Any]:
    """Analyze prompt template usage patterns in agent sessions.

    Args:
        records: List of turn dictionaries with keys:
            - turn_index: int
            - tool_name: str
            - tool_params: dict
            - tool_result: str
            - assistant_response: str
            - is_error: bool
            - is_last_turn: bool

    Returns:
        Dict with template adherence metrics and score.

    Raises:
        ValueError: If records is not a list.
    """
    if records is None:
        return _empty_result()
    if not isinstance(records, list):
        raise ValueError("records must be a list of turn dictionaries")
    if not records:
        return _empty_result()

    total_template_operations = 0
    heredoc_usage_count = 0
    template_variable_count = 0
    commit_format_adherence = 0
    total_commits = 0
    pr_template_usage = 0
    total_prs = 0
    version_references = 0

    # Track commit formatting styles for inconsistency detection
    commit_styles: list[str] = []  # "heredoc" or "inline"

    for record in records:
        if not isinstance(record, dict):
            continue

        tool_name = record.get("tool_name", "") or ""
        tool_params = record.get("tool_params") or {}
        if not isinstance(tool_params, dict):
            tool_params = {}
        assistant_response = record.get("assistant_response", "") or ""

        # Combine all string param values for scanning
        param_text = " ".join(
            str(v) for v in tool_params.values() if isinstance(v, str)
        )
        command = tool_params.get("command", "") or ""

        # --- Template markers ---
        if tool_name in ("Bash", "Task"):
            if _HEREDOC_RE.search(param_text):
                heredoc_usage_count += 1
                total_template_operations += 1

            tv_matches = _TEMPLATE_VAR_RE.findall(param_text)
            if tv_matches:
                template_variable_count += len(tv_matches)
                total_template_operations += 1

        # --- Version references ---
        for text in (param_text, assistant_response):
            vr_matches = _VERSION_REF_RE.findall(text)
            if vr_matches:
                version_references += len(vr_matches)

        # --- Commit message format ---
        if tool_name == "Bash" and _GIT_COMMIT_RE.search(command):
            total_commits += 1
            total_template_operations += 1
            adheres = False

            has_heredoc = bool(_HEREDOC_RE.search(command))
            has_coauthor = bool(_CO_AUTHORED_RE.search(command))
            has_conventional = bool(_CONVENTIONAL_COMMIT_RE.search(
                _extract_commit_message(command)
            ))

            if has_heredoc or has_coauthor or has_conventional:
                adheres = True
                commit_format_adherence += 1

            commit_styles.append("heredoc" if has_heredoc else "inline")

        # --- PR description template ---
        if tool_name == "Bash" and _GH_PR_CREATE_RE.search(command):
            total_prs += 1
            total_template_operations += 1
            has_summary = bool(_PR_SUMMARY_RE.search(command))
            has_test_plan = bool(_PR_TEST_PLAN_RE.search(command))
            if has_summary and has_test_plan:
                pr_template_usage += 1

    # --- Format inconsistency detection ---
    format_inconsistencies = 0
    if len(commit_styles) > 1:
        unique_styles = set(commit_styles)
        if len(unique_styles) > 1:
            format_inconsistencies += 1

    # --- Score calculation ---
    commit_score = commit_format_adherence / max(1, total_commits)
    pr_score = pr_template_usage / max(1, total_prs)
    consistency_score = 1.0 - (
        format_inconsistencies / max(1, total_template_operations)
    )

    raw_score = (
        0.4 * commit_score
        + 0.3 * pr_score
        + 0.3 * max(0.0, consistency_score)
    )
    template_adherence_score = round(min(1.0, max(0.0, raw_score)), 3)

    return {
        "total_template_operations": total_template_operations,
        "heredoc_usage_count": heredoc_usage_count,
        "template_variable_count": template_variable_count,
        "commit_format_adherence": commit_format_adherence,
        "total_commits": total_commits,
        "pr_template_usage": pr_template_usage,
        "total_prs": total_prs,
        "version_references": version_references,
        "format_inconsistencies": format_inconsistencies,
        "template_adherence_score": template_adherence_score,
    }


def _extract_commit_message(command: str) -> str:
    """Extract the commit message from a git commit command string."""
    # Try -m "message" or -m 'message'
    m = re.search(r'-m\s+["\'](.+?)["\']', command)
    if m:
        return m.group(1)
    # Try heredoc content after cat <<
    m = re.search(r"<<\s*['\"]?(?:EOF|HEREDOC|END)['\"]?\s*\n(.+?)(?:\n\s*(?:EOF|HEREDOC|END)\b)", command, re.DOTALL | re.IGNORECASE)
    if m:
        return m.group(1).strip()
    return ""
