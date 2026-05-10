"""Pack file read efficiency and cache utilization analyzer.

Analyzes Read tool usage patterns across Claude Code execution packs
to evaluate how efficiently sessions read files — focusing on targeted
reads (offset/limit) versus full-file reads and re-read waste.

Efficiency metrics:
- targeted_read_rate: % of Read calls using offset/limit parameters
- avg_lines_per_read: Average lines requested per read (lower is better)
- reread_count: Number of times the same file is read more than once
- efficiency_score: Weighted composite (0-1)
- recommendations: List of actionable improvement suggestions

Scoring weights:
- targeted_read_rate: 0.4 (higher is better)
- avg_lines_per_read penalty: 0.3 (penalty if avg > 200)
- re-read waste: 0.3 (fewer re-reads is better)
"""

from __future__ import annotations

from collections import Counter
from typing import Any, Mapping


class PackFileReadEfficiencyAnalyzer:
    """Analyzes file read efficiency across pack sessions.

    Parses Read tool calls, extracts offset/limit parameters, and computes
    efficiency metrics measuring targeted vs wasteful read patterns.
    """

    # Scoring weights
    WEIGHT_TARGETED = 0.4
    WEIGHT_AVG_LINES = 0.3
    WEIGHT_REREAD = 0.3

    # Thresholds
    AVG_LINES_IDEAL = 100
    AVG_LINES_WORST = 400
    REREAD_THRESHOLD = 5  # Up to this many re-reads is acceptable

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze Read tool efficiency across pack records.

        Args:
            records: List of pack dictionaries with keys:
                - pack_id: Pack identifier
                - sessions: List of session dictionaries with:
                    - session_id: Session identifier
                    - messages: List of assistant message dictionaries with:
                        - tool_calls: List of tool call dictionaries with:
                            - tool_name: Name of tool (Read, Edit, etc.)
                            - file_path: Path of file being read
                            - offset: Optional line offset
                            - limit: Optional line limit

        Returns:
            Dict with targeted_read_rate, avg_lines_per_read, reread_count,
            efficiency_score, and recommendations.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of pack dictionaries")

        total_reads = 0
        targeted_reads = 0
        lines_per_read: list[int] = []
        file_read_counts: Counter[str] = Counter()

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

                for message in messages:
                    if not isinstance(message, Mapping):
                        continue

                    tool_calls = message.get("tool_calls")
                    if not isinstance(tool_calls, list):
                        continue

                    for tc in tool_calls:
                        if not isinstance(tc, Mapping):
                            continue
                        if tc.get("tool_name") != "Read":
                            continue

                        total_reads += 1
                        file_path = tc.get("file_path", "")
                        offset = tc.get("offset")
                        limit = tc.get("limit")

                        # Track file re-reads
                        if file_path:
                            file_read_counts[file_path] += 1

                        # Determine if targeted
                        has_offset = offset is not None
                        has_limit = limit is not None

                        if has_offset or has_limit:
                            targeted_reads += 1

                        # Track lines per read
                        if has_limit and isinstance(limit, (int, float)):
                            lines_per_read.append(int(limit))
                        else:
                            # Full file read — use default of 2000
                            lines_per_read.append(2000)

        # Handle no-read sessions
        if total_reads == 0:
            return {
                "targeted_read_rate": 0.0,
                "avg_lines_per_read": 0.0,
                "reread_count": 0,
                "efficiency_score": 1.0,  # Neutral — no reads means no waste
                "recommendations": [],
            }

        # Calculate metrics
        targeted_read_rate = targeted_reads / total_reads
        avg_lines = sum(lines_per_read) / len(lines_per_read)

        # Re-read count: total reads minus unique files
        unique_files = len(file_read_counts)
        reread_count = total_reads - unique_files

        # Compute component scores (0-1 each)
        targeted_score = targeted_read_rate  # Already 0-1

        # Avg lines penalty: 1.0 if <= IDEAL, 0.0 if >= WORST, linear between
        if avg_lines <= self.AVG_LINES_IDEAL:
            lines_score = 1.0
        elif avg_lines >= self.AVG_LINES_WORST:
            lines_score = 0.0
        else:
            lines_score = 1.0 - (avg_lines - self.AVG_LINES_IDEAL) / (
                self.AVG_LINES_WORST - self.AVG_LINES_IDEAL
            )

        # Re-read waste: score drops as re-reads increase relative to total
        reread_ratio = reread_count / total_reads
        reread_score = max(0.0, 1.0 - reread_ratio)

        # Weighted composite
        efficiency_score = (
            self.WEIGHT_TARGETED * targeted_score
            + self.WEIGHT_AVG_LINES * lines_score
            + self.WEIGHT_REREAD * reread_score
        )

        # Generate recommendations
        recommendations = self._generate_recommendations(
            targeted_read_rate, avg_lines, reread_count, total_reads
        )

        return {
            "targeted_read_rate": round(targeted_read_rate, 4),
            "avg_lines_per_read": round(avg_lines, 2),
            "reread_count": reread_count,
            "efficiency_score": round(efficiency_score, 4),
            "recommendations": recommendations,
        }

    def _generate_recommendations(
        self,
        targeted_rate: float,
        avg_lines: float,
        reread_count: int,
        total_reads: int,
    ) -> list[str]:
        """Generate actionable recommendations based on metrics."""
        recommendations: list[str] = []

        if targeted_rate < 0.5:
            recommendations.append(
                "Use offset/limit parameters for targeted reads instead of reading entire files."
            )

        if avg_lines > 200:
            recommendations.append(
                "Reduce average lines per read by using smaller limit values "
                "to read only relevant sections."
            )

        if total_reads > 0 and reread_count / total_reads > 0.3:
            recommendations.append(
                "Cache file contents or use /cache snapshot after initial reads "
                "to avoid redundant re-reads."
            )

        if targeted_rate < 0.85:
            recommendations.append(
                "After edits, use targeted reads (last 30-50 lines) for verification "
                "instead of re-reading the full file."
            )

        return recommendations
