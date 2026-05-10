"""Session cache utilization and snapshot discipline analyzer.

Measures how well sessions use the /cache system for avoiding redundant
file reads. Respects optimization mode: baseline sessions get neutral scores.

Metrics:
- cache_query_rate: % of re-reads preceded by cache query
- snapshot_rate: % of full reads followed by cache snapshot
- missed_opportunities: Count of re-reads without cache involvement
- utilization_score: Composite score (0-1)
- mode: Detected optimization mode
"""

from __future__ import annotations

from typing import Any, Mapping


class SessionCacheUtilizationAnalyzer:
    """Analyzes cache utilization patterns within individual sessions."""

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze cache utilization in session records.

        Args:
            records: List of session dictionaries with:
                - session_id: Session identifier
                - optimization_mode: "baseline" or "optimized"
                - messages: List of message dictionaries with tool_calls

        Returns:
            Dict with cache_query_rate, snapshot_rate, missed_opportunities,
            utilization_score, and mode.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of session dictionaries")

        total_rereads = 0
        rereads_with_cache_query = 0
        total_full_reads = 0
        full_reads_with_snapshot = 0
        missed_opportunities = 0
        mode = "optimized"

        for record in records:
            if not isinstance(record, Mapping):
                continue

            # Check optimization mode
            record_mode = record.get("optimization_mode", "optimized")
            if record_mode == "baseline":
                mode = "baseline"

            messages = record.get("messages")
            if not isinstance(messages, list):
                continue

            files_read: set[str] = set()
            files_cached: set[str] = set()
            cache_queried: set[str] = set()

            for message in messages:
                if not isinstance(message, Mapping):
                    continue

                tool_calls = message.get("tool_calls")
                if not isinstance(tool_calls, list):
                    continue

                for tc in tool_calls:
                    if not isinstance(tc, Mapping):
                        continue

                    tool_name = tc.get("tool_name", "")
                    file_path = tc.get("file_path", "")

                    # Track cache commands (Skill tool with cache arg)
                    if tool_name == "Skill":
                        skill_name = tc.get("skill", "")
                        args = tc.get("args", "")
                        if skill_name == "cache":
                            if "query" in str(args):
                                # Extract file from args
                                cache_queried.add(str(args))
                                # Mark broadly that cache was queried
                                if file_path:
                                    cache_queried.add(file_path)
                            elif "snapshot" in str(args):
                                if file_path:
                                    files_cached.add(file_path)

                    # Track Bash cache commands
                    if tool_name == "Bash":
                        command = tc.get("command", "")
                        if isinstance(command, str):
                            if "/cache query" in command or "cache query" in command:
                                # Mark cache query
                                parts = command.split()
                                for part in parts:
                                    if "/" in part and "cache" not in part:
                                        cache_queried.add(part)
                            if "/cache snapshot" in command or "cache snapshot" in command:
                                parts = command.split()
                                for part in parts:
                                    if "/" in part and "cache" not in part:
                                        files_cached.add(part)

                    # Track Read calls
                    if tool_name == "Read" and file_path:
                        is_full_read = (
                            tc.get("offset") is None and tc.get("limit") is None
                        )

                        if file_path in files_read:
                            # This is a re-read
                            total_rereads += 1
                            if file_path in cache_queried or file_path in files_cached:
                                rereads_with_cache_query += 1
                            else:
                                missed_opportunities += 1
                        else:
                            files_read.add(file_path)

                        if is_full_read:
                            total_full_reads += 1

                # Check if full reads were followed by snapshots (simplified)
                # We check at end of session
            # After processing all messages, check snapshot coverage
            full_reads_with_snapshot += len(files_read & files_cached)

        # Baseline mode → neutral score
        if mode == "baseline":
            return {
                "cache_query_rate": 0.0,
                "snapshot_rate": 0.0,
                "missed_opportunities": 0,
                "utilization_score": 1.0,  # Neutral for baseline
                "mode": "baseline",
            }

        # Calculate metrics
        cache_query_rate = (
            rereads_with_cache_query / total_rereads
            if total_rereads > 0
            else 1.0  # No re-reads means no cache needed
        )

        snapshot_rate = (
            full_reads_with_snapshot / total_full_reads
            if total_full_reads > 0
            else 1.0
        )

        # Score: weighted combination
        if total_rereads == 0:
            # No re-reads → perfect (no cache needed)
            utilization_score = 1.0
        else:
            utilization_score = 0.6 * cache_query_rate + 0.4 * snapshot_rate

        return {
            "cache_query_rate": round(cache_query_rate, 4),
            "snapshot_rate": round(snapshot_rate, 4),
            "missed_opportunities": missed_opportunities,
            "utilization_score": round(utilization_score, 4),
            "mode": mode,
        }
