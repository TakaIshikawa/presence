"""Pack context window pressure and summarization trigger analyzer.

Measures context window usage patterns and whether sessions trigger
automatic summarization due to excessive context consumption.

Metrics:
- avg_message_count: Average messages per session
- large_read_ratio: % of reads over 500 lines
- estimated_pressure: Heuristic estimate of context pressure (0-1)
- summarization_triggers: Count of likely summarization events
- pressure_score: Overall context efficiency (higher is better, 0-1)
"""

from __future__ import annotations

from typing import Any, Mapping


class PackContextWindowPressureAnalyzer:
    """Analyzes context window pressure patterns across pack sessions."""

    # Thresholds
    HIGH_MESSAGE_COUNT = 50
    LARGE_READ_THRESHOLD = 500  # lines

    def analyze(self, records: object) -> dict[str, Any]:
        """Analyze context window pressure across pack records.

        Args:
            records: List of pack dictionaries with sessions/messages/tool_calls.

        Returns:
            Dict with avg_message_count, large_read_ratio, estimated_pressure,
            summarization_triggers, and pressure_score.

        Raises:
            ValueError: If records is not a list.
        """
        if records is None:
            records = []
        if not isinstance(records, list):
            raise ValueError("records must be a list of pack dictionaries")

        total_sessions = 0
        total_messages = 0
        total_reads = 0
        large_reads = 0
        summarization_triggers = 0

        for record in records:
            if not isinstance(record, Mapping):
                continue

            sessions = record.get("sessions")
            if not isinstance(sessions, list):
                continue

            for session in sessions:
                if not isinstance(session, Mapping):
                    continue

                total_sessions += 1
                messages = session.get("messages")
                if not isinstance(messages, list):
                    continue

                session_message_count = len(messages)
                total_messages += session_message_count

                # Detect likely summarization (high message count)
                if session_message_count > self.HIGH_MESSAGE_COUNT:
                    summarization_triggers += 1

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
                        limit = tc.get("limit")
                        if limit is None:
                            # Full file read → large
                            large_reads += 1
                        elif isinstance(limit, (int, float)) and limit > self.LARGE_READ_THRESHOLD:
                            large_reads += 1

        if total_sessions == 0:
            return {
                "avg_message_count": 0.0,
                "large_read_ratio": 0.0,
                "estimated_pressure": 0.0,
                "summarization_triggers": 0,
                "pressure_score": 1.0,
            }

        avg_message_count = total_messages / total_sessions
        large_read_ratio = large_reads / total_reads if total_reads > 0 else 0.0

        # Estimate pressure (higher = more pressure = worse)
        message_pressure = min(avg_message_count / self.HIGH_MESSAGE_COUNT, 1.0)
        read_pressure = large_read_ratio
        estimated_pressure = 0.5 * message_pressure + 0.5 * read_pressure

        # Score: inverse of pressure (higher is better)
        pressure_score = max(0.0, 1.0 - estimated_pressure)

        return {
            "avg_message_count": round(avg_message_count, 2),
            "large_read_ratio": round(large_read_ratio, 4),
            "estimated_pressure": round(estimated_pressure, 4),
            "summarization_triggers": summarization_triggers,
            "pressure_score": round(pressure_score, 4),
        }
