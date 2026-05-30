#!/usr/bin/env python3
"""Report persona guard publication overrides."""

from __future__ import annotations

from _batch_gap_report_cli import positive_int, run
from evaluation.content_persona_guard_publication_overrides import DEFAULT_LIMIT, build_content_persona_guard_publication_overrides_report_from_db, format_content_persona_guard_publication_overrides_json, format_content_persona_guard_publication_overrides_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_content_persona_guard_publication_overrides_report_from_db, json_formatter=format_content_persona_guard_publication_overrides_json, text_formatter=format_content_persona_guard_publication_overrides_text, options=[("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
