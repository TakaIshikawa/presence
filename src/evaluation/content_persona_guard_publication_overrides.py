"""Report persona guard publication overrides."""

from __future__ import annotations

from ._batch_gap_reports import persona_guard_from_db as build_content_persona_guard_publication_overrides_report_from_db
from ._batch_gap_reports import persona_guard_report as build_content_persona_guard_publication_overrides_report
from ._gap_report_utils import DEFAULT_LIMIT, format_json, format_text


def format_content_persona_guard_publication_overrides_json(report):
    return format_json(report)


def format_content_persona_guard_publication_overrides_text(report):
    return format_text("Content Persona Guard Publication Overrides", report)
