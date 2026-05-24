#!/usr/bin/env python3
"""Report eval batch score outliers."""

from __future__ import annotations

from _batch_gap_report_cli import non_negative_float, positive_int, run
from evaluation.eval_batch_score_outliers import DEFAULT_LIMIT, DEFAULT_MIN_BATCH_SIZE, DEFAULT_Z_THRESHOLD, build_eval_batch_score_outliers_report_from_db, format_eval_batch_score_outliers_json, format_eval_batch_score_outliers_text


def main(argv=None) -> int:
    return run(argv, description=__doc__ or "", builder=build_eval_batch_score_outliers_report_from_db, json_formatter=format_eval_batch_score_outliers_json, text_formatter=format_eval_batch_score_outliers_text, options=[("z_threshold", non_negative_float, DEFAULT_Z_THRESHOLD), ("min_batch_size", positive_int, DEFAULT_MIN_BATCH_SIZE), ("limit", positive_int, DEFAULT_LIMIT)])


if __name__ == "__main__":
    raise SystemExit(main())
