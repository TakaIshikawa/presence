"""Implementations for small batch gap reports."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import hashlib
import json
import math
from typing import Any

from ._gap_report_utils import (
    DEFAULT_LIMIT,
    base_report,
    clean,
    column,
    connection,
    display,
    format_json,
    format_text,
    json_shape,
    parse_ts,
    require_non_negative,
    require_positive,
    require_probability,
    schema,
    to_float,
    to_int,
    truthy,
    utc,
    valid_json_object,
)


def missing_report(
    artifact_type: str,
    reasons: tuple[str, ...],
    filters: dict[str, Any],
    missing_tables: list[str],
    missing_columns: dict[str, list[str]] | None = None,
    *,
    now: datetime | str | None = None,
    limit: int = DEFAULT_LIMIT,
) -> dict[str, Any]:
    return base_report(
        artifact_type=artifact_type,
        generated_at=utc(now),
        filters={**filters, "limit": limit},
        rows_scanned=0,
        findings=[],
        reasons=reasons,
        limit=limit,
        missing_tables=missing_tables,
        missing_columns=missing_columns,
    )


def load_rows(conn: Any, table: str, columns: set[str], *, order: str = "rowid") -> list[dict[str, Any]]:
    selected = ", ".join(columns)
    return [dict(row) for row in conn.execute(f"SELECT rowid AS row_id, {selected} FROM {table} ORDER BY {order} ASC").fetchall()]


def simple_script_text(title: str, report: dict[str, Any]) -> str:
    return format_text(title, report)


def mastodon_report(rows: list[dict[str, Any]], *, max_gap_hours: float = 48, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_positive("max_gap_hours", max_gap_hours); require_positive("limit", limit)
    reasons = ("count_drop", "duplicate_fetched_at", "invalid_raw_metrics_json", "stale_gap")
    findings = []
    groups = defaultdict(list)
    for row in rows:
        key = row.get("content_id") or row.get("post_id") or row.get("mastodon_url") or row.get("url")
        groups[key].append(row)
        if not valid_json_object(row.get("raw_metrics")):
            findings.append({"reason": "invalid_raw_metrics_json", "id": row.get("row_id") or row.get("id"), "content_id": row.get("content_id"), "detail": "raw_metrics is not a valid JSON object"})
    for key, items in groups.items():
        items.sort(key=lambda r: (parse_ts(r.get("fetched_at")) or utc("9999-12-31T00:00:00+00:00"), r.get("row_id", 0)))
        seen = set()
        prev = None
        for item in items:
            fetched = clean(item.get("fetched_at"))
            if fetched and fetched in seen:
                findings.append({"reason": "duplicate_fetched_at", "id": item.get("row_id") or item.get("id"), "content_id": item.get("content_id"), "detail": f"duplicate fetched_at for {key}"})
            seen.add(fetched)
            if prev:
                for metric in ("favourites_count", "favorites_count", "reblogs_count", "replies_count", "likes_count", "shares_count"):
                    a, b = to_int(prev.get(metric)), to_int(item.get(metric))
                    if a is not None and b is not None and b < a:
                        findings.append({"reason": "count_drop", "id": item.get("row_id") or item.get("id"), "content_id": item.get("content_id"), "detail": f"{metric} dropped from {a} to {b}"})
                        break
                pa, ca = parse_ts(prev.get("fetched_at")), parse_ts(item.get("fetched_at"))
                if pa and ca and (ca - pa).total_seconds() / 3600 > max_gap_hours:
                    findings.append({"reason": "stale_gap", "id": item.get("row_id") or item.get("id"), "content_id": item.get("content_id"), "detail": f"snapshot gap exceeds {max_gap_hours} hours"})
            prev = item
    return base_report(artifact_type="mastodon_engagement_snapshot_drift", generated_at=utc(now), filters={"max_gap_hours": max_gap_hours, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns, grouped=True)


def mastodon_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); table = "mastodon_engagement"; reasons = ("count_drop", "duplicate_fetched_at", "invalid_raw_metrics_json", "stale_gap")
    required = {"content_id", "fetched_at"}
    if table not in dbs:
        return missing_report("mastodon_engagement_snapshot_drift", reasons, {"max_gap_hours": kwargs.get("max_gap_hours", 48)}, [table], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    cols = dbs[table]; missing = sorted(required - cols)
    if missing:
        return missing_report("mastodon_engagement_snapshot_drift", reasons, {"max_gap_hours": kwargs.get("max_gap_hours", 48)}, [], {table: missing}, now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    select_cols = cols & {"id", "content_id", "post_id", "mastodon_url", "url", "fetched_at", "raw_metrics", "favourites_count", "favorites_count", "reblogs_count", "replies_count", "likes_count", "shares_count"}
    return mastodon_report(load_rows(conn, table, select_cols, order="rowid"), **kwargs)


def linkedin_identity_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_positive("limit", limit)
    reasons = ("missing_content", "missing_platform_identity", "publication_identity_conflict", "duplicate_identity")
    findings = []
    identities = defaultdict(list)
    for row in rows:
        rid = row.get("row_id") or row.get("id")
        identity = row.get("linkedin_url") or row.get("post_id")
        if row.get("resolved_content_id") is None:
            findings.append({"reason": "missing_content", "id": rid, "content_id": row.get("content_id"), "detail": "linkedin_engagement content_id is missing"})
        if not identity:
            findings.append({"reason": "missing_platform_identity", "id": rid, "content_id": row.get("content_id"), "detail": "row lacks linkedin_url and post_id"})
        else:
            identities[identity].append(row)
        pub_url, pub_post = clean(row.get("publication_linkedin_url")), clean(row.get("publication_post_id"))
        if (pub_url and row.get("linkedin_url") and pub_url != clean(row.get("linkedin_url"))) or (pub_post and row.get("post_id") and pub_post != clean(row.get("post_id"))):
            findings.append({"reason": "publication_identity_conflict", "id": rid, "content_id": row.get("content_id"), "detail": "engagement identity conflicts with published LinkedIn publication"})
    for identity, items in identities.items():
        if len(items) > 1:
            for row in items:
                findings.append({"reason": "duplicate_identity", "id": row.get("row_id") or row.get("id"), "content_id": row.get("content_id"), "detail": f"duplicate platform identity {identity}"})
    return base_report(artifact_type="linkedin_engagement_identity_gaps", generated_at=utc(now), filters={"limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def linkedin_identity_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("missing_content", "missing_platform_identity", "publication_identity_conflict", "duplicate_identity")
    if "linkedin_engagement" not in dbs or "generated_content" not in dbs:
        return missing_report("linkedin_engagement_identity_gaps", reasons, {}, [t for t in ("generated_content", "linkedin_engagement") if t not in dbs], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    required = {"linkedin_engagement": {"content_id"}, "generated_content": {"id"}}
    missing = {t: sorted(req - dbs[t]) for t, req in required.items() if req - dbs[t]}
    if missing:
        return missing_report("linkedin_engagement_identity_gaps", reasons, {}, [], missing, now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    le = dbs["linkedin_engagement"]; cp = dbs.get("content_publications", set())
    joins = "LEFT JOIN generated_content gc ON gc.id = le.content_id"
    pub_url = pub_post = "NULL"
    if cp and {"content_id", "platform"}.issubset(cp):
        joins += " LEFT JOIN content_publications cp ON cp.content_id = le.content_id AND lower(cp.platform) = 'linkedin'"
        pub_url = column(cp, "linkedin_url", column(cp, "url", "NULL", alias="cp"), alias="cp")
        pub_post = column(cp, "post_id", "NULL", alias="cp")
    rows = [dict(r) for r in conn.execute(f"""SELECT le.rowid AS row_id, {column(le,'id','le.rowid',alias='le')} AS id, le.content_id, {column(le,'linkedin_url','NULL',alias='le')} AS linkedin_url, {column(le,'post_id','NULL',alias='le')} AS post_id, gc.id AS resolved_content_id, {pub_url} AS publication_linkedin_url, {pub_post} AS publication_post_id FROM linkedin_engagement le {joins} ORDER BY le.rowid""")]
    return linkedin_identity_report(rows, **kwargs)


def feedback_revision_report(feedback_rows: list[dict[str, Any]], *, min_age_hours: float = 24, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_non_negative("min_age_hours", min_age_hours); require_positive("limit", limit)
    generated_at = utc(now); reasons = ("unadopted_revision", "adopted_in_variant", "adopted_in_source_content", "conflicting_replacements")
    findings = []; replacements = defaultdict(set)
    for row in feedback_rows:
        repl = clean(row.get("replacement_text")); 
        if clean(row.get("feedback_type") or row.get("type")).lower() not in {"revise", "prefer"} or not repl:
            continue
        replacements[row.get("content_id")].add(repl)
        in_source = repl in clean(row.get("source_content"))
        in_variant = repl in clean(row.get("variant_content"))
        in_publication = repl in clean(row.get("publication_content"))
        base = {"id": row.get("row_id") or row.get("id"), "content_id": row.get("content_id"), "detail": repl[:120]}
        if in_variant:
            findings.append({**base, "reason": "adopted_in_variant"})
        if in_source:
            findings.append({**base, "reason": "adopted_in_source_content"})
        created = parse_ts(row.get("created_at"))
        if not (in_source or in_variant or in_publication) and (created is None or (generated_at - created).total_seconds() / 3600 >= min_age_hours):
            findings.append({**base, "reason": "unadopted_revision"})
    for cid, values in replacements.items():
        if len(values) > 1:
            findings.append({"reason": "conflicting_replacements", "content_id": cid, "detail": f"{len(values)} replacement_text values"})
    return base_report(artifact_type="content_feedback_revision_adoption", generated_at=generated_at, filters={"min_age_hours": min_age_hours, "limit": limit}, rows_scanned=len(feedback_rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def feedback_revision_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("unadopted_revision", "adopted_in_variant", "adopted_in_source_content", "conflicting_replacements")
    if "content_feedback" not in dbs:
        return missing_report("content_feedback_revision_adoption", reasons, {"min_age_hours": kwargs.get("min_age_hours", 24)}, ["content_feedback"], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    cf = dbs["content_feedback"]; missing = sorted({"content_id", "replacement_text"} - cf)
    if missing:
        return missing_report("content_feedback_revision_adoption", reasons, {"min_age_hours": kwargs.get("min_age_hours", 24)}, [], {"content_feedback": missing}, now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    joins = ""; source = variant = pub = "NULL"
    if "generated_content" in dbs and "id" in dbs["generated_content"]:
        joins += " LEFT JOIN generated_content gc ON gc.id = cf.content_id"; source = column(dbs["generated_content"], "content", "NULL", alias="gc")
    if "content_variants" in dbs and "content_id" in dbs["content_variants"]:
        joins += " LEFT JOIN content_variants cv ON cv.content_id = cf.content_id"; variant = column(dbs["content_variants"], "content", "NULL", alias="cv")
    if "content_publications" in dbs and "content_id" in dbs["content_publications"]:
        joins += " LEFT JOIN content_publications cp ON cp.content_id = cf.content_id"; pub = column(dbs["content_publications"], "content", column(dbs["content_publications"], "published_content", "NULL", alias="cp"), alias="cp")
    rows = [dict(r) for r in conn.execute(f"SELECT cf.rowid AS row_id, {column(cf,'id','cf.rowid',alias='cf')} AS id, cf.content_id, {column(cf,'type',column(cf,'feedback_type','NULL',alias='cf'),alias='cf')} AS feedback_type, cf.replacement_text, {column(cf,'created_at','NULL',alias='cf')} AS created_at, {source} AS source_content, {variant} AS variant_content, {pub} AS publication_content FROM content_feedback cf {joins} ORDER BY cf.rowid")]
    return feedback_revision_report(rows, **kwargs)


def persona_guard_report(rows: list[dict[str, Any]], *, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_positive("limit", limit); reasons = ("failed_guard_published", "unchecked_guard_published", "failed_guard_queued")
    findings = []
    for row in rows:
        status = clean(row.get("guard_status")).lower(); passed = truthy(row.get("passed")); pub_status = clean(row.get("publication_status") or row.get("content_status")).lower()
        selected = truthy(row.get("selected")) or truthy(row.get("is_selected"))
        published = pub_status in {"published", "posted", "success"} or truthy(row.get("published")) or row.get("published_at")
        queued = pub_status in {"queued", "scheduled", "pending"} or selected
        failed = status in {"failed", "fail", "rejected"} or row.get("passed") in (0, "0", False)
        unchecked = not status and row.get("passed") is None
        base = {"id": row.get("row_id") or row.get("id"), "content_id": row.get("content_id"), "score": row.get("score"), "detail": clean(row.get("reasons")) or status}
        if failed and published: findings.append({**base, "reason": "failed_guard_published"})
        if unchecked and published: findings.append({**base, "reason": "unchecked_guard_published"})
        if failed and queued: findings.append({**base, "reason": "failed_guard_queued"})
    return base_report(artifact_type="content_persona_guard_publication_overrides", generated_at=utc(now), filters={"limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def persona_guard_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("failed_guard_published", "unchecked_guard_published", "failed_guard_queued")
    missing_tables = [t for t in ("generated_content", "content_persona_guard") if t not in dbs]
    if missing_tables:
        return missing_report("content_persona_guard_publication_overrides", reasons, {}, missing_tables, now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    gc = dbs["generated_content"]; guard = dbs["content_persona_guard"]
    missing_cols = {}
    if "id" not in gc: missing_cols["generated_content"] = ["id"]
    if "content_id" not in guard: missing_cols["content_persona_guard"] = ["content_id"]
    if missing_cols:
        return missing_report("content_persona_guard_publication_overrides", reasons, {}, [], missing_cols, now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    joins = "LEFT JOIN content_persona_guard cpg ON cpg.content_id = gc.id"; pub_status = selected = "NULL"
    if "content_publications" in dbs and "content_id" in dbs["content_publications"]:
        joins += " LEFT JOIN content_publications cp ON cp.content_id = gc.id"; pub_status = column(dbs["content_publications"], "status", "NULL", alias="cp")
    if "content_variants" in dbs and "content_id" in dbs["content_variants"]:
        joins += " LEFT JOIN content_variants cv ON cv.content_id = gc.id"; selected = column(dbs["content_variants"], "selected", column(dbs["content_variants"], "is_selected", "NULL", alias="cv"), alias="cv")
    rows = [dict(r) for r in conn.execute(f"SELECT gc.id AS content_id, gc.rowid AS row_id, {column(guard,'status','NULL',alias='cpg')} AS guard_status, {column(guard,'passed','NULL',alias='cpg')} AS passed, {column(guard,'score','NULL',alias='cpg')} AS score, {column(guard,'reasons','NULL',alias='cpg')} AS reasons, {column(gc,'published','0',alias='gc')} AS published, {column(gc,'published_at','NULL',alias='gc')} AS published_at, {column(gc,'status','NULL',alias='gc')} AS content_status, {pub_status} AS publication_status, {selected} AS selected FROM generated_content gc {joins} ORDER BY gc.rowid")]
    return persona_guard_report(rows, **kwargs)


def eval_outliers_report(rows: list[dict[str, Any]], *, z_threshold: float = 2.0, min_batch_size: int = 2, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_positive("z_threshold", z_threshold); require_positive("min_batch_size", min_batch_size); require_positive("limit", limit)
    reasons = ("score_outlier_low", "score_outlier_high", "missing_final_score", "threshold_mismatch", "count_anomaly")
    findings = []; by_batch = defaultdict(list)
    for r in rows: by_batch[r.get("batch_id")].append(r)
    for batch_id, items in by_batch.items():
        scores = [to_float(i.get("final_score")) for i in items if to_float(i.get("final_score")) is not None]
        mean = sum(scores) / len(scores) if scores else 0; sd = math.sqrt(sum((s - mean) ** 2 for s in scores) / len(scores)) if len(scores) > 1 else 0
        for item in items:
            score = to_float(item.get("final_score")); base = {"id": item.get("row_id") or item.get("result_id"), "batch_id": batch_id, "content_id": item.get("content_id")}
            if score is None and truthy(item.get("accepted")):
                findings.append({**base, "reason": "missing_final_score", "detail": "accepted output has no final_score"})
            if clean(item.get("result_threshold")) and clean(item.get("batch_threshold")) and clean(item.get("result_threshold")) != clean(item.get("batch_threshold")):
                findings.append({**base, "reason": "threshold_mismatch", "detail": "result threshold differs from batch threshold"})
            if len(scores) >= min_batch_size and sd > 0 and score is not None:
                z = (score - mean) / sd
                if z <= -z_threshold: findings.append({**base, "reason": "score_outlier_low", "detail": f"z={z:.2f}"})
                if z >= z_threshold: findings.append({**base, "reason": "score_outlier_high", "detail": f"z={z:.2f}"})
        cand, prompt = to_int(items[0].get("candidate_count")), to_int(items[0].get("prompt_count"))
        if cand is not None and prompt is not None and (cand <= 0 or prompt <= 0 or cand < prompt):
            findings.append({"reason": "count_anomaly", "id": batch_id, "batch_id": batch_id, "detail": f"candidate_count={cand} prompt_count={prompt}"})
    return base_report(artifact_type="eval_batch_score_outliers", generated_at=utc(now), filters={"z_threshold": z_threshold, "min_batch_size": min_batch_size, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns, grouped=True)


def eval_outliers_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("score_outlier_low", "score_outlier_high", "missing_final_score", "threshold_mismatch", "count_anomaly")
    missing = [t for t in ("eval_batches", "eval_results") if t not in dbs]
    if missing: return missing_report("eval_batch_score_outliers", reasons, {"z_threshold": kwargs.get("z_threshold", 2.0), "min_batch_size": kwargs.get("min_batch_size", 2)}, missing, now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    eb, er = dbs["eval_batches"], dbs["eval_results"]
    rows = [dict(r) for r in conn.execute(f"SELECT er.rowid AS row_id, {column(er,'id','er.rowid',alias='er')} AS result_id, {column(er,'batch_id','NULL',alias='er')} AS batch_id, {column(er,'content_id','NULL',alias='er')} AS content_id, {column(er,'final_score','NULL',alias='er')} AS final_score, {column(er,'accepted','NULL',alias='er')} AS accepted, {column(er,'threshold','NULL',alias='er')} AS result_threshold, {column(eb,'threshold','NULL',alias='eb')} AS batch_threshold, {column(eb,'candidate_count','NULL',alias='eb')} AS candidate_count, {column(eb,'prompt_count','NULL',alias='eb')} AS prompt_count FROM eval_results er LEFT JOIN eval_batches eb ON eb.id = er.batch_id ORDER BY er.rowid")]
    return eval_outliers_report(rows, **kwargs)


def engagement_prediction_report(rows: list[dict[str, Any]], *, max_backfill_age_hours: float = 72, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_non_negative("max_backfill_age_hours", max_backfill_age_hours); require_positive("limit", limit)
    generated_at = utc(now); reasons = ("missing_component", "invalid_component_range", "missing_prompt_identity", "stale_actual_backfill", "prediction_error_mismatch")
    component_cols = ("hook_score", "novelty_score", "clarity_score", "relevance_score", "timing_score")
    findings = []
    for row in rows:
        base = {"id": row.get("row_id") or row.get("id"), "content_id": row.get("content_id")}
        for col in component_cols:
            if col in row:
                val = to_float(row.get(col))
                if val is None: findings.append({**base, "reason": "missing_component", "detail": col})
                elif val < 0 or val > 1: findings.append({**base, "reason": "invalid_component_range", "detail": f"{col}={val}"})
        if not clean(row.get("prompt_version")) or not clean(row.get("prompt_hash")):
            findings.append({**base, "reason": "missing_prompt_identity", "detail": "prompt_version and prompt_hash are required"})
        if row.get("published_at") and row.get("actual_engagement_score") in (None, ""):
            published = parse_ts(row.get("published_at"))
            if published and (generated_at - published).total_seconds() / 3600 > max_backfill_age_hours:
                findings.append({**base, "reason": "stale_actual_backfill", "detail": "published content still lacks actual_engagement_score"})
        pred, actual, err = to_float(row.get("predicted_engagement_score")), to_float(row.get("actual_engagement_score")), to_float(row.get("prediction_error"))
        if pred is not None and actual is not None and err is not None and abs(abs(pred - actual) - abs(err)) > 0.0001:
            findings.append({**base, "reason": "prediction_error_mismatch", "detail": "prediction_error does not match predicted vs actual"})
    return base_report(artifact_type="engagement_prediction_component_completeness", generated_at=generated_at, filters={"max_backfill_age_hours": max_backfill_age_hours, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def engagement_prediction_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("missing_component", "invalid_component_range", "missing_prompt_identity", "stale_actual_backfill", "prediction_error_mismatch")
    if "engagement_predictions" not in dbs:
        return missing_report("engagement_prediction_component_completeness", reasons, {"max_backfill_age_hours": kwargs.get("max_backfill_age_hours", 72)}, ["engagement_predictions"], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    ep = dbs["engagement_predictions"]; joins = ""; published_at = "NULL"
    if "generated_content" in dbs and "content_id" in ep and "id" in dbs["generated_content"]:
        joins += " LEFT JOIN generated_content gc ON gc.id = ep.content_id"; published_at = column(dbs["generated_content"], "published_at", "NULL", alias="gc")
    selected = ["ep.rowid AS row_id"] + [f"ep.{c}" for c in ep if c != "rowid"]
    selected.append(f"{published_at} AS published_at")
    return engagement_prediction_report([dict(r) for r in conn.execute(f"SELECT {', '.join(selected)} FROM engagement_predictions ep {joins} ORDER BY ep.rowid")], **kwargs)


def publication_attempt_shape_report(rows: list[dict[str, Any]], *, max_metadata_bytes: int = 4096, rare_shape_threshold: int = 1, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_positive("max_metadata_bytes", max_metadata_bytes); require_positive("rare_shape_threshold", rare_shape_threshold); require_positive("limit", limit)
    reasons = ("malformed_json", "missing_success_response_key", "oversized_metadata", "rare_shape")
    findings = []; shape_counts = Counter()
    row_shapes = []
    for row in rows:
        raw = row.get("response_metadata"); shape = json_shape(raw); key = (row.get("platform"), row.get("success"), row.get("error_category"), shape)
        base = {"id": row.get("row_id") or row.get("id"), "content_id": row.get("content_id"), "detail": display(row.get("platform"))}
        if raw not in (None, "") and len(str(raw).encode()) > max_metadata_bytes:
            findings.append({**base, "reason": "oversized_metadata"})
        if shape is None:
            findings.append({**base, "reason": "malformed_json"})
        else:
            shape_counts[key] += 1; row_shapes.append((row, key, shape))
            if truthy(row.get("success")) and not ({"id", "url", "post_id", "response_id"} & set(shape)):
                findings.append({**base, "reason": "missing_success_response_key", "detail": "successful attempt lacks response id/url key"})
    for row, key, shape in row_shapes:
        if shape_counts[key] <= rare_shape_threshold:
            findings.append({"reason": "rare_shape", "id": row.get("row_id") or row.get("id"), "content_id": row.get("content_id"), "detail": ",".join(shape) or "<empty>"})
    summaries = [{"platform": k[0], "success": k[1], "error_category": k[2], "shape": list(k[3] or ()), "count": v} for k, v in sorted(shape_counts.items(), key=lambda kv: str(kv[0]))]
    return base_report(artifact_type="publication_attempt_response_shape_catalog", generated_at=utc(now), filters={"max_metadata_bytes": max_metadata_bytes, "rare_shape_threshold": rare_shape_threshold, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns, grouped=True, extra={"shape_summaries": summaries})


def publication_attempt_shape_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("malformed_json", "missing_success_response_key", "oversized_metadata", "rare_shape")
    if "publication_attempts" not in dbs:
        return missing_report("publication_attempt_response_shape_catalog", reasons, {"max_metadata_bytes": kwargs.get("max_metadata_bytes", 4096), "rare_shape_threshold": kwargs.get("rare_shape_threshold", 1)}, ["publication_attempts"], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    cols = dbs["publication_attempts"]; selected = ["rowid AS row_id"] + [c for c in cols if c in {"id", "content_id", "platform", "success", "error_category", "response_metadata"}]
    return publication_attempt_shape_report([dict(r) for r in conn.execute(f"SELECT {', '.join(selected)} FROM publication_attempts ORDER BY rowid")], **kwargs)


def newsletter_rank_report(rows: list[dict[str, Any]], *, score_gap_threshold: float = 0.05, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_non_negative("score_gap_threshold", score_gap_threshold); require_positive("limit", limit)
    reasons = ("duplicate_rank", "missing_selected_candidate", "selected_low_rank", "send_subject_mismatch")
    findings = []; groups = defaultdict(list)
    for row in rows: groups[(row.get("issue_id"), row.get("newsletter_send_id"))].append(row)
    for group_key, items in groups.items():
        rank_counts = Counter(to_int(i.get("rank")) for i in items if to_int(i.get("rank")) is not None)
        for rank, count in rank_counts.items():
            if count > 1: findings.append({"reason": "duplicate_rank", "id": f"{group_key}:{rank}", "detail": f"rank {rank} appears {count} times"})
        selected = [i for i in items if truthy(i.get("selected"))]
        if not selected:
            findings.append({"reason": "missing_selected_candidate", "id": str(group_key), "detail": "no selected candidate"})
            continue
        top_score = max((to_float(i.get("score")) for i in items if to_float(i.get("score")) is not None), default=None)
        for item in selected:
            rank = to_int(item.get("rank")); score = to_float(item.get("score"))
            if rank and rank > 1 and top_score is not None and score is not None and top_score - score > score_gap_threshold:
                findings.append({"reason": "selected_low_rank", "id": item.get("row_id") or item.get("id"), "detail": f"rank={rank} score_gap={top_score-score:.4f}"})
            if clean(item.get("send_subject")) and clean(item.get("subject")) and clean(item.get("send_subject")) != clean(item.get("subject")):
                findings.append({"reason": "send_subject_mismatch", "id": item.get("row_id") or item.get("id"), "detail": "selected subject differs from newsletter_sends.subject"})
    return base_report(artifact_type="newsletter_subject_candidate_rank_gaps", generated_at=utc(now), filters={"score_gap_threshold": score_gap_threshold, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def newsletter_rank_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("duplicate_rank", "missing_selected_candidate", "selected_low_rank", "send_subject_mismatch")
    if "newsletter_subject_candidates" not in dbs:
        return missing_report("newsletter_subject_candidate_rank_gaps", reasons, {"score_gap_threshold": kwargs.get("score_gap_threshold", 0.05)}, ["newsletter_subject_candidates"], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    c = dbs["newsletter_subject_candidates"]; joins = ""; send_subject = "NULL"
    if "newsletter_sends" in dbs and "newsletter_send_id" in c and "id" in dbs["newsletter_sends"]:
        joins += " LEFT JOIN newsletter_sends ns ON ns.id = nsc.newsletter_send_id"; send_subject = column(dbs["newsletter_sends"], "subject", "NULL", alias="ns")
    rows = [dict(r) for r in conn.execute(f"SELECT nsc.rowid AS row_id, {column(c,'id','nsc.rowid',alias='nsc')} AS id, {column(c,'issue_id','NULL',alias='nsc')} AS issue_id, {column(c,'newsletter_send_id','NULL',alias='nsc')} AS newsletter_send_id, {column(c,'rank','NULL',alias='nsc')} AS rank, {column(c,'score','NULL',alias='nsc')} AS score, {column(c,'selected','NULL',alias='nsc')} AS selected, {column(c,'subject','NULL',alias='nsc')} AS subject, {send_subject} AS send_subject FROM newsletter_subject_candidates nsc {joins} ORDER BY nsc.rowid")]
    return newsletter_rank_report(rows, **kwargs)


def reply_cursor_report(rows: list[dict[str, Any]], *, max_age_hours: float = 48, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_non_negative("max_age_hours", max_age_hours); require_positive("limit", limit)
    generated_at = utc(now); reasons = ("missing_cursor", "stale_platform_cursor", "legacy_cursor_divergence", "missing_platform_state")
    findings = []; updated = Counter()
    for row in rows:
        updated[clean(row.get("platform_updated_at"))] += 1
    for row in rows:
        base = {"id": row.get("row_id") or row.get("id"), "detail": clean(row.get("platform") or "x")}
        if not clean(row.get("cursor")):
            findings.append({**base, "reason": "missing_cursor"})
        ts = parse_ts(row.get("platform_updated_at"))
        if ts and (generated_at - ts).total_seconds() / 3600 > max_age_hours:
            findings.append({**base, "reason": "stale_platform_cursor"})
        if clean(row.get("platform_updated_at")) and updated[clean(row.get("platform_updated_at"))] > 1:
            findings.append({**base, "reason": "stale_platform_cursor", "detail": "duplicate updated_at"})
        if clean(row.get("legacy_last_mention_id")) and clean(row.get("x_cursor")) and clean(row.get("legacy_last_mention_id")) != clean(row.get("x_cursor")):
            findings.append({**base, "reason": "legacy_cursor_divergence"})
        if row.get("missing_platform_state"):
            findings.append({**base, "reason": "missing_platform_state"})
    return base_report(artifact_type="reply_platform_cursor_integrity", generated_at=generated_at, filters={"max_age_hours": max_age_hours, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def reply_cursor_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("missing_cursor", "stale_platform_cursor", "legacy_cursor_divergence", "missing_platform_state")
    if "reply_state" not in dbs:
        return missing_report("reply_platform_cursor_integrity", reasons, {"max_age_hours": kwargs.get("max_age_hours", 48)}, ["reply_state"], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    rows = []
    if "platform_reply_state" in dbs:
        prs = dbs["platform_reply_state"]
        platform_expr = column(prs, "platform", "'x'")
        cursor_expr = column(prs, "cursor", column(prs, "last_cursor", "NULL"))
        updated_expr = column(prs, "updated_at", "NULL")
        rows.extend(
            dict(r)
            for r in conn.execute(
                f"SELECT rowid AS row_id, {platform_expr} AS platform, {cursor_expr} AS cursor, "
                f"{updated_expr} AS platform_updated_at, NULL AS legacy_last_mention_id, NULL AS x_cursor, "
                "0 AS missing_platform_state FROM platform_reply_state ORDER BY rowid"
            )
        )
    rs = dbs["reply_state"]
    legacy = [dict(r) for r in conn.execute(f"SELECT rowid AS row_id, {column(rs,'last_mention_id','NULL')} AS legacy_last_mention_id, {column(rs,'x_cursor','NULL')} AS x_cursor FROM reply_state ORDER BY rowid")]
    if not rows:
        rows.extend({**r, "missing_platform_state": 1} for r in legacy)
    else:
        for r in legacy:
            rows.append({**r, "cursor": r.get("x_cursor") or r.get("legacy_last_mention_id"), "platform": "legacy", "platform_updated_at": None, "missing_platform_state": 0})
    return reply_cursor_report(rows, **kwargs)


def proactive_followup_report(rows: list[dict[str, Any]], *, window_hours: float = 24, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_positive("window_hours", window_hours); require_positive("limit", limit)
    reasons = ("missing_followup_reminder", "orphan_followup_reminder", "premature_followup_due", "duplicate_target_window")
    findings = []; actions = {}; reminders = []
    for row in rows:
        if row.get("row_type") == "action": actions[row.get("action_id")] = row
        else: reminders.append(row)
    reminded = {r.get("action_id") for r in reminders}
    for aid, action in actions.items():
        status = clean(action.get("status")).lower()
        if status in {"posted", "approved"} and aid not in reminded:
            findings.append({"reason": "missing_followup_reminder", "id": aid, "detail": clean(action.get("target_handle"))})
    for rem in reminders:
        action = actions.get(rem.get("action_id"))
        if not action:
            findings.append({"reason": "orphan_followup_reminder", "id": rem.get("reminder_id"), "detail": "reminder points at missing action"})
        elif parse_ts(rem.get("due_at")) and parse_ts(action.get("approved_at") or action.get("posted_at")) and parse_ts(rem.get("due_at")) < parse_ts(action.get("approved_at") or action.get("posted_at")):
            findings.append({"reason": "premature_followup_due", "id": rem.get("reminder_id"), "detail": "reminder due before approval/posting"})
    by_handle = defaultdict(list)
    for action in actions.values():
        if clean(action.get("target_handle")): by_handle[clean(action.get("target_handle")).lower()].append(action)
    for handle, items in by_handle.items():
        items.sort(key=lambda r: parse_ts(r.get("posted_at") or r.get("approved_at") or r.get("created_at")) or utc("9999-01-01T00:00:00+00:00"))
        for prev, cur in zip(items, items[1:]):
            a = parse_ts(prev.get("posted_at") or prev.get("approved_at") or prev.get("created_at")); b = parse_ts(cur.get("posted_at") or cur.get("approved_at") or cur.get("created_at"))
            if a and b and (b - a).total_seconds() / 3600 <= window_hours:
                findings.append({"reason": "duplicate_target_window", "id": cur.get("action_id"), "detail": handle})
    return base_report(artifact_type="proactive_action_followup_readiness", generated_at=utc(now), filters={"window_hours": window_hours, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def proactive_followup_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("missing_followup_reminder", "orphan_followup_reminder", "premature_followup_due", "duplicate_target_window")
    missing = [t for t in ("proactive_actions", "reply_followup_reminders") if t not in dbs]
    if missing: return missing_report("proactive_action_followup_readiness", reasons, {"window_hours": kwargs.get("window_hours", 24)}, missing, now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    pa, rr = dbs["proactive_actions"], dbs["reply_followup_reminders"]
    actions = [dict(r) | {"row_type": "action"} for r in conn.execute(f"SELECT {column(pa,'id','rowid')} AS action_id, {column(pa,'status','NULL')} AS status, {column(pa,'target_handle','NULL')} AS target_handle, {column(pa,'approved_at','NULL')} AS approved_at, {column(pa,'posted_at','NULL')} AS posted_at, {column(pa,'created_at','NULL')} AS created_at FROM proactive_actions ORDER BY rowid")]
    action_id_col = "action_id" if "action_id" in rr else "proactive_action_id" if "proactive_action_id" in rr else "NULL"
    reminders = [dict(r) | {"row_type": "reminder"} for r in conn.execute(f"SELECT {column(rr,'id','rowid')} AS reminder_id, {action_id_col} AS action_id, {column(rr,'due_at','NULL')} AS due_at FROM reply_followup_reminders ORDER BY rowid")]
    return proactive_followup_report(actions + reminders, **kwargs)


def profile_following_report(rows: list[dict[str, Any]], *, following_delta_threshold: int = 100, ratio_threshold: float = 0.5, limit: int = DEFAULT_LIMIT, now=None, missing_tables=None, missing_columns=None) -> dict[str, Any]:
    require_non_negative("following_delta_threshold", following_delta_threshold); require_non_negative("ratio_threshold", ratio_threshold); require_positive("limit", limit)
    reasons = ("following_spike", "following_drop", "ratio_collapse", "duplicate_fetched_at", "tweet_count_decrease")
    findings = []; groups = defaultdict(list)
    for row in rows: groups[row.get("platform") or "default"].append(row)
    for platform, items in groups.items():
        items.sort(key=lambda r: (parse_ts(r.get("fetched_at")) or utc("9999-12-31T00:00:00+00:00"), r.get("row_id", 0)))
        seen = set(); prev = None
        for item in items:
            base = {"id": item.get("row_id") or item.get("id"), "detail": platform}
            if clean(item.get("fetched_at")) in seen: findings.append({**base, "reason": "duplicate_fetched_at"})
            seen.add(clean(item.get("fetched_at")))
            if prev:
                pf, cf = to_int(prev.get("following_count")), to_int(item.get("following_count"))
                if pf is not None and cf is not None:
                    delta = cf - pf
                    if delta >= following_delta_threshold: findings.append({**base, "reason": "following_spike", "detail": f"{platform} delta={delta}"})
                    if delta <= -following_delta_threshold: findings.append({**base, "reason": "following_drop", "detail": f"{platform} delta={delta}"})
                p_ratio = (to_int(prev.get("follower_count")) or 0) / max(to_int(prev.get("following_count")) or 0, 1)
                c_ratio = (to_int(item.get("follower_count")) or 0) / max(to_int(item.get("following_count")) or 0, 1)
                if p_ratio and c_ratio / p_ratio < ratio_threshold: findings.append({**base, "reason": "ratio_collapse", "detail": f"{platform} ratio={c_ratio:.3f}"})
                pt, ct = to_int(prev.get("tweet_count")), to_int(item.get("tweet_count"))
                if pt is not None and ct is not None and ct < pt: findings.append({**base, "reason": "tweet_count_decrease"})
            prev = item
    return base_report(artifact_type="profile_metric_following_spikes", generated_at=utc(now), filters={"following_delta_threshold": following_delta_threshold, "ratio_threshold": ratio_threshold, "limit": limit}, rows_scanned=len(rows), findings=findings, reasons=reasons, limit=limit, missing_tables=missing_tables, missing_columns=missing_columns)


def profile_following_from_db(db_or_conn: Any, **kwargs: Any) -> dict[str, Any]:
    conn = connection(db_or_conn); dbs = schema(conn); reasons = ("following_spike", "following_drop", "ratio_collapse", "duplicate_fetched_at", "tweet_count_decrease")
    if "profile_metrics" not in dbs:
        return missing_report("profile_metric_following_spikes", reasons, {"following_delta_threshold": kwargs.get("following_delta_threshold", 100), "ratio_threshold": kwargs.get("ratio_threshold", 0.5)}, ["profile_metrics"], now=kwargs.get("now"), limit=kwargs.get("limit", DEFAULT_LIMIT))
    cols = dbs["profile_metrics"]; selected = {"id", "platform", "fetched_at", "following_count", "follower_count", "tweet_count"} & cols
    return profile_following_report(load_rows(conn, "profile_metrics", selected, order="rowid"), **kwargs)
