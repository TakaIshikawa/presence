"""Flag broken image references in generated blog draft content."""
from __future__ import annotations
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from ._batch_report_common import *

ARTIFACT_TYPE = "blog_draft_broken_image_references"
DEFAULT_LIMIT = 50
MD_IMAGE_RE = re.compile(r"!\[[^\]]*]\(([^)]*)\)")
HTML_IMAGE_RE = re.compile(r"<img\b[^>]*>", re.I)
SRC_RE = re.compile(r"\bsrc\s*=\s*(?:\"([^\"]*)\"|'([^']*)'|([^\s>]+))", re.I)
REMOTE_SCHEMES = {"http", "https", "data"}

def _image_refs(body: str) -> list[str]:
    refs = [m.group(1).strip().strip("\"'") for m in MD_IMAGE_RE.finditer(body or "")]
    for tag in HTML_IMAGE_RE.findall(body or ""):
        m = SRC_RE.search(tag)
        refs.append((m.group(1) or m.group(2) or m.group(3) or "").strip() if m else "")
    return refs

def _issue(ref: str, base_dir: Path | None) -> str | None:
    if not ref:
        return "empty_reference"
    parsed = urlparse(ref)
    if parsed.scheme:
        return None if parsed.scheme.lower() in REMOTE_SCHEMES else "unsupported_scheme"
    if ref.startswith("//"):
        return None
    if not base_dir:
        return "missing_path_context"
    path = Path(ref)
    if not path.is_absolute():
        path = base_dir / path
    return None if path.exists() else "missing_local_file"

def build_blog_draft_broken_image_references_report(rows: list[dict[str, Any]], *, base_dir: str | Path | None = None, limit: int = DEFAULT_LIMIT, missing_tables=None, missing_columns=None, now=None):
    positive("limit", limit); gen = now_value(now); root = Path(base_dir) if base_dir else None; findings = []
    for row in rows:
        body = clean(row.get("body") or row.get("content") or row.get("markdown"))
        row_base = Path(clean(row.get("path_context") or row.get("file_path") or row.get("source_path"))).parent if clean(row.get("path_context") or row.get("file_path") or row.get("source_path")) else root
        for ref in _image_refs(body):
            issue = _issue(ref, row_base)
            if issue:
                findings.append({"content_id": row.get("content_id") or row.get("id"), "image_reference": ref, "issue_type": issue, "content_type": row.get("content_type"), "created_at": row.get("created_at")})
    findings.sort(key=lambda f: (str(f["content_id"]), f["issue_type"], f["image_reference"]))
    shown = findings[:limit]
    warning = "generated_content schema is missing or incomplete" if missing_tables or missing_columns else None
    return {"artifact_type": ARTIFACT_TYPE, "generated_at": gen.isoformat(), "filters": {"limit": limit, "base_dir": str(root) if root else None}, "summary": {"content_count": len(rows), "broken_image_reference_count": len(findings), "shown": len(shown)}, "broken_image_references": shown, "missing_tables": sorted(missing_tables or []), "missing_columns": {k: sorted(v) for k, v in sorted((missing_columns or {}).items())}, "warning": warning, "empty_state": empty_state(findings, "No broken blog draft image references found.", schema_gap=bool(warning))}

def build_blog_draft_broken_image_references_report_from_db(db_or_conn: Any, **kw):
    conn = connection(db_or_conn); s = schema(conn); mt = []; mc = {}; rows = []
    if "generated_content" not in s:
        mt.append("generated_content")
    else:
        cols = s["generated_content"]
        missing = [c for c in ("id",) if c not in cols]
        if not ({"content", "body", "markdown"} & cols):
            missing.append("content|body|markdown")
        if missing:
            mc["generated_content"] = missing
        else:
            rows = load_table(conn, "generated_content", cols, {"content_id": ("id",), "content_type": ("content_type", "type"), "created_at": ("created_at",), "body": ("content", "body", "markdown"), "file_path": ("file_path", "source_path", "path")})
    return build_blog_draft_broken_image_references_report(rows, missing_tables=mt, missing_columns=mc, **kw)

def format_blog_draft_broken_image_references_json(r): return json_dumps(r)
def format_blog_draft_broken_image_references_text(r):
    s = r["summary"]; lines = ["Blog Draft Broken Image References", f"Generated: {r['generated_at']}", f"Totals: content={s['content_count']} broken_images={s['broken_image_reference_count']} shown={s['shown']}"]
    if r["warning"]: lines.append("Warning: " + r["warning"])
    if r["missing_tables"]: lines.append("Missing tables: " + ", ".join(r["missing_tables"]))
    if r["missing_columns"]: lines.append("Missing columns: " + flatten_missing(r["missing_columns"]))
    if not r["broken_image_references"]: lines.append(r["empty_state"]["message"]); return "\n".join(lines)
    lines += ["", "content_id | issue_type | image_reference"]
    for f in r["broken_image_references"]: lines.append(f"{f['content_id']} | {f['issue_type']} | {f['image_reference']}")
    return "\n".join(lines)
