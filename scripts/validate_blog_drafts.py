#!/usr/bin/env python3
"""Validate generated static-site blog drafts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from output.blog_frontmatter_validator import validate_blog_draft_file


def _draft_paths(draft_dir: Path, manifest_path: Path | None) -> list[Path]:
    if manifest_path is None:
        return sorted(draft_dir.glob("*.md"))

    manifest = json.loads(manifest_path.read_text())
    base = manifest_path.parent.parent if manifest_path.parent.name == "drafts" else draft_dir.parent
    paths: list[Path] = []
    for entry in manifest.get("drafts", []):
        draft_path = entry.get("draft_path")
        if not draft_path:
            continue
        path = Path(draft_path)
        paths.append(path if path.is_absolute() else base / path)
    return sorted(paths)


def validate_draft_directory(
    draft_dir: str | Path,
    *,
    manifest: str | Path | None = None,
) -> dict[str, Any]:
    """Validate markdown drafts found in a directory or manifest."""
    draft_root = Path(draft_dir)
    manifest_path = Path(manifest) if manifest else None
    results = []

    for path in _draft_paths(draft_root, manifest_path):
        if not path.exists():
            results.append(
                {
                    "ok": False,
                    "path": str(path),
                    "frontmatter": {},
                    "errors": [
                        {
                            "level": "error",
                            "code": "missing_draft_file",
                            "message": "Draft file listed for validation does not exist.",
                            "field": None,
                            "path": str(path),
                        }
                    ],
                    "warnings": [],
                }
            )
            continue
        results.append(validate_blog_draft_file(path).to_dict())

    error_count = sum(len(result["errors"]) for result in results)
    warning_count = sum(len(result["warnings"]) for result in results)
    return {
        "ok": error_count == 0,
        "draft_dir": str(draft_root),
        "manifest": str(manifest_path) if manifest_path else None,
        "draft_count": len(results),
        "error_count": error_count,
        "warning_count": warning_count,
        "results": results,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--draft-dir", default="drafts", help="Directory containing markdown drafts.")
    parser.add_argument("--manifest", help="Optional draft manifest JSON to validate from.")
    parser.add_argument("--json", action="store_true", help="Emit machine-readable JSON.")
    args = parser.parse_args(argv)

    report = validate_draft_directory(args.draft_dir, manifest=args.manifest)
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(
            f"Validated {report['draft_count']} draft(s): "
            f"{report['error_count']} error(s), {report['warning_count']} warning(s)"
        )
        for result in report["results"]:
            for issue in result["errors"] + result["warnings"]:
                print(
                    f"{issue['level'].upper()} {result['path']}: "
                    f"{issue['code']}: {issue['message']}",
                    file=sys.stderr if issue["level"] == "error" else sys.stdout,
                )

    return 0 if report["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
