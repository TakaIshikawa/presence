"""Read-only configuration drift audit."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


ENV_PLACEHOLDER_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")
SECRET_KEY_RE = re.compile(
    r"(secret|token|password|passwd|api[_-]?key|private[_-]?key|credential)",
    re.IGNORECASE,
)

BLOCKING_KINDS = {
    "missing",
    "unresolved-env",
    "empty-value",
    "type-mismatch",
}


@dataclass(frozen=True)
class ConfigDriftFinding:
    """One config drift finding."""

    kind: str
    path: str
    severity: str
    message: str
    expected_type: str | None = None
    actual_type: str | None = None
    redacted: bool = False

    def as_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "kind": self.kind,
            "path": self.path,
            "severity": self.severity,
            "message": self.message,
            "redacted": self.redacted,
        }
        if self.expected_type is not None:
            data["expected_type"] = self.expected_type
        if self.actual_type is not None:
            data["actual_type"] = self.actual_type
        return data


def audit_config_drift(
    config_path: str | Path = "config.yaml",
    local_config_path: str | Path = "config.local.yaml",
) -> dict[str, Any]:
    """Compare reference and local YAML config structure without exposing values."""
    reference = _load_yaml_mapping(Path(config_path), "config")
    local = _load_yaml_mapping(Path(local_config_path), "local config")
    findings = _compare_node(reference, local, ())
    findings.extend(_find_local_value_issues(reference, local, ()))
    findings = sorted(findings, key=lambda item: (item.path, item.kind, item.message))
    counts = _counts(findings)
    blocking_count = sum(1 for finding in findings if finding.kind in BLOCKING_KINDS)
    return {
        "config_path": str(config_path),
        "local_config_path": str(local_config_path),
        "status": "failed" if blocking_count else "passed",
        "blocking_count": blocking_count,
        "counts": counts,
        "findings": [finding.as_dict() for finding in findings],
    }


def _load_yaml_mapping(path: Path, label: str) -> dict[str, Any]:
    with path.open("r") as handle:
        data = yaml.safe_load(handle) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{label} must be a YAML mapping: {path}")
    return data


def _compare_node(
    reference: Any,
    local: Any,
    path: tuple[str, ...],
) -> list[ConfigDriftFinding]:
    findings: list[ConfigDriftFinding] = []
    current_path = _format_path(path)
    if isinstance(reference, dict):
        if not isinstance(local, dict):
            findings.append(
                _finding(
                    "type-mismatch",
                    current_path,
                    f"{current_path} should be a mapping",
                    expected_type=_type_name(reference),
                    actual_type=_type_name(local),
                )
            )
            return findings

        for key in sorted(reference):
            child_path = (*path, str(key))
            if key not in local:
                rendered = _format_path(child_path)
                findings.append(
                    _finding(
                        "missing",
                        rendered,
                        f"{rendered} is missing from local config",
                    )
                )
                continue
            findings.extend(_compare_node(reference[key], local[key], child_path))

        for key in sorted(local):
            if key not in reference:
                rendered = _format_path((*path, str(key)))
                findings.append(
                    _finding(
                        "extra",
                        rendered,
                        f"{rendered} exists only in local config",
                        severity="warning",
                    )
                )
        return findings

    if _value_kind(reference) != _value_kind(local):
        findings.append(
            _finding(
                "type-mismatch",
                current_path,
                f"{current_path} has a different value type than reference config",
                expected_type=_type_name(reference),
                actual_type=_type_name(local),
            )
        )
    return findings


def _find_local_value_issues(
    reference: Any,
    local: Any,
    path: tuple[str, ...],
) -> list[ConfigDriftFinding]:
    findings: list[ConfigDriftFinding] = []
    if isinstance(local, dict):
        reference_dict = reference if isinstance(reference, dict) else {}
        for key in sorted(local):
            findings.extend(
                _find_local_value_issues(
                    reference_dict.get(key),
                    local[key],
                    (*path, str(key)),
                )
            )
        return findings

    rendered = _format_path(path)
    if isinstance(local, str):
        missing_env = sorted(
            env_var
            for env_var in set(ENV_PLACEHOLDER_RE.findall(local))
            if not os.environ.get(env_var)
        )
        for env_var in missing_env:
            findings.append(
                _finding(
                    "unresolved-env",
                    rendered,
                    f"{rendered} references unset environment variable {env_var}",
                )
            )

    if _is_required_looking(reference, path) and _is_empty(local):
        findings.append(
            _finding(
                "empty-value",
                rendered,
                f"{rendered} is empty but appears required by reference config",
            )
        )
    return findings


def _is_required_looking(reference: Any, path: tuple[str, ...]) -> bool:
    if _is_secret_path(path):
        return True
    if isinstance(reference, str):
        return bool(reference.strip())
    if isinstance(reference, (int, float, bool)) and not isinstance(reference, bool):
        return True
    if isinstance(reference, bool):
        return True
    if isinstance(reference, list):
        return bool(reference)
    if isinstance(reference, dict):
        return bool(reference)
    return False


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    if isinstance(value, (list, dict)):
        return len(value) == 0
    return False


def _finding(
    kind: str,
    path: str,
    message: str,
    *,
    severity: str = "error",
    expected_type: str | None = None,
    actual_type: str | None = None,
) -> ConfigDriftFinding:
    return ConfigDriftFinding(
        kind=kind,
        path=path,
        severity=severity,
        message=message,
        expected_type=expected_type,
        actual_type=actual_type,
        redacted=_is_secret_path(tuple(path.split("."))),
    )


def _format_path(path: tuple[str, ...]) -> str:
    return ".".join(path) if path else "<root>"


def _is_secret_path(path: tuple[str, ...]) -> bool:
    return any(SECRET_KEY_RE.search(part) for part in path)


def _value_kind(value: Any) -> type:
    if isinstance(value, dict):
        return dict
    if isinstance(value, list):
        return list
    if isinstance(value, bool):
        return bool
    if isinstance(value, str):
        return str
    if isinstance(value, int) and not isinstance(value, bool):
        return int
    if isinstance(value, float):
        return float
    if value is None:
        return type(None)
    return object


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, dict):
        return "mapping"
    if isinstance(value, list):
        return "list"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    return type(value).__name__


def _counts(findings: list[ConfigDriftFinding]) -> dict[str, int]:
    counts = {kind: 0 for kind in sorted({*BLOCKING_KINDS, "extra"})}
    for finding in findings:
        counts[finding.kind] = counts.get(finding.kind, 0) + 1
    return dict(sorted(counts.items()))
