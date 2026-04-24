"""Portable review artifacts for pending reply drafts."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Iterable, Mapping

from engagement.reply_action_recommender import recommend_reply_action


PACKET_SCHEMA_VERSION = 1


def build_reply_review_packet(
    reply: Mapping[str, Any],
    *,
    include_recommended_action: bool = True,
) -> dict[str, Any]:
    """Build a JSON-safe packet for one queued reply draft."""
    relationship_context = _decode_json(reply.get("relationship_context"))
    platform_metadata = _decode_json(reply.get("platform_metadata"))
    quality_flags = _parse_string_list(reply.get("quality_flags"))
    packet: dict[str, Any] = {
        "schema_version": PACKET_SCHEMA_VERSION,
        "draft_id": reply.get("id"),
        "platform": reply.get("platform") or "x",
        "status": reply.get("status"),
        "detected_at": reply.get("detected_at"),
        "inbound": {
            "id": reply.get("inbound_tweet_id"),
            "url": reply.get("inbound_url"),
            "cid": reply.get("inbound_cid"),
            "text": reply.get("inbound_text"),
            "author": {
                "handle": reply.get("inbound_author_handle"),
                "id": reply.get("inbound_author_id"),
            },
        },
        "original_post": {
            "id": reply.get("our_platform_id") or reply.get("our_tweet_id"),
            "tweet_id": reply.get("our_tweet_id"),
            "platform_id": reply.get("our_platform_id"),
            "content_id": reply.get("our_content_id"),
            "text": reply.get("our_post_text"),
        },
        "draft": {
            "text": reply.get("draft_text"),
        },
        "classification": {
            "intent": reply.get("intent"),
            "priority": reply.get("priority"),
        },
        "relationship_context": relationship_context,
        "evaluator": {
            "quality_score": reply.get("quality_score"),
            "quality_flags": quality_flags,
            "sycophancy_flags": [flag for flag in quality_flags if "sycoph" in flag],
            "generic_flags": [flag for flag in quality_flags if "generic" in flag],
        },
        "dedup": _dedup_status(reply, platform_metadata),
        "platform_metadata": platform_metadata,
    }

    recommended_action = _recommended_action(reply, platform_metadata, include_recommended_action)
    if recommended_action is not None:
        packet["recommended_action"] = recommended_action
    return _json_safe(packet)


def build_reply_review_packets(
    replies: Iterable[Mapping[str, Any]],
    *,
    include_recommended_action: bool = True,
) -> list[dict[str, Any]]:
    return [
        build_reply_review_packet(
            reply,
            include_recommended_action=include_recommended_action,
        )
        for reply in replies
    ]


def packet_filename(packet: Mapping[str, Any]) -> str:
    """Return a deterministic filename containing platform and draft id."""
    platform = _filename_part(packet.get("platform") or "x")
    draft_id = _filename_part(packet.get("draft_id") or "unknown")
    return f"{platform}-draft-{draft_id}.json"


def write_reply_review_packets(
    packets: Iterable[Mapping[str, Any]],
    output_dir: str | Path,
) -> list[Path]:
    """Write one pretty JSON file per packet and return the paths."""
    out = Path(output_dir)
    out.mkdir(parents=True, exist_ok=True)
    paths: list[Path] = []
    for packet in packets:
        path = out / packet_filename(packet)
        path.write_text(
            json.dumps(packet, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        paths.append(path)
    return paths


def format_reply_packet_summary(packets: Iterable[Mapping[str, Any]]) -> str:
    """Return a compact human-readable summary for exported packets."""
    lines = []
    for packet in packets:
        inbound = packet.get("inbound") or {}
        author = inbound.get("author") or {}
        evaluator = packet.get("evaluator") or {}
        recommended = packet.get("recommended_action") or {}
        bits = [
            f"#{packet.get('draft_id')}",
            str(packet.get("platform") or "x"),
            f"@{author.get('handle') or '?'}",
        ]
        score = evaluator.get("quality_score")
        if score is not None:
            bits.append(f"quality={float(score):.1f}")
        flags = evaluator.get("quality_flags") or []
        if flags:
            bits.append(f"flags={','.join(flags)}")
        action = recommended.get("action")
        if action:
            bits.append(f"action={action}")
        lines.append("  ".join(bits))
    return "\n".join(lines) + ("\n" if lines else "")


def _decode_json(value: Any) -> Any:
    if value in (None, ""):
        return None
    if not isinstance(value, str):
        return _json_safe(value)
    try:
        return _json_safe(json.loads(value))
    except (json.JSONDecodeError, TypeError):
        return value


def _parse_string_list(value: Any) -> list[str]:
    decoded = _decode_json(value)
    if decoded in (None, ""):
        return []
    if isinstance(decoded, list):
        return [str(item) for item in decoded]
    return [str(decoded)]


def _dedup_status(reply: Mapping[str, Any], platform_metadata: Any) -> dict[str, Any]:
    metadata = platform_metadata if isinstance(platform_metadata, dict) else {}
    metadata_dedup = metadata.get("dedup")
    if isinstance(metadata_dedup, dict):
        return _json_safe(metadata_dedup)

    status = reply.get("dedup_status") or metadata.get("dedup_status") or "passed"
    dedup: dict[str, Any] = {"status": status}
    for key in ("dedup_reason", "dedup_similarity", "dedup_match_id", "dedup_source_table"):
        value = reply.get(key) if key in reply else metadata.get(key)
        if value is not None:
            dedup[key.removeprefix("dedup_")] = value
    return _json_safe(dedup)


def _recommended_action(
    reply: Mapping[str, Any],
    platform_metadata: Any,
    include_generated: bool,
) -> dict[str, Any] | None:
    for key in ("recommended_action", "action_recommendation"):
        if reply.get(key) is not None:
            return _coerce_recommendation(reply.get(key))
    if isinstance(platform_metadata, dict):
        for key in ("recommended_action", "action_recommendation"):
            if platform_metadata.get(key) is not None:
                return _coerce_recommendation(platform_metadata.get(key))
    if include_generated:
        return recommend_reply_action(reply).to_dict()
    return None


def _coerce_recommendation(value: Any) -> dict[str, Any]:
    decoded = _decode_json(value)
    if isinstance(decoded, dict):
        return _json_safe(decoded)
    return {"action": decoded}


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _json_safe(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [_json_safe(inner) for inner in value]
    if isinstance(value, tuple):
        return [_json_safe(inner) for inner in value]
    return value


def _filename_part(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value)).strip("-") or "unknown"
