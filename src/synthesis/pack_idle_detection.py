"""Pack idle detection and stalled session recovery analyzer.

Dimensions: idle period detection, stall recovery time, proactive intervention,
session continuity, resource waste.
"""

from __future__ import annotations

from typing import Any, Mapping


def _int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, float):
        return int(value)
    return 0


def _float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return float(value)
    return 0.0


def _percentage(numerator: int | float, denominator: int | float) -> float:
    if denominator <= 0:
        return 0.0
    return round((numerator / denominator) * 100.0, 2)


def _average(values: list[int | float]) -> float:
    if not values:
        return 0.0
    return round(sum(values) / len(values), 2)


def _empty_result() -> dict[str, Any]:
    return {
        "total_packs": 0,
        "total_sessions": 0,
        "idle_periods_detected": 0,
        "idle_recovery_rate": 0.0,
        "avg_idle_duration_seconds": 0.0,
        "stalled_session_rate": 0.0,
        "stall_recovery_rate": 0.0,
        "proactive_intervention_rate": 0.0,
        "idle_waste_percentage": 0.0,
        "high_quality_packs": 0,
        "low_quality_packs": 0,
        "idle_detection_score": 0.0,
    }


def analyze_pack_idle_detection(records: object) -> dict[str, Any]:
    """Analyze idle detection and stalled session recovery across packs."""
    if records is None:
        return _empty_result()
    if not isinstance(records, list):
        raise ValueError("records must be a list of pack dictionaries")
    if not records:
        return _empty_result()

    total_packs = 0
    total_sessions = 0
    total_idle_detected = 0
    total_idle_recovered = 0
    all_avg_idle_durations: list[float] = []
    total_stalled = 0
    total_recovered_stalled = 0
    total_proactive = 0
    total_session_duration = 0
    total_wasted = 0
    pack_scores: list[float] = []
    high_quality_packs = 0
    low_quality_packs = 0

    for record in records:
        if not isinstance(record, Mapping):
            continue

        total_packs += 1
        sessions = _int(record.get("total_sessions"))
        idle_detected = _int(record.get("idle_periods_detected"))
        idle_recovered = _int(record.get("idle_periods_recovered"))
        avg_idle_dur = _float(record.get("avg_idle_duration_seconds"))
        stalled = _int(record.get("stalled_sessions"))
        recovered_stalled = _int(record.get("recovered_stalled_sessions"))
        proactive = _int(record.get("proactive_interventions"))
        session_duration = _int(record.get("total_session_duration_seconds"))
        wasted = _int(record.get("wasted_idle_seconds"))

        total_sessions += sessions
        total_idle_detected += idle_detected
        total_idle_recovered += idle_recovered
        if avg_idle_dur > 0:
            all_avg_idle_durations.append(avg_idle_dur)
        total_stalled += stalled
        total_recovered_stalled += recovered_stalled
        total_proactive += proactive
        total_session_duration += session_duration
        total_wasted += wasted

        # Pack score calculation
        if sessions <= 0:
            pack_scores.append(0.0)
            low_quality_packs += 1
            continue

        # High idle recovery (0-0.30): >80% recovered = full
        ir_rate = idle_recovered / idle_detected if idle_detected > 0 else 1.0
        ir_score = min(ir_rate / 0.80, 1.0) * 0.30

        # Low stalled sessions (0-0.25): <10% stalled = full
        st_rate = stalled / sessions if sessions > 0 else 0.0
        st_score = max(0.0, 1.0 - st_rate / 0.10) * 0.25

        # Proactive interventions (0-0.25): >50% proactive = full
        pi_rate = proactive / idle_detected if idle_detected > 0 else 0.0
        pi_score = min(pi_rate / 0.50, 1.0) * 0.25

        # Low idle waste (0-0.20): <5% wasted = full
        iw_rate = wasted / session_duration if session_duration > 0 else 0.0
        iw_score = max(0.0, 1.0 - iw_rate / 0.05) * 0.20

        pack_score = round(ir_score + st_score + pi_score + iw_score, 4)
        pack_scores.append(pack_score)

        if pack_score > 0.7:
            high_quality_packs += 1
        elif pack_score < 0.4:
            low_quality_packs += 1

    # Overall aggregated score
    if total_sessions > 0:
        overall_ir_rate = (
            total_idle_recovered / total_idle_detected
            if total_idle_detected > 0
            else 1.0
        )
        ir_component = min(overall_ir_rate / 0.80, 1.0) * 0.30

        overall_st_rate = total_stalled / total_sessions
        st_component = max(0.0, 1.0 - overall_st_rate / 0.10) * 0.25

        overall_pi_rate = (
            total_proactive / total_idle_detected
            if total_idle_detected > 0
            else 0.0
        )
        pi_component = min(overall_pi_rate / 0.50, 1.0) * 0.25

        overall_iw_rate = (
            total_wasted / total_session_duration
            if total_session_duration > 0
            else 0.0
        )
        iw_component = max(0.0, 1.0 - overall_iw_rate / 0.05) * 0.20

        overall_score = round(ir_component + st_component + pi_component + iw_component, 4)
    else:
        overall_score = 0.0

    return {
        "total_packs": total_packs,
        "total_sessions": total_sessions,
        "idle_periods_detected": total_idle_detected,
        "idle_recovery_rate": _percentage(total_idle_recovered, total_idle_detected),
        "avg_idle_duration_seconds": _average(all_avg_idle_durations),
        "stalled_session_rate": _percentage(total_stalled, total_sessions),
        "stall_recovery_rate": _percentage(total_recovered_stalled, total_stalled),
        "proactive_intervention_rate": _percentage(total_proactive, total_idle_detected),
        "idle_waste_percentage": _percentage(total_wasted, total_session_duration),
        "high_quality_packs": high_quality_packs,
        "low_quality_packs": low_quality_packs,
        "idle_detection_score": overall_score,
    }
