"""
Unit tests for the speed layer's immediate-alert logic and the electrolyte
lab extension in common/clinical.py. Pure Python -- no Docker/Spark needed.

Run with: pytest tests/test_speed_alerts.py -v
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.clinical import (  # noqa: E402
    adjust_risk_with_labs,
    apply_alert_cooldown,
    detect_critical_spikes,
    lab_flag,
)

T0 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)

# --------------------------------------------------------------------------
# detect_critical_spikes
# --------------------------------------------------------------------------


def rules(breaches):
    return {b["rule"] for b in breaches}


def test_normal_reading_raises_nothing():
    assert detect_critical_spikes(78, 97, 120, 36.7) == []


def test_spo2_below_90_is_severe_hypoxia():
    breaches = detect_critical_spikes(78, 89.9, 120, 36.7)
    assert rules(breaches) == {"severe_hypoxia"}
    assert breaches[0]["threshold"] == 90 and breaches[0]["value"] == 89.9


def test_spo2_exactly_90_is_not_a_breach():
    assert detect_critical_spikes(78, 90, 120, 36.7) == []


def test_heart_rate_above_130_is_tachycardia_crisis():
    assert rules(detect_critical_spikes(131, 97, 120, 36.7)) == {"tachycardia_crisis"}
    assert detect_critical_spikes(130, 97, 120, 36.7) == []


def test_heart_rate_below_40_is_bradycardia_crisis():
    assert rules(detect_critical_spikes(39, 97, 120, 36.7)) == {"bradycardia_crisis"}


def test_low_systolic_and_high_temperature():
    assert rules(detect_critical_spikes(78, 97, 85, 36.7)) == {"hypotension_crisis"}
    assert rules(detect_critical_spikes(78, 97, 120, 39.6)) == {"hyperpyrexia"}


def test_multiple_simultaneous_breaches_all_reported():
    # a sepsis-like crash: fast HR, low SpO2, low BP, fever
    got = rules(detect_critical_spikes(140, 85, 80, 39.5))
    assert got == {"tachycardia_crisis", "severe_hypoxia", "hypotension_crisis", "hyperpyrexia"}


def test_missing_vitals_never_breach():
    assert detect_critical_spikes(None, None, None, None) == []


# --------------------------------------------------------------------------
# apply_alert_cooldown
# --------------------------------------------------------------------------


def cand(patient, rule, seconds):
    return {"patient_id": patient, "rule": rule, "event_ts": T0 + timedelta(seconds=seconds)}


def test_burst_collapses_to_one_alert_per_cooldown():
    burst = [cand("P001", "severe_hypoxia", s) for s in range(0, 40, 2)]  # 20 readings in 40s
    accepted = apply_alert_cooldown(burst, {}, cooldown_seconds=60)
    assert len(accepted) == 1
    assert accepted[0]["event_ts"] == T0


def test_alert_fires_again_after_cooldown_elapses():
    readings = [cand("P001", "severe_hypoxia", s) for s in (0, 30, 61, 100, 125)]
    accepted = apply_alert_cooldown(readings, {}, cooldown_seconds=60)
    assert [a["event_ts"] for a in accepted] == [T0, T0 + timedelta(seconds=61), T0 + timedelta(seconds=125)]


def test_cooldown_is_per_patient_and_per_rule():
    readings = [
        cand("P001", "severe_hypoxia", 0),
        cand("P002", "severe_hypoxia", 1),  # different patient
        cand("P001", "tachycardia_crisis", 2),  # different rule
    ]
    assert len(apply_alert_cooldown(readings, {}, 60)) == 3


def test_existing_alert_in_db_suppresses_new_candidate():
    last = {("P001", "severe_hypoxia"): T0}
    accepted = apply_alert_cooldown([cand("P001", "severe_hypoxia", 10)], last, 60)
    assert accepted == []


def test_out_of_order_candidates_are_processed_in_event_time_order():
    accepted = apply_alert_cooldown(
        [cand("P001", "severe_hypoxia", 30), cand("P001", "severe_hypoxia", 0)], {}, 60
    )
    assert len(accepted) == 1 and accepted[0]["event_ts"] == T0


# --------------------------------------------------------------------------
# Electrolytes in the lab flagging / risk adjustment
# --------------------------------------------------------------------------


def test_potassium_and_sodium_flagged_against_reference_ranges():
    assert lab_flag(6.1, 3.5, 5.0, "Potassium") == "high"
    assert lab_flag(4.2, 3.5, 5.0, "Potassium") == "normal"
    assert lab_flag(128, 135, 145, "Sodium") == "low"


def test_electrolyte_defaults_used_when_file_omits_reference_range():
    assert lab_flag(6.0, None, None, "Potassium") == "high"
    assert lab_flag(129, None, None, "Sodium") == "low"


def test_hyperkalaemia_and_hyponatraemia_raise_risk_but_opposite_directions_do_not():
    score, reasons = adjust_risk_with_labs(2, {"Potassium": "high", "Sodium": "low"})
    assert score == 2 + 2 + 1
    assert reasons == ["potassium_high", "sodium_low"]

    score, reasons = adjust_risk_with_labs(2, {"Potassium": "low", "Sodium": "high"})
    assert score == 2 and reasons == []
