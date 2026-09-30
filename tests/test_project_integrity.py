"""
Deliverable-integrity tests: guard the configuration artefacts the marking
rubric looks at (roster, topics, alert rules, SQL migrations, dashboards,
API surface) so a refactor cannot silently break them.

Pure Python -- reads files from the repo; no Docker required.
Run with: pytest tests/test_project_integrity.py -v
"""

from __future__ import annotations

import csv
import glob
import json
import os
import re
import sys

import pytest
import yaml

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from common.clinical import news2_score  # noqa: E402
from config.settings import Settings  # noqa: E402


def path(*parts: str) -> str:
    return os.path.join(ROOT, *parts)


# --------------------------------------------------------------------------
# Patient roster
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def roster() -> list[dict]:
    with open(path("config", "patients.csv"), newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_roster_has_15_to_20_unique_patients(roster):
    ids = [p["patient_id"] for p in roster]
    assert 15 <= len(ids) <= 20
    assert len(set(ids)) == len(ids)
    assert len({p["name"] for p in roster}) == len(roster)
    assert len({p["bed"] for p in roster}) == len(roster)


def test_roster_covers_the_four_required_wards(roster):
    assert {p["ward"] for p in roster} == {"ICU", "General Ward", "Cardiology", "Post-Op"}


def test_every_patient_baseline_is_healthy_enough_not_to_alert_at_rest(roster):
    """A resting patient must score 'low', otherwise the ward would raise
    constant false alerts and drown out the injected anomalies."""
    for p in roster:
        score, band, _ = news2_score(
            float(p["baseline_hr"]),
            float(p["baseline_spo2"]),
            float(p["baseline_sbp"]),
            float(p["baseline_temp"]),
        )
        assert band == "low", f"{p['patient_id']} baseline scores {score} ({band})"


# --------------------------------------------------------------------------
# Kafka topics / settings
# --------------------------------------------------------------------------


def test_default_topics_are_the_specified_names():
    s = Settings(_env_file=None)
    assert s.topic_vitals_raw == "vitals_raw"
    assert s.topic_lab_batch == "lab_batch"
    assert s.topic_vitals_raw_partitions == 3


def test_env_example_and_compose_agree_on_topics():
    with open(path(".env.example"), encoding="utf-8") as fh:
        env = dict(re.findall(r"^([A-Z_]+)=(.*)$", fh.read(), flags=re.M))
    with open(path("docker-compose.yml"), encoding="utf-8") as fh:
        compose = fh.read()
    for key in ("TOPIC_VITALS_RAW", "TOPIC_LAB_BATCH"):
        assert f"{key}:-{env[key]}}}" in compose, f"compose default for {key} differs from .env.example"


def test_simulated_clock_is_five_minutes_per_day():
    with open(path(".env.example"), encoding="utf-8") as fh:
        assert re.search(r"^SIM_DAY_SECONDS=300$", fh.read(), flags=re.M)


# --------------------------------------------------------------------------
# Alert rules
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def alert_rules() -> dict[str, dict]:
    with open(path("observability", "alerts.yml"), encoding="utf-8") as fh:
        doc = yaml.safe_load(fh)
    return {r["alert"]: r for g in doc["groups"] for r in g["rules"]}


def test_ingestion_silence_alert_is_30_seconds(alert_rules):
    rule = alert_rules["IngestionSilence"]
    assert "> 30" in rule["expr"] and "vitals_last_emit_timestamp_seconds" in rule["expr"]
    assert rule["labels"]["severity"] == "critical"


def test_clinical_urgency_alerts_exist(alert_rules):
    assert "CriticalPatientDeterioration" in alert_rules
    assert "CriticalVitalsSpike" in alert_rules
    assert alert_rules["CriticalPatientDeterioration"]["labels"]["severity"] == "critical"


def test_every_alert_has_expr_and_summary(alert_rules):
    for name, rule in alert_rules.items():
        assert rule.get("expr"), name
        assert rule["annotations"].get("summary"), name


def test_metrics_used_by_alert_rules_are_exported_by_the_code(alert_rules):
    sources = ""
    for f in glob.glob(path("serving", "*.py")) + glob.glob(path("ingestion", "*.py")):
        with open(f, encoding="utf-8") as fh:
            sources += fh.read()
    for name in (
        "vitals_last_emit_timestamp_seconds",
        "ward_patient_high_risk",
        "ward_spike_alerts_last_minute",
        "ops_pipeline_heartbeat_last_seen_seconds",
        "lab_file_last_written_timestamp_seconds",
    ):
        assert f'"{name}"' in sources, f"{name} is alerted on but never exported"


# --------------------------------------------------------------------------
# SQL migrations: idempotent
# --------------------------------------------------------------------------


@pytest.mark.parametrize("sql_file", sorted(glob.glob(os.path.join(ROOT, "sql", "*.sql"))))
def test_sql_migrations_are_idempotent(sql_file):
    with open(sql_file, encoding="utf-8") as fh:
        sql = re.sub(r"--.*", "", fh.read())  # strip comments
    for stmt in re.findall(
        r"CREATE\s+(?:UNIQUE\s+)?(?:TABLE|INDEX|SCHEMA)\s+(?!IF NOT EXISTS)\S+", sql, flags=re.I
    ):
        pytest.fail(f"{os.path.basename(sql_file)}: non-idempotent statement: {stmt}")
    for stmt in re.findall(r"CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(?!IF NOT EXISTS)\S+", sql, flags=re.I):
        # plain CREATE VIEW is only acceptable directly after an explicit DROP VIEW IF EXISTS
        name = stmt.split()[-1]
        assert re.search(
            rf"DROP\s+VIEW\s+IF\s+EXISTS\s+{re.escape(name)}", sql, flags=re.I
        ), f"{os.path.basename(sql_file)}: {stmt} is not guarded"


def test_the_three_named_warehouse_tables_exist_in_sql():
    text = ""
    for f in glob.glob(path("sql", "*.sql")):
        with open(f, encoding="utf-8") as fh:
            text += fh.read()
    for name in ("speed_vitals_alerts", "batch_lab_results", "daily_patient_risk_summary"):
        assert re.search(rf"VIEW\s+public\.{name}\b", text), name
    assert "CREATE TABLE IF NOT EXISTS speed.vitals_alerts" in text
    assert "CREATE TABLE IF NOT EXISTS batch.patient_lab_results" in text
    assert "CREATE TABLE IF NOT EXISTS batch.patient_risk_daily" in text


# --------------------------------------------------------------------------
# Grafana dashboards
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dash", sorted(glob.glob(os.path.join(ROOT, "observability", "grafana", "dashboards", "*.json")))
)
def test_dashboards_are_valid_and_panels_do_not_overlap(dash):
    with open(dash, encoding="utf-8") as fh:
        d = json.load(fh)
    assert d["uid"] and d["title"] and d["panels"]
    ids = [p["id"] for p in d["panels"]]
    assert len(ids) == len(set(ids)), "duplicate panel ids"
    boxes = [(p["title"], p["gridPos"]) for p in d["panels"]]
    for i, (t1, a) in enumerate(boxes):
        assert a["x"] + a["w"] <= 24, f"{t1} exceeds the 24-column grid"
        for t2, b in boxes[i + 1 :]:
            overlap = (
                a["x"] < b["x"] + b["w"]
                and b["x"] < a["x"] + a["w"]
                and a["y"] < b["y"] + b["h"]
                and b["y"] < a["y"] + a["h"]
            )
            assert not overlap, f"panels overlap: {t1!r} / {t2!r}"


def test_both_required_dashboards_are_provisioned():
    uids = set()
    for f in glob.glob(path("observability", "grafana", "dashboards", "*.json")):
        with open(f, encoding="utf-8") as fh:
            uids.add(json.load(fh)["uid"])
    assert {"ward-clinical", "pipeline-health"} <= uids


# --------------------------------------------------------------------------
# Serving API surface
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def api_app():
    pytest.importorskip("fastapi")
    pytest.importorskip("prometheus_fastapi_instrumentator")
    from serving import api

    return api


def test_api_exposes_the_required_v1_endpoints(api_app):
    # OpenAPI schema is stable across FastAPI versions (unlike app.routes internals)
    paths = set(api_app.app.openapi()["paths"])
    for required in (
        "/api/v1/ward/live",
        "/api/v1/patients/{patient_id}/risk",
        "/api/v1/alerts/active",
        "/health",
        "/metrics",
    ):
        assert required in paths, required


def test_api_keeps_legacy_unversioned_paths(api_app):
    """Probe with a request that is rejected by input validation (400) before
    touching Postgres: 400 proves the route exists, 404 would prove it doesn't."""
    from fastapi.testclient import TestClient

    client = TestClient(api_app.app)
    assert client.get("/patients/P001/vitals?window=abc").status_code == 400
    assert client.get("/api/v1/patients/P001/vitals?window=abc").status_code == 400


def test_overall_risk_band_is_the_worse_of_the_two_layers(api_app):
    assert api_app._worst_band("low", "high") == "high"
    assert api_app._worst_band("medium", None) == "medium"
    assert api_app._worst_band(None, None) is None


def test_ack_rejects_unknown_alert_type(api_app):
    from fastapi.testclient import TestClient

    client = TestClient(api_app.app)
    assert client.post("/api/v1/alerts/1/ack?alert_type=bogus").status_code == 422
