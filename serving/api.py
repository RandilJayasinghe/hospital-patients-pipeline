"""
FastAPI serving layer -- the "API endpoint for real-time ward monitoring
figures" the use case asks for, and the merge point where the speed layer's
"right now" and the batch layer's "as of the last completed day" answer the
business question together.

Versioned REST surface (also served at the un-prefixed legacy paths):

    GET  /api/v1/ward/live                  real-time ward figures
    GET  /api/v1/patients                   roster with current risk band
    GET  /api/v1/patients/{id}/risk         realtime trend  +  lab-adjusted daily risk
    GET  /api/v1/patients/{id}/vitals       recent windowed/raw series
    GET  /api/v1/alerts/active              sustained + immediate-spike alerts
    POST /api/v1/alerts/{id}/ack            acknowledge an alert
    GET  /api/v1/reports/daily/{sim_day}    the consolidated daily report (HTML)
    GET  /health, /ready, /metrics          liveness (with lag), readiness, Prometheus

Every endpoint reads from Postgres via common.db (same DSN, same module
every other service uses -- no separate config surface). A background task
started at app startup polls a handful of cheap queries every
METRICS_POLL_SECONDS and republishes them as Prometheus gauges, so pipeline
health (heartbeat age, speed-layer lag, batch freshness) and clinical state
(high-risk patients, spike alerts) are visible at /metrics alongside the
automatic per-route HTTP metrics from prometheus-fastapi-instrumentator --
both share the default registry, so one /metrics endpoint serves both.

Every request is logged as one structured JSON line carrying an
X-Trace-Id header (generated per-request, or echoed back if the caller
supplied one) -- this is what lets a demo grep one trace_id across
data/logs/*.jsonl and show a single vitals reading's journey from producer
to Kafka to Spark to this API response.
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Literal, Optional

import psycopg2
import psycopg2.extras
from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from prometheus_client import Gauge
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.db import get_connection  # noqa: E402
from common.logging_setup import configure_logging, new_trace_id  # noqa: E402
from config.settings import get_settings  # noqa: E402

SETTINGS = get_settings()
LOGGER = configure_logging("api", logs_dir=os.environ.get("LOGS_DIR"))

METRICS_POLL_SECONDS = 15
BAND_SEVERITY = {"low": 0, "medium": 1, "high": 2}

# ---- Prometheus gauges kept fresh by the background poller ----------------
HEARTBEAT_AGE = Gauge(
    "ops_pipeline_heartbeat_last_seen_seconds",
    "Seconds since each pipeline service last reported a heartbeat",
    ["service"],
)
BATCH_LAST_SUCCESS_AGE = Gauge(
    "ops_batch_last_success_seconds", "Seconds since the last successful batch_job run"
)
SPEED_LATEST_WINDOW_AGE = Gauge(
    "speed_latest_window_age_seconds", "Seconds since the most recent speed-layer window was written"
)
SPEED_LAG = Gauge(
    "speed_layer_lag_seconds", "Seconds since the newest cleaned reading landed in speed.vitals_clean"
)
DATA_QUALITY_CRITICAL_FAILURES = Gauge(
    "ops_data_quality_critical_failures",
    "Count of critical data-quality rule failures for the most recent sim_day checked",
)
WARD_ACTIVE_BEDS = Gauge("ward_active_beds", "Patients with a speed-layer window in the active period")
WARD_HIGH_RISK_PATIENTS = Gauge(
    "ward_high_risk_patients", "Patients whose current speed-layer NEWS2 band is 'high'"
)
WARD_PATIENT_HIGH_RISK = Gauge(
    "ward_patient_high_risk",
    "1 if this patient's current speed-layer NEWS2 band is 'high' (fresh window), else 0",
    ["patient_id", "ward"],
)
WARD_SPIKE_ALERTS_LAST_MINUTE = Gauge(
    "ward_spike_alerts_last_minute", "Immediate spike alerts raised in speed.vitals_alerts in the last 60s"
)


def _dsn() -> str:
    return SETTINGS.postgres_dsn_vitals


async def poll_metrics_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(_refresh_gauges)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("metrics_poll_failed", stage="serving", error=str(exc))
        await asyncio.sleep(METRICS_POLL_SECONDS)


def _refresh_gauges() -> None:
    active = SETTINGS.api_active_window_seconds
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT service, EXTRACT(EPOCH FROM (now() - last_seen_at)) FROM ops.pipeline_heartbeat"
            )
            for service, age in cur.fetchall():
                HEARTBEAT_AGE.labels(service=service).set(float(age))

            cur.execute(
                "SELECT EXTRACT(EPOCH FROM (now() - finished_at)) FROM ops.batch_runs "
                "WHERE status = 'success' ORDER BY finished_at DESC LIMIT 1"
            )
            row = cur.fetchone()
            if row:
                BATCH_LAST_SUCCESS_AGE.set(float(row[0]))

            # GREATEST(0, ..): an open window's end is in the future, which would read as negative age
            cur.execute(
                "SELECT GREATEST(0, EXTRACT(EPOCH FROM (now() - max(window_end)))) FROM speed.patient_vitals_1m"
            )
            row = cur.fetchone()
            if row and row[0] is not None:
                SPEED_LATEST_WINDOW_AGE.set(float(row[0]))

            cur.execute("SELECT EXTRACT(EPOCH FROM (now() - max(inserted_at))) FROM speed.vitals_clean")
            row = cur.fetchone()
            if row and row[0] is not None:
                SPEED_LAG.set(float(row[0]))

            cur.execute(
                "SELECT COALESCE(SUM(failed_count), 0) FROM ops.data_quality "
                "WHERE is_critical AND NOT passed AND sim_day = (SELECT max(sim_day) FROM ops.data_quality)"
            )
            row = cur.fetchone()
            DATA_QUALITY_CRITICAL_FAILURES.set(float(row[0]) if row else 0.0)

            cur.execute(
                """
                SELECT patient_id, ward,
                       (current_risk_band = 'high'
                        AND current_window_start >= now() - make_interval(secs => %s))::int AS high_now,
                       (current_window_start >= now() - make_interval(secs => %s))::int AS active_now
                FROM serving.patient_360
                """,
                (active, active),
            )
            high_total = active_total = 0
            for patient_id, ward, high_now, active_now in cur.fetchall():
                WARD_PATIENT_HIGH_RISK.labels(patient_id=patient_id, ward=ward or "unknown").set(
                    high_now or 0
                )
                high_total += high_now or 0
                active_total += active_now or 0
            WARD_HIGH_RISK_PATIENTS.set(high_total)
            WARD_ACTIVE_BEDS.set(active_total)

            cur.execute(
                "SELECT count(*) FROM speed.vitals_alerts WHERE raised_at >= now() - interval '60 seconds'"
            )
            WARD_SPIKE_ALERTS_LAST_MINUTE.set(float(cur.fetchone()[0]))


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(poll_metrics_loop())
    LOGGER.info("api_started", stage="serving")
    yield
    task.cancel()


app = FastAPI(
    title="Hospital Vitals Monitoring API",
    version="1.0.0",
    description="Lambda serving layer: merges speed-layer (real-time) and batch-layer (daily, lab-adjusted) views.",
    lifespan=lifespan,
)
Instrumentator().instrument(app).expose(app)


@app.middleware("http")
async def trace_id_middleware(request: Request, call_next):
    trace_id = request.headers.get("X-Trace-Id", new_trace_id())
    start = time.time()
    response = await call_next(request)
    duration_ms = round((time.time() - start) * 1000, 2)
    response.headers["X-Trace-Id"] = trace_id
    LOGGER.info(
        "api_request",
        stage="serving",
        trace_id=trace_id,
        method=request.method,
        path=request.url.path,
        status_code=response.status_code,
        duration_ms=duration_ms,
    )
    return response


# --------------------------------------------------------------------------
# Response models
# --------------------------------------------------------------------------


class HealthResponse(BaseModel):
    status: str
    postgres: bool
    kafka: bool
    speed_layer_lag_seconds: Optional[float] = None
    stream_heartbeat_age_seconds: Optional[float] = None
    detail: Optional[str] = None


class ReadyResponse(BaseModel):
    ready: bool
    heartbeat_fresh: bool
    serving_view_populated: bool


class AvgVitals(BaseModel):
    heart_rate: Optional[float] = None
    spo2: Optional[float] = None
    systolic_bp: Optional[float] = None
    diastolic_bp: Optional[float] = None
    temperature: Optional[float] = None


class WardLiveResponse(BaseModel):
    as_of: datetime
    patients_monitored: int
    active_beds: int
    abnormal_vitals_count: int
    readings_last_5min: int
    risk_band_counts: dict[str, int]
    active_alerts: int
    spike_alerts_last_5min: int
    avg_vitals: AvgVitals
    avg_score_by_ward: dict[str, Optional[float]]


class AckRequest(BaseModel):
    acknowledged_by: Optional[str] = None


class AlertmanagerWebhook(BaseModel):
    receiver: Optional[str] = None
    status: Optional[str] = "firing"
    alerts: list[dict] = []


# --------------------------------------------------------------------------
# Health / readiness (root-level, not versioned)
# --------------------------------------------------------------------------


@app.get("/health", response_model=HealthResponse, tags=["ops"])
def health() -> HealthResponse:
    """Liveness + pipeline lag. `degraded` if a dependency is unreachable or
    the speed layer has stopped landing data for longer than
    STREAM_STALLED_SECONDS (the end-to-end lag check)."""
    postgres_ok = True
    lag: Optional[float] = None
    stream_hb_age: Optional[float] = None
    try:
        with get_connection(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT EXTRACT(EPOCH FROM (now() - max(inserted_at))) FROM speed.vitals_clean")
                row = cur.fetchone()
                lag = float(row[0]) if row and row[0] is not None else None
                cur.execute(
                    "SELECT EXTRACT(EPOCH FROM (now() - last_seen_at)) FROM ops.pipeline_heartbeat "
                    "WHERE service = 'spark-stream'"
                )
                row = cur.fetchone()
                stream_hb_age = float(row[0]) if row else None
    except Exception:  # noqa: BLE001
        postgres_ok = False

    kafka_ok = True
    try:
        from confluent_kafka.admin import AdminClient

        admin = AdminClient({"bootstrap.servers": SETTINGS.kafka_bootstrap_internal})
        admin.list_topics(timeout=3)
    except Exception:  # noqa: BLE001
        kafka_ok = False

    lagging = lag is not None and lag > SETTINGS.stream_stalled_seconds
    status = "ok" if (postgres_ok and kafka_ok and not lagging) else "degraded"
    detail = f"speed layer has not landed data for {int(lag)}s" if lagging else None
    return HealthResponse(
        status=status,
        postgres=postgres_ok,
        kafka=kafka_ok,
        speed_layer_lag_seconds=lag,
        stream_heartbeat_age_seconds=stream_hb_age,
        detail=detail,
    )


@app.get("/ready", response_model=ReadyResponse, tags=["ops"])
def ready() -> ReadyResponse:
    heartbeat_fresh = False
    serving_populated = False
    try:
        with get_connection(_dsn()) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT count(*) FROM ops.pipeline_heartbeat WHERE now() - last_seen_at < interval '5 minutes'"
                )
                heartbeat_fresh = cur.fetchone()[0] > 0

                cur.execute("SELECT count(*) FROM serving.patient_360")
                serving_populated = cur.fetchone()[0] > 0
    except Exception:  # noqa: BLE001
        pass

    is_ready = heartbeat_fresh and serving_populated
    body = ReadyResponse(
        ready=is_ready, heartbeat_fresh=heartbeat_fresh, serving_view_populated=serving_populated
    )
    if not is_ready:
        return JSONResponse(status_code=503, content=json.loads(body.model_dump_json()))
    return body


# --------------------------------------------------------------------------
# Versioned business endpoints
# --------------------------------------------------------------------------

router = APIRouter()


def _worst_band(*bands: Optional[str]) -> Optional[str]:
    present = [b for b in bands if b in BAND_SEVERITY]
    return max(present, key=BAND_SEVERITY.__getitem__) if present else None


@router.get("/ward/live", response_model=WardLiveResponse, tags=["ward"])
def ward_live() -> WardLiveResponse:
    """Real-time ward monitoring figures (speed layer): how many beds are
    actively reporting, how many patients currently show abnormal vitals,
    ward-average vitals, and open alerts."""
    active = SETTINGS.api_active_window_seconds
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM reference.patients")
            patients_monitored = cur.fetchone()[0]

            cur.execute(
                """
                SELECT count(*) FILTER (WHERE current_window_start >= now() - make_interval(secs => %s)),
                       count(*) FILTER (WHERE current_window_start >= now() - make_interval(secs => %s)
                                          AND current_risk_band IN ('medium', 'high'))
                FROM serving.patient_360
                """,
                (active, active),
            )
            active_beds, abnormal_vitals_count = cur.fetchone()

            cur.execute(
                "SELECT count(*) FROM speed.vitals_clean WHERE inserted_at >= now() - interval '5 minutes'"
            )
            readings_last_5min = cur.fetchone()[0]

            cur.execute(
                """
                SELECT current_risk_band, count(*) FROM serving.patient_360
                WHERE current_risk_band IS NOT NULL
                  AND current_window_start >= now() - make_interval(secs => %s)
                GROUP BY current_risk_band
                """,
                (active,),
            )
            risk_band_counts = {band: count for band, count in cur.fetchall()}

            cur.execute(
                """
                SELECT (SELECT count(*) FROM speed.clinical_alerts
                         WHERE NOT acknowledged AND raised_at >= now() - make_interval(mins => %s))
                     + (SELECT count(*) FROM speed.vitals_alerts
                         WHERE NOT acknowledged AND raised_at >= now() - make_interval(mins => %s))
                """,
                (SETTINGS.api_alert_active_minutes, SETTINGS.api_alert_active_minutes),
            )
            active_alerts = cur.fetchone()[0]

            cur.execute(
                "SELECT count(*) FROM speed.vitals_alerts WHERE raised_at >= now() - interval '5 minutes'"
            )
            spike_alerts_last_5min = cur.fetchone()[0]

            cur.execute(
                """
                SELECT avg(w.avg_hr), avg(w.avg_spo2), avg(w.avg_sbp), avg(w.avg_dbp), avg(w.avg_temp)
                FROM reference.patients p
                JOIN LATERAL (
                    SELECT * FROM speed.patient_vitals_1m
                    WHERE patient_id = p.patient_id ORDER BY window_start DESC LIMIT 1
                ) w ON true
                WHERE w.window_start >= now() - make_interval(secs => %s)
                """,
                (active,),
            )
            hr, spo2, sbp, dbp, temp = cur.fetchone()

            cur.execute("SELECT ward, avg(current_score) FROM serving.patient_360 GROUP BY ward")
            avg_score_by_ward = {
                ward: (round(float(avg), 2) if avg is not None else None) for ward, avg in cur.fetchall()
            }

    def _r(value, digits=1):
        return round(float(value), digits) if value is not None else None

    return WardLiveResponse(
        as_of=datetime.now(timezone.utc),
        patients_monitored=patients_monitored,
        active_beds=active_beds,
        abnormal_vitals_count=abnormal_vitals_count,
        readings_last_5min=readings_last_5min,
        risk_band_counts=risk_band_counts,
        active_alerts=active_alerts,
        spike_alerts_last_5min=spike_alerts_last_5min,
        avg_vitals=AvgVitals(
            heart_rate=_r(hr),
            spo2=_r(spo2),
            systolic_bp=_r(sbp),
            diastolic_bp=_r(dbp),
            temperature=_r(temp, 2),
        ),
        avg_score_by_ward=avg_score_by_ward,
    )


@router.get("/patients", tags=["patients"])
def list_patients():
    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT patient_id, name, ward, bed, current_score, current_risk_band, current_trend,
                       batch_risk_band, spike_alerts_15m
                FROM serving.patient_360 ORDER BY patient_id
                """
            )
            return [dict(r) for r in cur.fetchall()]


@router.get("/patients/{patient_id}/risk", tags=["patients"])
def patient_risk(patient_id: str):
    """The merged Lambda answer for one patient: the speed layer's current
    bedside trend AND the batch layer's lab-adjusted risk from the last
    completed simulated day, with reason codes and the day-over-day delta."""
    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT * FROM serving.patient_360 WHERE patient_id = %s", (patient_id,))
            row = cur.fetchone()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Unknown patient_id {patient_id}")

    result = dict(row)
    return {
        "patient_id": result["patient_id"],
        "name": result["name"],
        "age": result["age"],
        "ward": result["ward"],
        "bed": result["bed"],
        "overall_risk_band": _worst_band(result["current_risk_band"], result["batch_risk_band"]),
        "realtime": {
            "window_start": result["current_window_start"],
            "score": result["current_score"],
            "risk_band": result["current_risk_band"],
            "trend": result["current_trend"],
            "immediate_alerts_last_15m": result["spike_alerts_15m"],
            "last_immediate_alert_at": result["last_spike_at"],
        },
        "batch_lab_adjusted": {
            "sim_day": result["batch_sim_day"],
            "vitals_band": result["batch_vitals_band"],
            "adjusted_score": result["batch_adjusted_score"],
            "risk_band": result["batch_risk_band"],
            "reason_codes": result["batch_reason_codes"],
            "abnormal_test_count": result["batch_abnormal_test_count"],
            "abnormal_labs": result["batch_abnormal_labs"],
            "score_delta_vs_previous_day": result["batch_score_delta"],
            "delta_direction": result["batch_delta_direction"],
        },
    }


@router.get("/patients/{patient_id}/vitals", tags=["patients"])
def patient_vitals(patient_id: str, window: str = "30m"):
    try:
        minutes = int(window.rstrip("m"))
    except ValueError:
        raise HTTPException(status_code=400, detail="window must look like '30m'")

    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT event_id, heart_rate, spo2, systolic_bp, diastolic_bp, temperature,
                       event_ts, trace_id
                FROM speed.vitals_clean
                WHERE patient_id = %s AND event_ts >= now() - make_interval(mins => %s)
                ORDER BY event_ts DESC
                LIMIT 500
                """,
                (patient_id, minutes),
            )
            items = [dict(r) for r in cur.fetchall()]
    return {"patient_id": patient_id, "window": window, "items": items}


@router.get("/alerts/active", tags=["alerts"])
def active_alerts():
    """Unacknowledged alerts raised in the last API_ALERT_ACTIVE_MINUTES:
    `sustained` (NEWS2 high for consecutive windows) and `immediate_spike`
    (single critical reading, e.g. SpO2 < 90)."""
    with get_connection(_dsn()) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT * FROM (
                    SELECT 'sustained'::text AS alert_type, id, patient_id,
                           window_start AS event_ts, score, risk_band,
                           components AS details, raised_at
                    FROM speed.clinical_alerts
                    WHERE NOT acknowledged AND raised_at >= now() - make_interval(mins => %s)
                  UNION ALL
                    SELECT 'immediate_spike'::text, id, patient_id, event_ts,
                           NULL::int, NULL::text,
                           jsonb_build_object('rule', rule, 'vital', vital, 'value', observed_value,
                                              'threshold', threshold, 'op', op),
                           raised_at
                    FROM speed.vitals_alerts
                    WHERE NOT acknowledged AND raised_at >= now() - make_interval(mins => %s)
                ) a
                ORDER BY raised_at DESC
                LIMIT 100
                """,
                (SETTINGS.api_alert_active_minutes, SETTINGS.api_alert_active_minutes),
            )
            return [dict(r) for r in cur.fetchall()]


@router.post("/alerts/{alert_id}/ack", tags=["alerts"])
def acknowledge_alert(
    alert_id: int,
    alert_type: Literal["sustained", "immediate_spike"] = "sustained",
    body: AckRequest = AckRequest(),
):
    table = "speed.clinical_alerts" if alert_type == "sustained" else "speed.vitals_alerts"
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"UPDATE {table} SET acknowledged = true, acknowledged_at = now() WHERE id = %s",  # noqa: S608
                (alert_id,),
            )
            if cur.rowcount == 0:
                raise HTTPException(status_code=404, detail=f"Unknown {alert_type} alert id {alert_id}")
    return {"id": alert_id, "alert_type": alert_type, "acknowledged": True}


@router.get("/reports/daily/{sim_day}", tags=["reports"])
def daily_report(sim_day: str):
    reports_dir = os.environ.get("REPORTS_DIR", SETTINGS.reports_dir)
    path = os.path.join(reports_dir, f"daily_risk_report_{sim_day}.html")
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail=f"No report generated yet for sim_day={sim_day}")
    return FileResponse(path, media_type="text/html")


# Versioned surface (documented in OpenAPI) + legacy un-prefixed paths kept
# so existing scripts/dashboards keep working (hidden from the schema).
app.include_router(router, prefix="/api/v1")
app.include_router(router, include_in_schema=False)


@app.get("/ward/status", response_model=WardLiveResponse, include_in_schema=False)
def ward_status_legacy() -> WardLiveResponse:
    return ward_live()


# --------------------------------------------------------------------------
# Alertmanager webhook receiver
# --------------------------------------------------------------------------


@app.post("/internal/alertmanager", tags=["ops"])
def alertmanager_webhook(payload: AlertmanagerWebhook):
    with get_connection(_dsn()) as conn:
        with conn.cursor() as cur:
            for alert in payload.alerts or [{}]:
                cur.execute(
                    """
                    INSERT INTO ops.alerts_received (source, alert_name, severity, status, labels, annotations, received_at)
                    VALUES (%s,%s,%s,%s,%s,%s, now())
                    """,
                    (
                        "alertmanager",
                        alert.get("labels", {}).get("alertname", "unknown"),
                        alert.get("labels", {}).get("severity"),
                        alert.get("status", payload.status or "firing"),
                        json.dumps(alert.get("labels", {})),
                        json.dumps(alert.get("annotations", {})),
                    ),
                )
    LOGGER.warning("alertmanager_webhook_received", stage="serving", alert_count=len(payload.alerts))
    return {"received": len(payload.alerts)}
