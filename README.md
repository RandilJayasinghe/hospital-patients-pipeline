# Hospital Patient Vital Signs Monitoring — Lambda Architecture Pipeline

**EC8203 Applied Big Data Engineering — Mini Project · Use Case 2**

A hospital ward wants continuous, near-real-time monitoring of bedside vitals, correlated
daily with lab results the pathology lab uploads once a day.

> Business question: *Which patients show concerning vital-sign trends right now, and how
> do yesterday's lab results change the risk picture for those patients going forward?*

Design rationale (Lambda vs Kappa, stack justification, phase-by-phase build/verification
plan) is in [`PLAN.md`](PLAN.md); the full written report is
[`docs/REPORT.md`](docs/REPORT.md); diagrams are in [`docs/architecture.md`](docs/architecture.md).

---

## 1. Architecture at a glance

**Lambda architecture.** A continuous vitals stream and a once-a-day lab file are processed
on two paths that share one clinical scoring module and are merged in a serving layer.

```
 bedside monitors ──► Kafka `vitals_raw` (3 partitions, key=patient_id)
 (vitals_producer)         │
                           ├─► SPEED LAYER  spark-stream (stream_job.py, 4 queries)
                           │     Q1 malformed ─► `vitals_dlq`
                           │     Q2 clean+dedup(watermark) ─► speed.vitals_clean, `vitals_clean`
                           │     Q3 1-min/30-s windows ─► NEWS2 ─► speed.patient_vitals_1m,
                           │        speed.patient_risk_now, speed.clinical_alerts (sustained)
                           │     Q4 immediate spike (SpO2<90, HR>130 …, 5-s trigger)
                           │        ─► speed.vitals_alerts, `clinical_alerts`
                           │
                           └─► spark-archiver (raw_archiver.py) ─► Parquet MASTER DATASET
                                                                   data/master/vitals/dt=/hour=
 pathology lab ──► data/landing/labs/labs_{sim_day}.csv (+.done, manifest)  and Kafka `lab_batch`
 (lab_batch_producer)          │
                               ▼
                 BATCH LAYER  Airflow DAG daily_patient_risk ─► batch_job.py (Spark)
                   recompute from the Parquet master dataset, clean/quarantine labs,
                   THE JOIN (24h vitals × that day's labs) ─► batch.patient_lab_results,
                   batch.patient_daily_vitals, batch.patient_risk_daily
                               │
   speed.* ("right now") ──────┴────► SERVING LAYER  serving.patient_360 (query-time view)
                                         ├─ FastAPI /api/v1/…   ├─ Grafana ×2   └─ daily HTML/CSV report
```

`common/clinical.py` (NEWS2-style scoring, validation, spike rules, lab flagging) is imported
by **both** the speed and batch jobs — one implementation, two execution contexts, which is
this project's answer to Lambda's "two codebases" criticism.

### Why Lambda and not Kappa (short version — full argument in the report §2)

| Requirement | Consequence |
|---|---|
| Vitals every 2 s per patient; alerts must reach a clinician in seconds | Speed layer with its own short-trigger alert path |
| Labs arrive as **one bulk file per day**, and are amended/re-issued | Natural batch source; forcing it through a log would be artificial |
| Changing the risk formula must restate history | Immutable Parquet master dataset + idempotent per-day recompute, not bounded Kafka retention |
| A wrong number in a clinical record is worse than a late one | Batch layer is authoritative and corrects speed-layer approximations |

Rejected: **Kappa** (single streaming code path; simpler ops, but it needs Kafka retention
sized to the whole admission history and a retraction/upsert protocol for amended labs).

## 2. Technology stack

| Layer | Choice |
|---|---|
| Ingestion | Apache Kafka 3.9 (KRaft) — topics `vitals_raw` (3p), `lab_batch`, `vitals_clean`, `clinical_alerts`, `vitals_dlq` |
| Stream processing | Spark Structured Streaming (PySpark 3.5) |
| Batch processing | Spark (batch mode) — same codebase and jars |
| Orchestration | Apache Airflow 2.10 (LocalExecutor) |
| Master dataset | Parquet on a bind-mounted volume (stands in for HDFS/S3) |
| Serving store | PostgreSQL 16 (`speed`, `batch`, `serving`, `ops`, `reference` schemas) |
| Serving API | FastAPI + Uvicorn |
| Dashboards | Grafana 11 (provisioned from JSON) |
| Metrics / alerting | Prometheus + Alertmanager (webhook → API → `ops.alerts_received`) |
| Logging | structlog, JSON lines with `trace_id` |
| Reproducibility | Docker Compose |

## 3. Simulated clock

```
1 simulated day        = 5 real minutes   (SIM_DAY_SECONDS=300)
Vitals emitted         every 2 real seconds per patient (18 patients ≈ 9 events/s)
Lab file dropped       at the end of each simulated day (≈ every 5 real minutes)
Airflow DAG schedule   */5 * * * *  (processes the simulated day that just completed)
Anomaly episodes       ≈ 26 % of patients per simulated day (ANOMALY_PROBABILITY=0.002 per tick)
```

Two clocks, deliberately: each vital's `event_ts` is **real wall-clock time** (Spark windows
and the 45-s watermark only make sense against a timeline that advances at the rate events
are produced), while every event and lab row also carries a `sim_day` label that drives the
daily-batch cadence (Parquet `dt=` partitions, lab file names, which day Airflow processes).
See `ingestion/sim_clock.py`.

## 4. Prerequisites (Windows 10/11)

- **Docker Desktop, WSL2 backend.** Allocate **≥ 8 GB RAM and ≥ 4 CPUs** to Docker
  (Settings → Resources; or `memory=8GB` in `%UserProfile%\.wslconfig`, then `wsl --shutdown`).
  The full stack (3 JVM-based Spark/Airflow-batch processes + Kafka + Airflow + Grafana …)
  is OOM-killed under ~6 GB. If your machine has ≤ 8 GB total, use the staged start in §5.
- Git with the repo's `.gitattributes` honoured (LF endings; CRLF breaks Linux entrypoints).
- Nothing else on the host: every service runs in a container on Python 3.11. Host Python is
  used only for the unit tests (`pip install -r requirements-dev.txt`).

## 5. Running it

```bash
cp .env.example .env            # Windows PowerShell: Copy-Item .env.example .env
docker compose up -d --build    # first build downloads Spark jars: ~10-15 min
docker compose ps               # all services running/healthy within ~2 min
```

**Low-memory / staged start** (8 GB machines): start the streaming path first, let it
accumulate data for ≥ 1 simulated day, then add batch orchestration:

```bash
docker compose up -d kafka kafka-init postgres vitals-producer lab-producer \
    spark-archiver spark-stream api prometheus alertmanager grafana
# … after ≥ 6-10 minutes …
docker compose stop spark-stream           # optional: free ~1 GB while the batch job runs
docker compose up -d airflow-init airflow-webserver airflow-scheduler
```

### Ports

| Port | Service |
|---|---|
| 8000 | Serving API (`/docs` = interactive OpenAPI) |
| 3000 | Grafana (`admin` / `admin`) — dashboards auto-provisioned |
| 8081 | Airflow UI (`admin` / `admin`) |
| 9090 / 9093 | Prometheus / Alertmanager |
| 8080 | Kafka UI |
| 29092 | Kafka (host listener) |
| 5432 | PostgreSQL (`postgres` / `postgres`, db `vitals`) |
| 8001 / 8002 | Vitals / lab producer Prometheus metrics |
| 4040 | Spark UI + Prometheus metrics (spark-stream) |

### Tear down

```bash
docker compose down        # keep volumes
docker compose down -v     # wipe Kafka/Postgres/Grafana/Airflow state (true cold start)
rm -rf data/               # also wipe Parquet master data, checkpoints, reports, logs
```

> Existing Postgres volumes are **not** re-migrated by `docker-entrypoint-initdb.d`. After
> pulling schema changes on a non-fresh volume, re-apply idempotently:
> `for f in sql/00[1-5]*.sql; do docker compose exec -T postgres psql -U postgres -d vitals -f - < $f; done`

## 6. API

Versioned at `/api/v1` (un-prefixed legacy paths still work). Interactive docs: <http://localhost:8000/docs>.

| Endpoint | Purpose |
|---|---|
| `GET /api/v1/ward/live` | Real-time ward figures: active beds, patients with abnormal vitals, ward-average vitals, band counts, open alerts |
| `GET /api/v1/patients/{id}/risk` | **The merged answer**: realtime trend **and** last day's lab-adjusted risk, reason codes, abnormal labs, day-over-day delta |
| `GET /api/v1/patients`, `/patients/{id}/vitals?window=30m` | Roster with current band; recent readings (with `trace_id`) |
| `GET /api/v1/alerts/active` | Unacknowledged alerts: `sustained` (NEWS2 high ≥ 2 windows) + `immediate_spike` |
| `POST /api/v1/alerts/{id}/ack?alert_type=…` | Acknowledge an alert |
| `GET /api/v1/reports/daily/{sim_day}` | The consolidated daily report (HTML) |
| `GET /health` | Liveness + **pipeline lag** (`speed_layer_lag_seconds`, stream heartbeat age); `degraded` if lag > `STREAM_STALLED_SECONDS` |
| `GET /ready`, `GET /metrics` | Readiness; Prometheus exposition (HTTP metrics + pipeline/clinical gauges) |

```bash
curl -s localhost:8000/api/v1/ward/live | python -m json.tool
curl -s localhost:8000/api/v1/patients/P003/risk | python -m json.tool
curl -s localhost:8000/api/v1/alerts/active | python -m json.tool
curl -s localhost:8000/health
```

## 7. Warehouse tables

| Layer | Table | Notes |
|---|---|---|
| Speed | `speed.vitals_clean`, `speed.patient_vitals_1m`, `speed.patient_risk_now`, `speed.clinical_alerts` | idempotent upserts |
| Speed | `speed.vitals_alerts` (alias `public.speed_vitals_alerts`) | immediate threshold alerts, unique on `(event_id, rule)` |
| Batch | `batch.patient_lab_results` (alias `public.batch_lab_results`) | cleaned, reference-range-flagged labs |
| Batch | `batch.patient_daily_vitals`, `batch.patient_risk_daily` (alias `public.daily_patient_risk_summary`) | delete-then-insert per `sim_day` in one transaction |
| Serving | `serving.patient_360` | plain **view**: latest speed window ⨝ latest batch day ⨝ abnormal labs ⨝ spike count |
| Ops | `ops.pipeline_heartbeat`, `ops.data_quality`, `ops.batch_runs`, `ops.alerts_received` | health & audit |

## 8. Observability

- **Structured logs** — every service emits single-line JSON (`ts, level, service, stage, event, trace_id, patient_id …`) to stdout and `data/logs/<service>.jsonl`. A reading's `trace_id` travels in the Kafka message, `speed.vitals_clean`, API responses and — for readings that raise an alert — the `spark-stream` log line and `speed.vitals_alerts`. Ordinary readings are not logged individually (volume). Example: `select trace_id from speed.vitals_alerts limit 1;` then `grep -h <trace_id> data/logs/*.jsonl`.
- **Metrics** — producers (`:8001/:8002`), Spark (`:4040`), API `/metrics` (HTTP latency/error rate + `ops_*`, `speed_layer_lag_seconds`, `ward_*` gauges).
- **Alert rules** (`observability/alerts.yml`), routed Prometheus → Alertmanager → `POST /internal/alertmanager` → `ops.alerts_received`:

| Alert | Family | Fires when |
|---|---|---|
| `IngestionSilence` | health | no vitals event acked by Kafka for > 30 s (or producer target missing) |
| `CriticalPatientDeterioration` | clinical | a patient's speed-layer NEWS2 band is `high` for ≥ 30 s (label carries patient/ward) |
| `CriticalVitalsSpike` | clinical | ≥ 1 immediate critical reading alert in the last minute |
| `PipelineComponentStalled`, `ServingDataStale` | health | heartbeat > 2 min / newest window > 3 min old |
| `BatchLate`, `LabFileMissing`, `DataQualityFailed` | health | no batch success in 12 min / no lab file in 1.5 sim-days / critical DQ rule failed |
| `ApiErrorRateHigh`, `LabKafkaPublishErrors` | health | 5xx > 5 % / lab rows failed Kafka delivery |

- **Dashboards** (auto-provisioned, `http://localhost:3000`): *Ward Clinical Dashboard* (`ward-clinical`) and *Pipeline Health Dashboard* (`pipeline-health`).
- **Airflow-native health**: `pipeline_health` DAG every 2 min (heartbeat + master-dataset freshness).

## 9. Tests

```bash
pip install -r requirements-dev.txt
pytest tests/ -v
```

Pure-Python (no Docker/Spark): clinical scoring and spike rules, alert cool-down, lab
flagging incl. electrolytes, simulation logic, lab publisher, batch join/DQ transforms, plus
integrity tests for the roster, topics, alert rules, SQL idempotency, dashboard layout and
API surface. Docker-level verification scripts live in `scripts/`
(`check_partitioning.py`, `sample_topic.py`, `verify_speed_vs_raw.py`,
`verify_alerts.py --source both`, `measure_latency.py`).

## 10. Repository layout

```
config/        settings.py (single source of config truth), patients.csv (18 patients, 4 wards), thresholds.yml
common/        logging, DB helpers, wire schemas, clinical scoring (imported by speed AND batch)
ingestion/     the two simulated sources: vitals_producer.py (stream), lab_batch_producer.py (daily), sim_clock.py
processing/    Spark jobs: stream_job.py (speed), raw_archiver.py (master dataset), batch_job.py (batch)
orchestration/ Airflow DAGs: daily_patient_risk, pipeline_health
serving/       api.py (FastAPI), report_builder.py (daily report)
observability/ prometheus.yml, alerts.yml, alertmanager.yml, grafana provisioning + dashboards
sql/           idempotent Postgres DDL, run on first boot
scripts/       verification helpers
tests/         pytest suite
docs/          REPORT.md (+ generated REPORT.docx / REPORT.pdf), architecture.md, report/ (OUTLINE.md, images/)
data/          gitignored runtime data (landing files, Parquet, checkpoints, logs, reports)
```

Regenerate the Word/PDF report after editing `docs/REPORT.md`: `pip install pypandoc_binary python-docx pymupdf && python scripts/build_report.py` (needs Chrome or Edge for the PDF).

## 11. Troubleshooting

- **Container restarts with `exec format error`** → CRLF endings; re-checkout with `.gitattributes` in effect.
- **Exit code 137 / OOM** → raise Docker memory (§4) or use the staged start (§5).
- **`bind: An attempt was made to access a socket in a way forbidden`** → Windows reserved that port (`netsh interface ipv4 show excludedportrange protocol=tcp`). Set `GRAFANA_HOST_PORT` (e.g. 3300) in `.env`.
- **Port in use** → `netstat -ano | findstr :<port>`, or change the host-side mapping in `docker-compose.yml`.
- **`role "postgres" does not exist` / stale schema** → volume predates the current `.env`/SQL; `docker compose down -v` (wipes data).
- **Grafana shows "No data" right after start** → the ward panels need ~1 min of stream output; batch panels need the first DAG run (~5-6 min).
- **Airflow `wait_for_lab_file` retries** → expected until the lab producer finishes its first simulated day.

## 12. Assumptions & simplifications

- Parquet on a bind mount stands in for HDFS/S3. Kafka is a single broker (RF=1). Spark runs in local mode; Airflow uses `LocalExecutor`.
- Latency is **seconds, not sub-second**: Structured Streaming is micro-batch. The immediate-alert query uses a 5 s trigger; the windowed/sustained path uses 30 s.
- Sustained-alert streak state lives in the Spark driver's memory (not checkpointed); a restart re-accumulates two windows before it can re-alert. Postgres writes are idempotent so aggregates stay correct.
- No PHI security/encryption/audit trail — data is simulated; a real deployment needs all three.
- The clinical score is a simplified NEWS2 (no respiration rate, consciousness or supplemental-O₂ inputs); it is illustrative, not a validated clinical tool.
- Simulated-time compression: 1 simulated day = 5 real minutes (§3).
