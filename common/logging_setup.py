"""
Structured JSON logging shared by every service in the pipeline.

Every producer, Spark job, Airflow task and the FastAPI app calls
`configure_logging(service_name)` once at startup and then uses the returned
structlog logger for everything. Log lines are:

  * emitted as single-line JSON to stdout (so `docker compose logs` and any
    log-shipper can parse them), and
  * mirrored to a per-service file at ``{LOGS_DIR}/{service}.jsonl`` so the
    demo can `grep` a trace_id across every stage.

The common keys every log line SHOULD carry (not all are always applicable):
    ts, level, service, stage, event, trace_id, patient_id, count, latency_ms

`trace_id` provides per-record correlation across stages: it is generated once
per vital reading in the producer, carried in the Kafka message, stored on the
Spark output row, included in any alert raised from that reading (log line,
DB row, Kafka message) and returned in API responses. Individual readings are
not logged per event (volume), so the log-level journey exists for alerting
readings; this is correlation, not distributed tracing.
"""

from __future__ import annotations

import logging
import os
import sys
import uuid
from pathlib import Path

import structlog


def new_trace_id() -> str:
    """Generate a new trace id (used by producers for each event)."""
    return uuid.uuid4().hex


def configure_logging(
    service: str, logs_dir: str | None = None, preserve_handlers: bool = False
) -> structlog.stdlib.BoundLogger:
    """
    Configure structlog for `service` and return a bound logger.

    Safe to call multiple times within a process (e.g. in tests); the
    underlying stdlib logging handlers are reset each time so we don't end up
    with duplicate log lines.

    `preserve_handlers=True` is for hosts that already own the root logger --
    Airflow tasks. Airflow replaces `sys.stdout` with a writer that logs via
    the root logger, so attaching a handler to `sys.stdout` recurses forever,
    and clearing the root handlers silently discards Airflow's own task log
    (including any traceback). In this mode existing handlers are kept, the
    stdout handler is skipped (Airflow already captures the records), and only
    the per-service JSON file handler is added.
    """
    logs_dir = logs_dir or os.environ.get("LOGS_DIR", "/data/logs")

    handlers: list[logging.Handler] = [] if preserve_handlers else [logging.StreamHandler(sys.stdout)]

    try:
        Path(logs_dir).mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(os.path.join(logs_dir, f"{service}.jsonl"), encoding="utf-8")
        handlers.append(file_handler)
    except OSError:
        # If the mount isn't available (e.g. running a quick unit test on the
        # host without /data), fall back to stdout-only rather than crashing.
        pass

    root_logger = logging.getLogger()
    if preserve_handlers:
        for handler in handlers:
            root_logger.addHandler(handler)
    else:
        root_logger.handlers = handlers
    root_logger.setLevel(logging.INFO)

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", utc=True, key="ts"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=False,
    )

    logger = structlog.get_logger(service=service)
    return logger
