"""
logger.py
---------
Shared structured logger for the property pipeline.
Writes human-readable lines to stdout and JSON records to logs/pipeline.jsonl.

Usage
-----
    from logger import log, flush_logs, build_logger

    log("STEP", "message", key=value, ...)   # main structured logger
    flush_logs()                              # write buffered JSON to disk

    # legacy helper used by src/main.py (ASSI2)
    log_obj, log_path = build_logger(__file__)
"""

from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import config

# ── internal buffer ────────────────────────────────────────────────────────────
_records: list[dict] = []
_LOG_PATH = os.path.join(config.LOG_DIR, "pipeline.jsonl")


# ── public API ─────────────────────────────────────────────────────────────────

def log(step: str, message: str, **kwargs: Any) -> None:
    """
    Emit one structured log record.

    Parameters
    ----------
    step    : Short tag for the pipeline stage, e.g. "READ", "JOIN", "WRITE".
    message : Human-readable description.
    **kwargs: Any extra key=value pairs to include in the JSON record.
    """
    ts = datetime.now(timezone.utc).isoformat()
    record = {"ts": ts, "step": step, "msg": message, **kwargs}
    _records.append(record)

    # human-readable stdout line
    extras = "  ".join(f"{k}={v}" for k, v in kwargs.items())
    line   = f"[{ts}] [{step:10s}] {message}"
    if extras:
        line += f"  |  {extras}"
    print(line, flush=True)


def flush_logs() -> None:
    """Append all buffered records to logs/pipeline.jsonl."""
    with open(_LOG_PATH, "a", encoding="utf-8") as fh:
        for rec in _records:
            fh.write(json.dumps(rec, default=str) + "\n")
    _records.clear()
    print(f"[logger] Logs flushed → {_LOG_PATH}", flush=True)


# ── legacy helper (used by src/main.py ASSI2) ──────────────────────────────────

def build_logger(script_path: str) -> tuple[logging.Logger, str]:
    """
    Build a standard Python logger that writes to logs/<script_name>.log.
    Returns (logger, log_file_path).

    This is the interface expected by the original ASSI2 main.py which calls:
        log, log_path = build_logger(__file__)
        log.info("message", extra={"step": "..."})
    """
    name     = Path(script_path).stem
    log_file = os.path.join(config.LOG_DIR, f"{name}.log")

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if not logger.handlers:
        # file handler — full detail
        fh = logging.FileHandler(log_file, encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(message)s"
        ))
        logger.addHandler(fh)

        # stdout handler — INFO+
        sh = logging.StreamHandler(sys.stdout)
        sh.setLevel(logging.INFO)
        sh.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        logger.addHandler(sh)

    return logger, log_file
