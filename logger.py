"""
logger.py
---------
Structured JSON logger for the PySpark property pipeline.

All log entries are accumulated in memory during the pipeline run
and flushed to a single JSON file at the end via flush_logs().

Log file location: logs/<YYMMDD>/<script>_<YYMMDD>_<HHMMSS>.json
"""

import json
import os
from datetime import datetime

import config

# ---------------------------------------------------------------------------
# Internal state
# ---------------------------------------------------------------------------

def _log_path():
    """
    Build and return the log file path.
    Creates the date-stamped directory if it does not already exist.

    Returns
    -------
    str: logs/<YYMMDD>/<script>_<YYMMDD>_<HHMMSS>.json
    """
    now     = datetime.now()
    date    = now.strftime("%y%m%d")
    time    = now.strftime("%H%M%S")
    log_dir = os.path.join(config.LOG_BASE_DIR, date)
    os.makedirs(log_dir, exist_ok=True)
    return os.path.join(log_dir, f"{config.SCRIPT_NAME}_{date}_{time}.json")


LOG_FILE   = _log_path()
_log_lines = []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def log(step, message, **extra):
    """
    Append a structured log entry to memory and print to stdout.

    Parameters
    ----------
    step    : Pipeline step label (e.g. 'READ', 'JOIN', 'WRITE').
    message : Human-readable description of the event.
    **extra : Any additional key/value context to include in the entry.
    """
    entry = {
        "timestamp": datetime.now().isoformat(),
        "step":      step,
        "message":   message,
        **extra,
    }
    _log_lines.append(entry)
    print(
        f"[{entry['timestamp']}] [{step}] {message}"
        + (f" | {extra}" if extra else "")
    )


def flush_logs():
    """
    Write all accumulated log entries to the JSON log file.
    Should be called once at the very end of the pipeline.
    """
    with open(LOG_FILE, "w") as fh:
        json.dump(_log_lines, fh, indent=2, default=str)
    print(f"\nLogs written -> {LOG_FILE}")