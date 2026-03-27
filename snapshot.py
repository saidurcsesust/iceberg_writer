"""
snapshot.py
-----------
Helpers for Iceberg maintenance operations.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Tuple

import config
from logger import log


def _default_cutoff(days: int = config.MAINTENANCE_RETENTION_DAYS) -> str:
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime(
        "%Y-%m-%d %H:%M:%S"
    )


def _format_cutoff(older_than: str | datetime | None) -> str:
    if older_than is None:
        return _default_cutoff()
    if isinstance(older_than, datetime):
        ts = older_than
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        else:
            ts = ts.astimezone(timezone.utc)
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return str(older_than)


def _resolve_table(table: str, catalog: str | None) -> Tuple[str, str]:
    """Return (catalog, "db.table") from "db.table" or "catalog.db.table"."""
    parts = table.split(".")
    if len(parts) == 3:
        cat, db, tbl = parts
        return cat, f"{db}.{tbl}"
    if len(parts) == 2:
        cat = catalog or config.ICEBERG_CATALOG
        return cat, table
    raise ValueError(
        "Table must be 'db.table' or 'catalog.db.table', got: {table}"
    )


def _ensure_maintenance_properties(
    spark,
    *,
    catalog: str,
    table_ref: str,
    retention_days: int,
) -> None:
    retention_ms = retention_days * 24 * 60 * 60 * 1000
    full_table_name = f"{catalog}.{table_ref}"
    spark.sql(
        f"""
        ALTER TABLE {full_table_name} SET TBLPROPERTIES (
          'history.expire.max-snapshot-age-ms' = '{retention_ms}',
          'write.metadata.delete-after-commit.enabled' = '{config.MAINTENANCE_METADATA_DELETE_AFTER_COMMIT}',
          'write.metadata.previous-versions-max' = '{config.MAINTENANCE_METADATA_PREVIOUS_VERSIONS_MAX}'
        )
        """
    )


def expire_snapshots(
    spark,
    table: str,
    *,
    catalog: str | None = None,
    older_than: str | datetime | None = None,
    retain_last: int = 1,
) -> None:
    cat, table_ref = _resolve_table(table, catalog)
    ts = _format_cutoff(older_than)

    if not spark.catalog.tableExists(f"{cat}.{table_ref}"):
        log("MAINT", "Table does not exist; skipping expire_snapshots", table=table_ref, catalog=cat)
        return

    # _ensure_maintenance_properties(
    #     spark,
    #     catalog=cat,
    #     table_ref=table_ref,
    #     retention_days=config.MAINTENANCE_RETENTION_DAYS,
    # )
    log("MAINT", "Running expire_snapshots", table=table_ref, catalog=cat, older_than=ts)
    try:
        spark.sql(
            f"""
            CALL {cat}.system.expire_snapshots(
              table => '{table_ref}',
              older_than => TIMESTAMP '{ts}'
            )
            """
        ).show(truncate=True)
        log("MAINT", "expire_snapshots done", table=table_ref, catalog=cat)
    except Exception as exc:
        log("MAINT", f"expire_snapshots skipped: {exc}", table=table_ref, catalog=cat)


def remove_orphan_files(
    spark,
    table: str,
    *,
    catalog: str | None = None,
    older_than: str | datetime | None = None,
) -> None:
    cat, table_ref = _resolve_table(table, catalog)
    ts = _format_cutoff(older_than)

    if not spark.catalog.tableExists(f"{cat}.{table_ref}"):
        log("MAINT", "Table does not exist; skipping remove_orphan_files", table=table_ref, catalog=cat)
        return

    log("MAINT", "Running remove_orphan_files", table=table_ref, catalog=cat, older_than=ts)
    try:
        spark.sql(
            f"""
            CALL {cat}.system.remove_orphan_files(
              table => '{table_ref}',
              older_than => TIMESTAMP '{ts}'
            )
            """
        ).show(truncate=True)
        log("MAINT", "remove_orphan_files done", table=table_ref, catalog=cat)
    except Exception as exc:
        log("MAINT", f"remove_orphan_files skipped: {exc}", table=table_ref, catalog=cat)
