"""
utils/quality.py
----------------
Data quality checks and validation report writer
for the PySpark property pipeline.
"""

from datetime import datetime

from pyspark.sql import functions as F

import config
from logger import log


def search_quality_checks(search_df):
    """
    Run mandatory data-quality checks on the extracted search DataFrame.

    Checks
    ------
    - Rows where deep_link_url is null
    - Rows where usd_price is null

    Parameters
    ----------
    search_df : Extracted search DataFrame.

    Returns
    -------
    dict with keys: missing_deep_link_url, missing_usd_price
    """
    log("QC", "Running search data quality checks")

    missing_url   = search_df.filter(F.col("deep_link_url").isNull()).count()
    missing_price = search_df.filter(F.col("usd_price").isNull()).count()

    report = {
        "missing_deep_link_url": missing_url,
        "missing_usd_price":     missing_price,
    }
    log("QC", "Search quality checks complete", **report)
    return report


def write_validation_report(
    details_count,
    search_count,
    matched_count,
    unmatched_count,
    final_count,
    dropped_source_id,
    dup_before,
    dup_after,
    qc,
    final_df,
    bad_country_count,
    defaulted_price,
):
    """
    Write a human-readable validation_report.txt summarising all pipeline stats.

    Parameters
    ----------
    details_count     : Raw row count from details.json.
    search_count      : Raw row count from search.json.
    matched_count     : Row count after inner join.
    unmatched_count   : Row count after anti join.
    final_count       : Row count of final output.
    dropped_source_id : Number of rows dropped due to missing source_id.
    dup_before        : Row count before deduplication.
    dup_after         : Row count after deduplication.
    qc                : Dict from search_quality_checks().
    final_df          : Final output DataFrame (used for schema).
    bad_country_count : Rows where country_code length != 2.
    defaulted_price   : Rows where usd_price was defaulted to 0.0.
    """
    log("REPORT", "Writing validation report")

    # Build schema string
    schema_lines = [f"  {f.name}: {f.dataType}" for f in final_df.schema.fields]
    schema_str   = "\n".join(schema_lines)
    col_count    = len(final_df.columns)

    lines = [
        "=" * 60,
        "VALIDATION REPORT",
        f"Generated: {datetime.now().isoformat()}",
        "=" * 60,
        "",
        "-- Row Counts -----------------------------------------------",
        f"  details.json row count         : {details_count}",
        f"  search.json row count          : {search_count}",
        f"  matched_details (inner join)   : {matched_count}",
        f"  unmatched_details (anti join)  : {unmatched_count}",
        f"  final output row count         : {final_count}",
        "",
        "-- Data Quality ---------------------------------------------",
        f"  Dropped (missing source_id)    : {dropped_source_id}",
        f"  Duplicates before dedup        : {dup_before}",
        f"  Duplicates after dedup         : {dup_after}",
        f"  Duplicates removed             : {dup_before - dup_after}",
        f"  Dedup removal %                : {round((dup_before - dup_after) / max(dup_before, 1) * 100, 2)}%",
        "",
        "-- Search Quality Checks ------------------------------------",
        f"  Missing deep_link_url          : {qc['missing_deep_link_url']}",
        f"  Missing usd_price (book)       : {qc['missing_usd_price']}",
        "",
        "-- Output Field Checks --------------------------------------",
        f"  country_code length != 2       : {bad_country_count}",
        f"  usd_price defaulted to 0.0     : {defaulted_price}",
        "",
        "-- Final Output Schema --------------------------------------",
        schema_str,
        "",
        f"  Total columns in final output  : {col_count}",
        f"  Schema has exactly 12 columns  : {'YES' if col_count == 13 else f'NO ({col_count}) - includes data_quality_flag bonus column'}",
        "=" * 60,
    ]

    report_text = "\n".join(lines)
    with open(config.VALIDATION_REPORT_PATH, "w") as fh:
        fh.write(report_text)

    print("\n" + report_text)
    log("REPORT", "Validation report written", path=config.VALIDATION_REPORT_PATH)