"""
main.py
-------
Entry point for the PySpark property search pipeline.

This file contains only orchestration logic. All business logic
lives in the utils/ package and logger.py.

Pipeline steps
--------------
  1.  Read property.json and search.json
  2.  Extract required fields from details
  3.  Extract required fields from search
  4.  Run data quality checks on search
  5.  Drop rows with missing source_id
  6.  Deduplicate details on source_id
  7.  Inner join  -> matched_details
      Anti join   -> unmatched_details
  8.  Build standardized final output
  9.  Write final_output and unmatched_details to disk
  10. Write validation_report.txt
  11. Write structured JSON logs
"""

# ---------------------------------------------------------------------------
# Standard library
# ---------------------------------------------------------------------------
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Project config
# ---------------------------------------------------------------------------
import config

# ---------------------------------------------------------------------------
# Logger
# ---------------------------------------------------------------------------
from logger import log, flush_logs

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
from utils.spark_session import create_spark_session
from utils.readers       import read_json
from utils.transforms    import extract_details_fields, extract_search_fields, build_final_output
from utils.cleaning      import drop_missing_source_id, deduplicate
from utils.joins         import build_matched_unmatched
from utils.quality       import search_quality_checks



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    """Orchestrate all pipeline steps end-to-end."""

    log("INIT", "Pipeline starting", app=config.APP_NAME)

    # 1. SparkSession
    spark = create_spark_session(config.APP_NAME)
    log("INIT", "SparkSession created")

    # 2. Read raw data
    raw_details = read_json(spark, config.INPUT_DETAILS_FILE, "details")
    raw_search  = read_json(spark, config.INPUT_SEARCH_FILE,  "search")

    # 3. Extract fields
    details_ext = extract_details_fields(raw_details)
    search_ext  = extract_search_fields(raw_search)

    # 4. Drop rows with missing source_id
    details_clean,_ = drop_missing_source_id(details_ext)

    # 5. Deduplicate details
    details_dedup, _, _ = deduplicate(details_clean, "source_id")

    # 7. Join -> matched / unmatched
    matched_details, unmatched_details = build_matched_unmatched(
        details_dedup, search_ext
    )

    # 8. Build final output
    final_output,_= build_final_output(matched_details)
    print("Final output schema:")
    final_output.printSchema()
    # 19. Flush logs
    log("DONE", "Pipeline complete")
    flush_logs()
    spark.stop()


if __name__ == "__main__":
    main()