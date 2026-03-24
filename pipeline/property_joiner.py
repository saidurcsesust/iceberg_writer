"""
pipeline/property_joiner.py
---------------------------
Joins rental_property and property_reviews Iceberg tables,
then generates one complete JSON file per property.

Tables     : local.property_db.rental_property
             local.property_db.property_reviews
Join key   : rental_property.id  ==  property_reviews.gen_id
Output     : data/final_data/GEN-<id>.json
Engine     : Spark RDD  →  Row  →  dict  →  JSON file
Dedup      : dropDuplicates on (gen_id, review_id) before aggregation
Maintenance: expire_snapshots + remove_orphan_files on both tables

Output shape
------------
{
  "id":               "GEN-3005942",
  "feed_provider_id": "3005942",
  "property_name":    "Anvil Barn...",
  "property_slug":    "anvil-barn...",
  "country_code":     "GB",
  "currency":         "GBP",
  "usd_price":        0.0,
  "star_rating":      4.0,
  "review_score":     9.0,
  "commission":       null,
  "meal_plan":        null,
  "published":        true,
  "data_quality_flag":"GOOD",
  "total_reviews":    3,
  "reviews": [
    {
      "review_id": 5068520533, "review_date": "2024-08-16",
      "score": 10.0, "summary": "A lovely holiday", ...
    }
  ]
}
"""

from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
import os, sys, json

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from spark_utils import build_spark
from logger import log, flush_logs
from pipeline.transform import (
    JOINED_RENTAL_GROUP_COLUMNS,
    aggregate_reviews_per_property,
    deduplicate_joined_rental_reviews,
)
from snapshot import expire_snapshots, remove_orphan_files

_OUTPUT_DIR = config.OUTPUT_FINAL_JSON
os.makedirs(_OUTPUT_DIR, exist_ok=True)


# -----------------------------------------------------------------------------
# 1.  Load both Iceberg tables
# -----------------------------------------------------------------------------
def load_tables(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df_rental  = spark.table(config.ICEBERG_RENTALS_TABLE)
    df_reviews = spark.table(config.ICEBERG_REVIEWS_TABLE)
    log("READ", "Tables loaded",
        rental_rows=df_rental.count(), reviews_rows=df_reviews.count())
    return df_rental, df_reviews


# --------------------------------------------------- --------------------------
# 2.  Join
#     rental_property  INNER JOIN  property_reviews
#     Rename shared columns on the reviews side to avoid ambiguity.
# -----------------------------------------------------------------------------
def join_tables(df_rental: DataFrame, df_reviews: DataFrame) -> DataFrame:
    shared = {
        "property_name", "property_slug", "country_code", "currency",
        "star_rating", "review_score", "published", "data_quality_flag",
    }
    df_rev = df_reviews
    for col in shared:
        df_rev = df_rev.withColumnRenamed(col, f"r_{col}")

    df_joined = df_rental.join(
        df_rev,
        df_rental["id"] == df_rev["gen_id"],
        how="inner",
    ).drop("gen_id", "source_id_r")

    log("JOIN", "Join complete", rows=df_joined.count())
    return df_joined


# -----------------------------------------------------------------------------
# 3.  RDD  -->  Row  -->  dict  -->  JSON file
# -----------------------------------------------------------------------------
def _joined_row_to_dict(row) -> dict:
    reviews = [
        {
            "review_id":               r.review_id,
            "review_date":             r.review_date,
            "review_year":             r.review_year,
            "score":                   r.review_individual_score,
            "language":                r.review_language,
            "summary":                 r.review_summary,
            "positive":                r.review_positive,
            "negative":                r.review_negative,
            "reviewer_name":           r.reviewer_name,
            "reviewer_country":        r.reviewer_country,
            "reviewer_travel_purpose": r.reviewer_travel_purpose,
            "reviewer_type":           r.reviewer_type,
        }
        for r in (row.reviews or [])
    ]

    return {
        "id":                row.id,
        "feed_provider_id":  row.feed_provider_id,
        "property_name":     row.property_name,
        "property_slug":     row.property_slug,
        "country_code":      row.country_code,
        "currency":          row.currency,
        "usd_price":         row.usd_price,
        "star_rating":       row.star_rating,
        "review_score":      row.review_score,
        "commission":        row.commission,
        "meal_plan":         row.meal_plan,
        "published":         row.published,
        "data_quality_flag": row.data_quality_flag,
        "total_reviews":     len(reviews),
        "reviews":           reviews,
    }


def _row_to_file(row) -> str:
    doc = _joined_row_to_dict(row)
    safe_id = str(row.id).replace("/", "_").replace("\\", "_")
    out_path = os.path.join(_OUTPUT_DIR, f"{safe_id}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2, default=str)
    return out_path


def generate_json_files(df_agg: DataFrame) -> list[str]:
    log("WRITE", "Generating JSON files", output_dir=_OUTPUT_DIR)
    written = df_agg.rdd.map(_row_to_file).collect()
    log("WRITE", "JSON generation complete", files_written=len(written))
    return written


# -----------------------------------------------------------------------------
# 4.  Maintenance — both tables
# -----------------------------------------------------------------------------
def run_maintenance(spark: SparkSession) -> None:
    log("MAINT", "Running maintenance on both tables")
    db = config.ICEBERG_DATABASE

    for tbl in ("rental_property", "property_reviews"):
        table_ref = f"{db}.{tbl}"
        expire_snapshots(spark, table_ref)
        remove_orphan_files(spark, table_ref)

    log("MAINT", "Maintenance complete")


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
def run() -> None:
    log("INIT", "PropertyJoiner starting")

    spark = build_spark("PropertyJoiner")

    df_rental, df_reviews   = load_tables(spark)
    df_joined               = join_tables(df_rental, df_reviews)
    df_joined_dedup         = deduplicate_joined_rental_reviews(df_joined)
    df_agg                  = aggregate_reviews_per_property(
        df_joined_dedup, JOINED_RENTAL_GROUP_COLUMNS, "id"
    )
    written                 = generate_json_files(df_agg)

    run_maintenance(spark)

    log("DONE", "PropertyJoiner complete", files_written=len(written))
    flush_logs()
    spark.stop()
