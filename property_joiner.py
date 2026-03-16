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
from pyspark.sql import functions as F
import os, sys, json

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from logger import log, flush_logs

_OUTPUT_DIR = config.OUTPUT_FINAL_JSON
os.makedirs(_OUTPUT_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# 1.  SparkSession
# ══════════════════════════════════════════════════════════════════════════════
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PropertyJoiner")
        .master("local[*]")
        .config("spark.jars.packages", config.ICEBERG_JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}.warehouse",
                config.ICEBERG_WAREHOUSE)
        .config("spark.sql.iceberg.write.format.default", "parquet")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[PropertyJoiner] SparkSession ready")
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Load both Iceberg tables
# ══════════════════════════════════════════════════════════════════════════════
def load_tables(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df_rental  = spark.table(config.ICEBERG_PROPERTY_TABLE)
    df_reviews = spark.table(config.ICEBERG_REVIEWS_TABLE)
    log("READ", "Tables loaded",
        rental_rows=df_rental.count(), reviews_rows=df_reviews.count())
    return df_rental, df_reviews


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Deduplicate reviews
# ══════════════════════════════════════════════════════════════════════════════
def deduplicate_reviews(df: DataFrame) -> DataFrame:
    before   = df.count()
    df_dedup = df.dropDuplicates(["gen_id", "review_id"])
    dropped  = before - df_dedup.count()
    if dropped:
        log("DEDUP", "Removed duplicate review rows", dropped=dropped)
    else:
        log("DEDUP", "No duplicates found", rows=df_dedup.count())
    return df_dedup


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Join
#     rental_property  LEFT JOIN  property_reviews
#     Rename shared columns on the reviews side to avoid ambiguity.
# ══════════════════════════════════════════════════════════════════════════════
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
        how="left",
    ).drop("gen_id", "source_id_r")

    log("JOIN", "Join complete", rows=df_joined.count())
    return df_joined


# ══════════════════════════════════════════════════════════════════════════════
# 5.  Aggregate — one row per property, reviews nested
# ══════════════════════════════════════════════════════════════════════════════
def aggregate_per_property(df: DataFrame) -> DataFrame:
    df_struct = df.withColumn(
        "review_struct",
        F.when(
            F.col("review_id").isNotNull(),
            F.struct(
                F.col("review_id"), F.col("review_date"), F.col("review_year"),
                F.col("review_individual_score"), F.col("review_language"),
                F.col("review_summary"), F.col("review_positive"),
                F.col("review_negative"), F.col("reviewer_name"),
                F.col("reviewer_country"), F.col("reviewer_travel_purpose"),
                F.col("reviewer_type"),
            ),
        ).otherwise(F.lit(None)),
    )

    df_agg = (
        df_struct
        .groupBy(
            "id", "feed_provider_id", "property_name", "property_slug",
            "country_code", "currency", "usd_price", "star_rating",
            "review_score", "commission", "meal_plan", "published",
            "data_quality_flag",
        )
        .agg(
            F.collect_list(
                F.when(F.col("review_struct").isNotNull(), F.col("review_struct"))
            ).alias("reviews")
        )
        .orderBy("id")
    )

    log("AGG", "Aggregated per property", distinct_properties=df_agg.count())
    return df_agg


# ══════════════════════════════════════════════════════════════════════════════
# 6.  RDD  →  Row  →  dict  →  JSON file
# ══════════════════════════════════════════════════════════════════════════════
def _row_to_file(row) -> str:
    """RDD mapper: one aggregated Row → one JSON file. Runs on executors."""
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

    doc = {
        "id":               row.id,
        "feed_provider_id": row.feed_provider_id,
        "property_name":    row.property_name,
        "property_slug":    row.property_slug,
        "country_code":     row.country_code,
        "currency":         row.currency,
        "usd_price":        row.usd_price,
        "star_rating":      row.star_rating,
        "review_score":     row.review_score,
        "commission":       row.commission,
        "meal_plan":        row.meal_plan,
        "published":        row.published,
        "data_quality_flag":row.data_quality_flag,
        "total_reviews":    len(reviews),
        "reviews":          reviews,
    }

    safe_id  = str(row.id).replace("/", "_").replace("\\", "_")
    out_path = os.path.join(_OUTPUT_DIR, f"{safe_id}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2, default=str)
    return out_path


def generate_json_files(df_agg: DataFrame) -> list[str]:
    log("WRITE", "Generating final JSON files", output_dir=_OUTPUT_DIR)
    written = df_agg.rdd.map(_row_to_file).collect()
    log("WRITE", "JSON generation complete", files_written=len(written))
    return written


# ══════════════════════════════════════════════════════════════════════════════
# 7.  Maintenance — both tables
# ══════════════════════════════════════════════════════════════════════════════
def _cutoff() -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")


def _maintain(spark: SparkSession, db: str, tbl: str) -> None:
    cat = config.ICEBERG_CATALOG
    ts  = _cutoff()
    try:
        spark.sql(f"""
            CALL {cat}.system.expire_snapshots(
              table => '{db}.{tbl}', older_than => TIMESTAMP '{ts}', retain_last => 1
            )
        """).show(truncate=False)
        log("MAINT", "expire_snapshots done", table=f"{db}.{tbl}")
    except Exception as e:
        log("MAINT", f"expire_snapshots skipped: {e}", table=f"{db}.{tbl}")

    try:
        spark.sql(f"""
            CALL {cat}.system.remove_orphan_files(
              table => '{db}.{tbl}', older_than => TIMESTAMP '{ts}'
            )
        """).show(truncate=False)
        log("MAINT", "remove_orphan_files done", table=f"{db}.{tbl}")
    except Exception as e:
        log("MAINT", f"remove_orphan_files skipped: {e}", table=f"{db}.{tbl}")


def run_maintenance(spark: SparkSession) -> None:
    log("MAINT", "Running maintenance on both tables")
    db = config.ICEBERG_DATABASE
    _maintain(spark, db, "rental_property")
    _maintain(spark, db, "property_reviews")
    log("MAINT", "Maintenance complete")


# ══════════════════════════════════════════════════════════════════════════════
# 8.  Verification
# ══════════════════════════════════════════════════════════════════════════════
def verify(written: list[str]) -> None:
    print("\n[PropertyJoiner] ── Sample output files ──")
    for path in written[:5]:
        print(f"  {path}")
    if len(written) > 5:
        print(f"  … and {len(written) - 5} more")
    print(f"\n[PropertyJoiner] Total: {len(written)} files → {_OUTPUT_DIR}")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    log("INIT", "PropertyJoiner starting")

    spark = build_spark()

    df_rental, df_reviews   = load_tables(spark)
    df_reviews_clean        = deduplicate_reviews(df_reviews)
    df_joined               = join_tables(df_rental, df_reviews_clean)
    df_agg                  = aggregate_per_property(df_joined)
    written                 = generate_json_files(df_agg)

    run_maintenance(spark)
    verify(written)

    log("DONE", "PropertyJoiner complete", files_written=len(written))
    flush_logs()
    spark.stop()


if __name__ == "__main__":
    main()
