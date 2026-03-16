"""
pipeline/json_generator.py
--------------------------
Reads the property_reviews Iceberg table and generates
one JSON file per property using Spark RDDs.

Table  : local.property_db.property_reviews
Output : data/property_data/GEN-<id>.json

Each file contains the property details with all its
reviews aggregated into a nested list.

Output shape
------------
{
  "property_id":  3005942,
  "gen_id":       "GEN-3005942",
  "property_name":"Anvil Barn...",
  "country_code": "GB",
  ...
  "total_reviews": 3,
  "reviews": [
    { "review_id": ..., "score": 10.0, ... },
    ...
  ]
}
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import os, sys, json

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from logger import log, flush_logs

_OUTPUT_DIR = config.OUTPUT_PROPERTY_DIR
os.makedirs(_OUTPUT_DIR, exist_ok=True)


# ══════════════════════════════════════════════════════════════════════════════
# 1.  SparkSession
# ══════════════════════════════════════════════════════════════════════════════
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("JsonGenerator")
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
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[JsonGenerator] SparkSession ready")
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Read Iceberg table
# ══════════════════════════════════════════════════════════════════════════════
def read_reviews_table(spark: SparkSession) -> DataFrame:
    df = spark.table(config.ICEBERG_REVIEWS_TABLE)
    log("READ", "Loaded property_reviews",
        rows=df.count(), table=config.ICEBERG_REVIEWS_TABLE)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Deduplicate
# ══════════════════════════════════════════════════════════════════════════════
def deduplicate(df: DataFrame) -> DataFrame:
    before   = df.count()
    df_dedup = df.dropDuplicates(["gen_id", "review_id"])
    dropped  = before - df_dedup.count()
    if dropped:
        log("DEDUP", "Removed duplicate review rows", dropped=dropped)
    else:
        log("DEDUP", "No duplicates found", rows=df_dedup.count())
    return df_dedup


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Aggregate — one row per property with reviews nested
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
            "property_id", "gen_id", "property_name", "property_slug",
            "country_code", "currency", "star_rating", "review_score",
            "published", "data_quality_flag",
        )
        .agg(
            F.collect_list(
                F.when(F.col("review_struct").isNotNull(), F.col("review_struct"))
            ).alias("reviews")
        )
        .orderBy("gen_id")
    )

    log("AGG", "Aggregated per property", distinct_properties=df_agg.count())
    return df_agg


# ══════════════════════════════════════════════════════════════════════════════
# 5.  RDD  →  Row  →  dict  →  JSON file
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
        "property_id":      row.property_id,
        "gen_id":           row.gen_id,
        "property_name":    row.property_name,
        "property_slug":    row.property_slug,
        "country_code":     row.country_code,
        "currency":         row.currency,
        "star_rating":      row.star_rating,
        "review_score":     row.review_score,
        "published":        row.published,
        "data_quality_flag":row.data_quality_flag,
        "total_reviews":    len(reviews),
        "reviews":          reviews,
    }

    safe_id  = str(row.gen_id).replace("/", "_").replace("\\", "_")
    out_path = os.path.join(_OUTPUT_DIR, f"{safe_id}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(doc, fh, ensure_ascii=False, indent=2, default=str)
    return out_path


def generate_json_files(df_agg: DataFrame) -> list[str]:
    log("WRITE", "Generating JSON files", output_dir=_OUTPUT_DIR)
    written = df_agg.rdd.map(_row_to_file).collect()
    log("WRITE", "JSON generation complete", files_written=len(written))
    return written


# ══════════════════════════════════════════════════════════════════════════════
# 6.  Verification
# ══════════════════════════════════════════════════════════════════════════════
def verify(written: list[str]) -> None:
    print("\n[JsonGenerator] ── Sample files ──")
    for path in written[:5]:
        print(f"  {path}")
    if len(written) > 5:
        print(f"  … and {len(written) - 5} more")
    print(f"\n[JsonGenerator] Total: {len(written)} files → {_OUTPUT_DIR}")


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    log("INIT", "JsonGenerator starting")

    spark    = build_spark()
    df_raw   = read_reviews_table(spark)
    df_dedup = deduplicate(df_raw)
    df_agg   = aggregate_per_property(df_dedup)
    written  = generate_json_files(df_agg)
    verify(written)

    log("DONE", "JsonGenerator complete", files_written=len(written))
    flush_logs()
    spark.stop()


if __name__ == "__main__":
    main()
