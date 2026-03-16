"""
╔══════════════════════════════════════════════════════╗
║  MEMBER 2 — json_generator.py                        ║
║  Reads the Iceberg table and generates one           ║
║  JSON file per property using Spark RDDs.            ║
║  Output: data/property_data/<property_id>.json       ║
╚══════════════════════════════════════════════════════╝
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
import os, json, sys

# ── paths ──────────────────────────────────────────────────────────────────────
_SCRIPT_DIR   = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR  = os.path.dirname(_SCRIPT_DIR)
_DATA_DIR     = os.path.join(_PROJECT_DIR, "data")
_OUTPUT_DIR   = os.path.join(_DATA_DIR, "property_data")
_WAREHOUSE    = os.path.join(_PROJECT_DIR, "warehouse")

os.makedirs(_OUTPUT_DIR, exist_ok=True)

# ── Iceberg identifiers (must match iceberg_writer.py) ────────────────────────
CATALOG    = "local"
DATABASE   = "property_db"
TABLE      = "property_reviews"
FULL_TABLE = f"{CATALOG}.{DATABASE}.{TABLE}"


# ══════════════════════════════════════════════════════════════════════════════
# 1.  Spark Session  (same Iceberg config as writer)
# ══════════════════════════════════════════════════════════════════════════════
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("IcebergPropertyJsonGenerator")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", _WAREHOUSE)
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.2",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[JsonGenerator] SparkSession ready")
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Read Iceberg table
# ══════════════════════════════════════════════════════════════════════════════
def read_iceberg_table(spark: SparkSession) -> DataFrame:
    df = spark.table(FULL_TABLE)
    print(f"[JsonGenerator] Loaded {df.count()} rows from {FULL_TABLE}")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Build property → reviews structure using Spark SQL aggregation
# ══════════════════════════════════════════════════════════════════════════════
def build_property_review_df(df: DataFrame) -> DataFrame:
    """
    Aggregate all reviews per property_id into a single JSON-serialisable struct.
    Returns one row per property.
    """
    # Collect reviews array per property
    return (
        df
        .withColumn(
            "review_struct",
            F.struct(
                F.col("review_id"),
                F.col("review_date"),
                F.col("review_year"),
                F.col("review_individual_score"),
                F.col("review_language"),
                F.col("review_summary"),
                F.col("review_positive"),
                F.col("review_negative"),
                F.col("reviewer_name"),
                F.col("reviewer_country"),
                F.col("reviewer_travel_purpose"),
                F.col("reviewer_type"),
            ),
        )
        .groupBy(
            "property_id",
            "gen_id",
            "property_name",
            "property_slug",
            "country_code",
            "currency",
            "star_rating",
            "review_score",
            "published",
            "data_quality_flag",
        )
        .agg(
            F.collect_list(
                F.when(F.col("review_id").isNotNull(), F.col("review_struct"))
            ).alias("reviews")
        )
        .orderBy("property_id")
    )


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Generate JSON files via Spark RDDs
#     One file per property → data/property_data/<property_id>.json
# ══════════════════════════════════════════════════════════════════════════════
def _row_to_json_file(row) -> str:
    """
    Mapper function: runs on each executor partition.
    Serialises one Row to a JSON file on the driver (collected via mapPartitions).
    Returns the file path written.
    """
    # Build Python dict from Row
    reviews_list = []
    for r in (row.reviews or []):
        reviews_list.append({
            "review_id":              r.review_id,
            "review_date":            r.review_date,
            "review_year":            r.review_year,
            "score":                  r.review_individual_score,
            "language":               r.review_language,
            "summary":                r.review_summary,
            "positive":               r.review_positive,
            "negative":               r.review_negative,
            "reviewer_name":          r.reviewer_name,
            "reviewer_country":       r.reviewer_country,
            "reviewer_travel_purpose":r.reviewer_travel_purpose,
            "reviewer_type":          r.reviewer_type,
        })

    property_doc = {
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
        "total_reviews":    len(reviews_list),
        "reviews":          reviews_list,
    }

    out_path = os.path.join(_OUTPUT_DIR, f"{row.property_id}.json")
    with open(out_path, "w", encoding="utf-8") as fh:
        json.dump(property_doc, fh, ensure_ascii=False, indent=2, default=str)

    return out_path


def generate_property_json_files(df: DataFrame) -> list[str]:
    """
    Use Spark RDD .map() to write one JSON file per property.
    Returns list of written file paths.
    """
    print(f"[JsonGenerator] Generating JSON files → {_OUTPUT_DIR}")

    rdd = df.rdd                    # convert DataFrame to RDD of Row objects
    written_paths = rdd.map(_row_to_json_file).collect()

    print(f"[JsonGenerator] ✓ {len(written_paths)} JSON files written.")
    return written_paths


# ══════════════════════════════════════════════════════════════════════════════
# 5.  Fallback: run without Iceberg (reads raw JSON via ASSI2 pipeline)
#     Used when iceberg_writer.py has not been run yet.
# ══════════════════════════════════════════════════════════════════════════════
def fallback_from_pipeline(spark: SparkSession) -> DataFrame:
    """
    If the Iceberg warehouse doesn't exist yet, run the ASSI2 pipeline
    directly and produce the same aggregated DataFrame.
    """
    print("[JsonGenerator] Iceberg table not found — falling back to pipeline.")
    sys.path.insert(0, _SCRIPT_DIR)

    from main import (load_sources, flatten_properties, flatten_reviews,
                      join_property_reviews, enrich_columns, add_data_quality_flag)
    from iceberg_writer import prepare_iceberg_df

    df_prop_raw, df_rev_raw = load_sources(spark)
    df_prop_flat, _         = flatten_properties(df_prop_raw)
    df_rev_flat             = flatten_reviews(df_rev_raw)
    df_joined               = join_property_reviews(df_prop_flat, df_rev_flat)
    df_enriched             = enrich_columns(df_joined)
    df_quality              = add_data_quality_flag(df_enriched)
    return prepare_iceberg_df(df_quality)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    spark = build_spark()

    # Try reading from Iceberg; fall back to pipeline if table missing
    try:
        df_raw = read_iceberg_table(spark)
    except Exception as exc:
        print(f"[JsonGenerator] Warning: {exc}")
        df_raw = fallback_from_pipeline(spark)

    # Aggregate: one row per property with nested reviews list
    df_agg = build_property_review_df(df_raw)
    print(f"[JsonGenerator] Distinct properties: {df_agg.count()}")

    # Write JSON files via RDD
    written = generate_property_json_files(df_agg)

    # Summary
    print("\n[JsonGenerator] Sample files written:")
    for p in written[:5]:
        print(f"  {p}")
    if len(written) > 5:
        print(f"  … and {len(written)-5} more.")

    print("\n[JsonGenerator] ✓ All done.")
    spark.stop()


if __name__ == "__main__":
    main()