"""
pipeline/reviews_writer.py
--------------------------
Reads property.json and reviews.json, builds the enriched
property-reviews DataFrame, and writes it to the Iceberg catalog.

Table      : local.property_db.property_reviews
Format     : Parquet / Snappy
Partitions : country_code, review_year
Maintenance: expire_snapshots + remove_orphan_files

Pipeline
--------
  1. Read   property.json  →  raw properties
     Read   reviews.json   →  raw reviews
  2. Flatten properties (resolve localised name, extract fields)
  3. Explode reviews (one row per review per property)
  4. Left-join properties × reviews
  5. Enrich (gen_id, slug, quality flag, review_year)
  6. Write  →  Iceberg table  (property_reviews)
  7. Expire snapshots + remove orphan files
  8. Verify written data
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, IntegerType, BooleanType
import os, sys

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from logger import log, flush_logs


# ══════════════════════════════════════════════════════════════════════════════
# 1.  SparkSession
# ══════════════════════════════════════════════════════════════════════════════
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("ReviewsWriter")
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
    print("[ReviewsWriter] SparkSession ready")
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Ingestion
# ══════════════════════════════════════════════════════════════════════════════
def load_sources(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    log("READ", "Reading property.json", path=config.INPUT_DETAILS_FILE)
    df_prop = spark.read.option("multiLine", "true").json(config.INPUT_DETAILS_FILE)
    log("READ", "Reading reviews.json",  path=config.INPUT_REVIEWS_FILE)
    df_rev  = spark.read.option("multiLine", "true").json(config.INPUT_REVIEWS_FILE)
    log("READ", "Sources loaded",
        property_rows=df_prop.count(), review_groups=df_rev.count())
    return df_prop, df_rev


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Flatten properties
# ══════════════════════════════════════════════════════════════════════════════
def flatten_properties(df: DataFrame) -> DataFrame:
    """Filter nulls, resolve localised name, extract core fields."""
    log("EXTRACT", "Flattening properties")
    df_clean    = df.filter(F.col("id").isNotNull())
    name_fields = [f.name for f in df.schema["name"].dataType.fields]
    ordered     = (
        [f for f in config.NAME_PREFERENCE if f in name_fields]
        + [f for f in name_fields if f not in config.NAME_PREFERENCE]
    )
    name_cols = [F.col(f"`name`.`{field}`") for field in ordered]
    best_name = F.coalesce(*name_cols) if len(name_cols) > 1 else name_cols[0]

    return df_clean.select(
        F.col("id").cast("long").alias("source_id"),
        best_name.alias("raw_name"),
        F.col("location.city").alias("raw_city"),
        F.col("currency").alias("raw_currency"),
        F.col("location.country").alias("raw_country"),
        F.col("rating.stars").cast("double").alias("star_rating_raw"),
        F.col("rating.review_score").cast("double").alias("review_score_raw"),
    ).withColumn(
        "property_name",
        F.when(
            F.col("raw_name").isNotNull() & (F.trim(F.col("raw_name")) != ""),
            F.trim(F.col("raw_name")),
        ).otherwise(F.trim(F.col("raw_city"))),
    )


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Flatten reviews
# ══════════════════════════════════════════════════════════════════════════════
def flatten_reviews(df: DataFrame) -> DataFrame:
    """Explode the reviews array — one row per review."""
    log("EXTRACT", "Exploding reviews")
    return (
        df
        .withColumn("review", F.explode_outer(F.col("reviews")))
        .select(
            F.col("id").cast("long").alias("source_id_r"),
            F.col("review.id").cast("long").alias("review_id"),
            F.col("review.date").alias("review_date"),
            F.col("review.score").cast("double").alias("review_individual_score"),
            F.col("review.language").alias("review_language"),
            F.col("review.summary").alias("review_summary"),
            F.col("review.positive").alias("review_positive"),
            F.col("review.negative").alias("review_negative"),
            F.col("review.reviewer.name").alias("reviewer_name"),
            F.col("review.reviewer.country").alias("reviewer_country"),
            F.col("review.reviewer.travel_purpose").alias("reviewer_travel_purpose"),
            F.col("review.reviewer.type").alias("reviewer_type"),
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# 5.  Join + enrich
# ══════════════════════════════════════════════════════════════════════════════
def _make_slug(col: F.Column) -> F.Column:
    return (
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.trim(F.lower(col)), r"[^\w\s-]", ""),
                    r"[\s_]+", "-"),
                r"-{2,}", "-"),
            r"^-|-$", "")
    )


def join_and_enrich(df_prop: DataFrame, df_rev: DataFrame) -> DataFrame:
    """Left-join then add gen_id, slug, country_code, quality flag."""
    log("JOIN", "Joining properties × reviews")
    df_joined = df_prop.join(
        df_rev,
        df_prop["source_id"] == df_rev["source_id_r"],
        how="left",
    )
    log("JOIN", "Join complete", rows=df_joined.count())

    return (
        df_joined
        .withColumn("gen_id",
            F.concat(F.lit("GEN-"), F.col("source_id").cast("string")))
        .withColumn("feed_provider_id", F.col("source_id").cast("string"))
        .withColumn("property_slug", _make_slug(F.col("property_name")))
        .withColumn("country_code", F.upper(F.col("raw_country")))
        .withColumn("currency",
            F.when(
                F.col("raw_currency").isNull() | (F.trim(F.col("raw_currency")) == ""),
                F.lit(config.DEFAULT_CURRENCY),
            ).otherwise(F.col("raw_currency")))
        .withColumn("star_rating",
            F.coalesce(F.col("star_rating_raw"), F.lit(config.DEFAULT_STAR_RATING)))
        .withColumn("review_score",
            F.coalesce(F.col("review_score_raw"), F.lit(config.DEFAULT_REVIEW_SCORE)))
        .withColumn("published", F.lit(True))
        .withColumn("data_quality_flag",
            F.when(
                F.col("property_name").isNull()
                | (F.trim(F.col("property_name")) == "")
                | F.col("country_code").isNull()
                | (F.col("review_score") == 0.0)
                | (F.col("star_rating")  == 0.0),
                F.lit("NEEDS_REVIEW"),
            ).otherwise(F.lit("GOOD")))
    )


# ══════════════════════════════════════════════════════════════════════════════
# 6.  Prepare Iceberg DataFrame
# ══════════════════════════════════════════════════════════════════════════════
def prepare_for_iceberg(df: DataFrame) -> DataFrame:
    """Select final columns, add review_year partition column."""
    return (
        df
        .withColumn("review_year",
            F.when(F.col("review_date").isNotNull(),
                   F.year(F.to_date(F.col("review_date"), "yyyy-MM-dd")))
             .otherwise(F.lit(None).cast("int")))
        .select(
            F.col("source_id").cast(LongType())       .alias("property_id"),
            F.col("gen_id"),
            F.col("property_name"),
            F.col("property_slug"),
            F.col("country_code"),
            F.col("currency"),
            F.col("star_rating")          .cast(DoubleType()),
            F.col("review_score")         .cast(DoubleType()),
            F.col("published")            .cast(BooleanType()),
            F.col("data_quality_flag"),
            F.col("review_id")            .cast(LongType()),
            F.col("review_date"),
            F.col("review_year")          .cast(IntegerType()),
            F.col("review_individual_score").cast(DoubleType()),
            F.col("review_language"),
            F.col("review_summary"),
            F.col("review_positive"),
            F.col("review_negative"),
            F.col("reviewer_name"),
            F.col("reviewer_country"),
            F.col("reviewer_travel_purpose"),
            F.col("reviewer_type"),
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# 7.  Write to Iceberg
# ══════════════════════════════════════════════════════════════════════════════
def _create_table(spark: SparkSession, df: DataFrame) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS "
              f"{config.ICEBERG_CATALOG}.{config.ICEBERG_DATABASE}")
    partition_cols = {"country_code", "review_year"}
    non_part = [f for f in df.schema.fields if f.name not in partition_cols]
    col_defs = ",\n  ".join(
        f"`{f.name}` {f.dataType.simpleString()}" for f in non_part
    )
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.ICEBERG_REVIEWS_TABLE} (
          {col_defs},
          country_code  string,
          review_year   int
        )
        USING iceberg
        PARTITIONED BY (country_code, review_year)
        TBLPROPERTIES (
          'write.format.default'               = 'parquet',
          'write.parquet.compression-codec'    = 'snappy',
          'history.expire.max-snapshot-age-ms' = '86400000'
        )
    """)
    log("DDL", "property_reviews table ready", table=config.ICEBERG_REVIEWS_TABLE)


def write_to_iceberg(spark: SparkSession, df: DataFrame) -> None:
    _create_table(spark, df)
    (
        df.writeTo(config.ICEBERG_REVIEWS_TABLE)
          .option("write.format.default", "parquet")
          .option("fanout-enabled", "true")
          .append()
    )
    log("WRITE", "Iceberg write complete", table=config.ICEBERG_REVIEWS_TABLE)


# ══════════════════════════════════════════════════════════════════════════════
# 8.  Maintenance
# ══════════════════════════════════════════════════════════════════════════════
def _cutoff() -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")


def run_maintenance(spark: SparkSession) -> None:
    log("MAINT", "Running maintenance", table=config.ICEBERG_REVIEWS_TABLE)
    db  = config.ICEBERG_DATABASE
    tbl = "property_reviews"
    cat = config.ICEBERG_CATALOG
    ts  = _cutoff()

    try:
        spark.sql(f"""
            CALL {cat}.system.expire_snapshots(
              table => '{db}.{tbl}', older_than => TIMESTAMP '{ts}', retain_last => 1
            )
        """).show(truncate=False)
    except Exception as e:
        log("MAINT", f"expire_snapshots skipped: {e}")

    try:
        spark.sql(f"""
            CALL {cat}.system.remove_orphan_files(
              table => '{db}.{tbl}', older_than => TIMESTAMP '{ts}'
            )
        """).show(truncate=False)
    except Exception as e:
        log("MAINT", f"remove_orphan_files skipped: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# 9.  Verification
# ══════════════════════════════════════════════════════════════════════════════
def verify(spark: SparkSession) -> None:
    print("\n[ReviewsWriter] ── Rows per partition ──")
    spark.sql(f"""
        SELECT country_code, review_year, COUNT(*) AS cnt
        FROM   {config.ICEBERG_REVIEWS_TABLE}
        GROUP BY country_code, review_year
        ORDER BY country_code, review_year
    """).show(50, truncate=False)

    print("\n[ReviewsWriter] ── Snapshot history ──")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation
        FROM   {config.ICEBERG_REVIEWS_TABLE}.snapshots
    """).show(truncate=False)

    print("\n[ReviewsWriter] ── Partition files ──")
    spark.sql(f"""
        SELECT partition, record_count, file_count
        FROM   {config.ICEBERG_REVIEWS_TABLE}.partitions ORDER BY partition
    """).show(50, truncate=False)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    log("INIT", "ReviewsWriter starting")

    spark = build_spark()

    df_prop_raw, df_rev_raw = load_sources(spark)

    df_prop_flat = flatten_properties(df_prop_raw)
    df_rev_flat  = flatten_reviews(df_rev_raw)

    df_enriched  = join_and_enrich(df_prop_flat, df_rev_flat)
    df_iceberg   = prepare_for_iceberg(df_enriched)

    write_to_iceberg(spark, df_iceberg)
    run_maintenance(spark)
    verify(spark)

    log("DONE", "ReviewsWriter complete")
    flush_logs()
    spark.stop()


if __name__ == "__main__":
    main()
