"""
pipeline/rental_writer.py
-------------------------
Reads property.json and search.json, builds the standardized
rental_property DataFrame, and writes it to the Iceberg catalog.

Table     : local.property_db.rental_property
Format    : Parquet / Snappy
Partition : country_code
Maintenance: expire_snapshots + remove_orphan_files

Pipeline
--------
  1. Read   property.json  →  raw details
     Read   search.json    →  raw search
  2. Extract fields from each source
  3. Drop rows with missing source_id
  4. Deduplicate on source_id
  5. Inner join details × search  →  matched
  6. Build 13-column final output + data_quality_flag
  7. Write  →  single Parquet file  (output/final_output/)
  8. Write  →  Iceberg table        (rental_property)
  9. Expire snapshots + remove orphan files
 10. Verify written data
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType
import os, sys

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from logger import log, flush_logs

from pyspark.sql.types import (
    StructType, StructField, BooleanType, DoubleType, StringType
)

# ── Iceberg table schema ────────────────────────────────────────────────────────
RENTAL_PROPERTY_SCHEMA = StructType([
    StructField("id",                StringType(),  False),
    StructField("feed_provider_id",  StringType(),  True),
    StructField("property_name",     StringType(),  True),
    StructField("property_slug",     StringType(),  True),
    StructField("country_code",      StringType(),  True),
    StructField("currency",          StringType(),  True),
    StructField("usd_price",         DoubleType(),  True),
    StructField("star_rating",       DoubleType(),  True),
    StructField("review_score",      DoubleType(),  True),
    StructField("commission",        DoubleType(),  True),
    StructField("meal_plan",         StringType(),  True),
    StructField("published",         BooleanType(), True),
    StructField("data_quality_flag", StringType(),  True),
])


# ══════════════════════════════════════════════════════════════════════════════
# 1.  SparkSession
# ══════════════════════════════════════════════════════════════════════════════
def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("RentalPropertyWriter")
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
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[RentalWriter] SparkSession ready")
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Ingestion
# ══════════════════════════════════════════════════════════════════════════════
def load_sources(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    log("READ", "Reading property.json", path=config.INPUT_DETAILS_FILE)
    df_details = spark.read.option("multiline", "true").json(config.INPUT_DETAILS_FILE)
    log("READ", "Reading search.json",   path=config.INPUT_SEARCH_FILE)
    df_search  = spark.read.option("multiline", "true").json(config.INPUT_SEARCH_FILE)
    log("READ", "Sources loaded",
        details_rows=df_details.count(), search_rows=df_search.count())
    return df_details, df_search


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Extraction
# ══════════════════════════════════════════════════════════════════════════════
def extract_details(df: DataFrame) -> DataFrame:
    """Flatten property.json → source_id, property_name, country_code,
    currency, star_rating, review_score."""
    log("EXTRACT", "Extracting details fields")
    name_fields = [f.name for f in df.schema["name"].dataType.fields]
    ordered     = (
        [f for f in config.NAME_PREFERENCE if f in name_fields]
        + [f for f in name_fields if f not in config.NAME_PREFERENCE]
    )
    name_cols = [F.col(f"`name`.`{field}`") for field in ordered]
    best_name = F.coalesce(*name_cols) if len(name_cols) > 1 else name_cols[0]

    return df.select(
        F.col("id").cast(StringType()).alias("source_id"),
        best_name.alias("property_name"),
        F.trim(F.upper(F.col("location.country"))).alias("country_code"),
        F.col("currency"),
        F.col("rating.stars").cast(DoubleType()).alias("star_rating"),
        F.col("rating.review_score").cast(DoubleType()).alias("review_score"),
    )


def extract_search(df: DataFrame) -> DataFrame:
    """Flatten search.json → search_id, usd_price, commission_pct,
    meal_plan, deep_link_url, search_currency."""
    log("EXTRACT", "Extracting search fields")
    return df.select(
        F.col("id").cast(StringType()).alias("search_id"),
        F.col("price.book").cast(DoubleType()).alias("usd_price"),
        F.col("commission.percentage").cast(DoubleType()).alias("commission_pct"),
        F.col("products").getItem(0)
         .getField("policies").getField("meal_plan").getField("meals")
         .cast(StringType()).alias("meal_plan"),
        F.col("deep_link_url"),
        F.col("currency").alias("search_currency"),
    )


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Cleaning + deduplication
# ══════════════════════════════════════════════════════════════════════════════
def clean(df: DataFrame) -> DataFrame:
    before  = df.count()
    clean   = df.filter(F.col("source_id").isNotNull() & (F.col("source_id") != ""))
    dropped = before - clean.count()
    log("CLEAN", "Dropped null source_id rows", dropped=dropped)
    return clean


def deduplicate(df: DataFrame, key: str) -> DataFrame:
    before   = df.count()
    df_dedup = df.dropDuplicates([key])
    log("DEDUP", f"Deduplicated on '{key}'",
        removed=before - df_dedup.count())
    return df_dedup


# ══════════════════════════════════════════════════════════════════════════════
# 5.  Join
# ══════════════════════════════════════════════════════════════════════════════
def join_details_search(
    details_df: DataFrame, search_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Inner join + left-anti join on source_id == search_id."""
    log("JOIN", "Joining details × search")
    matched = details_df.join(
        search_df, details_df["source_id"] == search_df["search_id"], how="inner"
    )
    unmatched = details_df.join(
        search_df, details_df["source_id"] == search_df["search_id"], how="left_anti"
    )
    log("JOIN", "Join complete",
        matched=matched.count(), unmatched=unmatched.count())
    return matched, unmatched


# ══════════════════════════════════════════════════════════════════════════════
# 6.  Build final output
# ══════════════════════════════════════════════════════════════════════════════
def _make_slug(col: F.Column) -> F.Column:
    return F.lower(F.regexp_replace(F.trim(col), r"[^a-zA-Z0-9]+", "-"))


def build_final_output(matched_df: DataFrame) -> tuple[DataFrame, int]:
    """Produce the 13-column standardized DataFrame + data_quality_flag."""
    log("TRANSFORM", "Building final output")
    defaulted = matched_df.filter(F.col("usd_price").isNull()).count()

    df = matched_df.select(
        F.concat_ws("-", F.lit("GEN"), F.col("source_id")).alias("id"),
        F.col("source_id").alias("feed_provider_id"),
        F.col("property_name"),
        _make_slug(F.col("property_name")).alias("property_slug"),
        F.col("country_code"),
        F.coalesce(F.col("currency"),     F.lit(config.DEFAULT_CURRENCY))    .alias("currency"),
        F.coalesce(F.col("usd_price"),    F.lit(config.DEFAULT_USD_PRICE))   .cast(DoubleType()).alias("usd_price"),
        F.coalesce(F.col("star_rating"),  F.lit(config.DEFAULT_STAR_RATING)) .cast(DoubleType()).alias("star_rating"),
        F.coalesce(F.col("review_score"), F.lit(config.DEFAULT_REVIEW_SCORE)).cast(DoubleType()).alias("review_score"),
        F.col("commission_pct").alias("commission"),
        F.col("meal_plan"),
        F.lit(config.DEFAULT_PUBLISHED).cast(BooleanType()).alias("published"),
    ).withColumn(
        "data_quality_flag",
        F.when(
            F.col("property_name").isNull()
            | F.col("usd_price").isNull()
            | (F.length(F.col("country_code")) != 2),
            F.lit("NEEDS_REVIEW"),
        ).otherwise(F.lit("GOOD")),
    )

    log("TRANSFORM", "Final output ready",
        rows=df.count(), columns=len(df.columns), defaulted_prices=defaulted)
    return df, defaulted


# ══════════════════════════════════════════════════════════════════════════════
# 7.  Write
# ══════════════════════════════════════════════════════════════════════════════
def _create_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS "
              f"{config.ICEBERG_CATALOG}.{config.ICEBERG_DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.ICEBERG_PROPERTY_TABLE} (
            id                STRING  NOT NULL,
            feed_provider_id  STRING,
            property_name     STRING,
            property_slug     STRING,
            country_code      STRING,
            currency          STRING,
            usd_price         DOUBLE,
            star_rating       DOUBLE,
            review_score      DOUBLE,
            commission        DOUBLE,
            meal_plan         STRING,
            published         BOOLEAN,
            data_quality_flag STRING
        )
        USING iceberg
        PARTITIONED BY ({config.PARTITION_PROPERTY})
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    log("DDL", "rental_property table ready", table=config.ICEBERG_PROPERTY_TABLE)


def write_to_iceberg(spark: SparkSession, df: DataFrame) -> None:
    # single-file Parquet backup
    log("WRITE", "Single-file Parquet write", path=config.OUTPUT_FINAL_DIR)
    df.coalesce(1).write.mode("overwrite").parquet(config.OUTPUT_FINAL_DIR)

    # Iceberg partitioned write
    ordered_df = df.select([f.name for f in RENTAL_PROPERTY_SCHEMA.fields])
    _create_table(spark)
    ordered_df.writeTo(config.ICEBERG_PROPERTY_TABLE).overwritePartitions()
    log("WRITE", "Iceberg write complete", table=config.ICEBERG_PROPERTY_TABLE)


# ══════════════════════════════════════════════════════════════════════════════
# 8.  Maintenance
# ══════════════════════════════════════════════════════════════════════════════
def _cutoff() -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")


def run_maintenance(spark: SparkSession) -> None:
    log("MAINT", "Running maintenance", table=config.ICEBERG_PROPERTY_TABLE)
    db  = config.ICEBERG_DATABASE
    tbl = "rental_property"
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
    print("\n[RentalWriter] ── Rows per country ──")
    spark.sql(f"""
        SELECT country_code, COUNT(*) AS cnt
        FROM   {config.ICEBERG_PROPERTY_TABLE}
        GROUP BY country_code ORDER BY cnt DESC
    """).show(30, truncate=False)

    print("\n[RentalWriter] ── Snapshot history ──")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation
        FROM   {config.ICEBERG_PROPERTY_TABLE}.snapshots
    """).show(truncate=False)

    print("\n[RentalWriter] ── Partition files ──")
    spark.sql(f"""
        SELECT partition, record_count, file_count
        FROM   {config.ICEBERG_PROPERTY_TABLE}.partitions ORDER BY partition
    """).show(30, truncate=False)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    log("INIT", "RentalWriter starting")

    spark = build_spark()

    df_details, df_search = load_sources(spark)

    details_ext   = extract_details(df_details)
    search_ext    = extract_search(df_search)

    details_clean = clean(details_ext)
    details_dedup = deduplicate(details_clean, "source_id")

    matched, unmatched = join_details_search(details_dedup, search_ext)
    log("INFO", "Unmatched properties", count=unmatched.count())

    final_df, defaulted = build_final_output(matched)

    write_to_iceberg(spark, final_df)
    run_maintenance(spark)
    verify(spark)

    log("DONE", "RentalWriter complete", defaulted_prices=defaulted)
    flush_logs()
    spark.stop()


if __name__ == "__main__":
    main()
