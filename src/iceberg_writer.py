"""
╔══════════════════════════════════════════════════════╗
║  MEMBER 1 — iceberg_writer.py                        ║
║  Writes the final SparkDataFrame (from main.py)      ║
║  into an Iceberg catalog table (Parquet format).     ║
║  Also expires snapshots and removes orphan files.    ║
╚══════════════════════════════════════════════════════╝

Table   : local.property_db.property_reviews
Catalog : Hadoop FileSystem catalog  (./warehouse)
Format  : Parquet
Partitions:
  • country_code  (29 countries — best geo-query field)
  • review_year   (2023-2026 — isolates hot vs cold data)
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, DoubleType, IntegerType, BooleanType
import os, sys

# ── paths ───────────────────────────────────────────────────────────────────────
_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
_DATA_DIR    = os.path.join(_PROJECT_DIR, "data")
_WAREHOUSE   = os.path.join(_PROJECT_DIR, "warehouse")

_PATH_PROPERTY = os.path.join(_DATA_DIR, "property.json")
_PATH_REVIEWS  = os.path.join(_DATA_DIR, "reviews.json")

# ── Iceberg identifiers ─────────────────────────────────────────────────────────
CATALOG    = "local"
DATABASE   = "property_db"
TABLE      = "property_reviews"
FULL_TABLE = f"{CATALOG}.{DATABASE}.{TABLE}"

_NAME_PREFERENCE = ["en-us", "en", "de", "fr", "es"]


# ══════════════════════════════════════════════════════════════════════════════
# 1.  Spark Session
# ══════════════════════════════════════════════════════════════════════════════
def build_spark_iceberg() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("IcebergPropertyWriter")
        .master("local[*]")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(f"spark.sql.catalog.{CATALOG}",
                "org.apache.iceberg.spark.SparkCatalog")
        .config(f"spark.sql.catalog.{CATALOG}.type", "hadoop")
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", _WAREHOUSE)
        .config("spark.sql.iceberg.write.format.default", "parquet")
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("[IcebergWriter] SparkSession ready — catalog:", CATALOG)
    print("[IcebergWriter] Warehouse path:", _WAREHOUSE)
    return spark


# ══════════════════════════════════════════════════════════════════════════════
# 2.  Pipeline helpers  (self-contained — no import of main.py needed)
#
#     main.py depends on a custom `logger` module which is not a standard
#     package. Importing it here would break with ModuleNotFoundError.
#     The pipeline logic is reproduced directly so iceberg_writer.py works
#     standalone without any path or import tricks.
# ══════════════════════════════════════════════════════════════════════════════

def _make_slug(col: F.Column) -> F.Column:
    return (
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.trim(F.lower(col)), r"[^\w\s-]", ""),
                    r"[\s_]+", "-",
                ),
                r"-{2,}", "-",
            ),
            r"^-|-$", "",
        )
    )


def _resolve_name_col(df: DataFrame) -> F.Column:
    name_fields = [f.name for f in df.schema["name"].dataType.fields]
    ordered = (
        [f for f in _NAME_PREFERENCE if f in name_fields]
        + [f for f in name_fields if f not in _NAME_PREFERENCE]
    )
    cols = [F.col(f"`name`.`{field}`") for field in ordered]
    return F.coalesce(*cols) if len(cols) > 1 else cols[0]


def _build_property_name(name_col: F.Column, city_col: F.Column) -> F.Column:
    name_ok = name_col.isNotNull() & (F.trim(name_col) != "")
    city_ok = city_col.isNotNull() & (F.trim(city_col) != "")
    return (
        F.when(name_ok & city_ok, F.concat(F.trim(name_col), F.trim(city_col)))
        .when(name_ok, F.trim(name_col))
        .otherwise(F.trim(city_col))
    )


def _load_sources(spark: SparkSession):
    df_prop = spark.read.option("multiLine", "true").json(_PATH_PROPERTY)
    df_rev  = spark.read.option("multiLine", "true").json(_PATH_REVIEWS)
    print(f"[IcebergWriter] Loaded {df_prop.count()} properties, "
          f"{df_rev.count()} review groups")
    return df_prop, df_rev


def _flatten_properties(df: DataFrame) -> DataFrame:
    df_clean = df.filter(F.col("id").isNotNull())
    name_col = _resolve_name_col(df)
    city_col = F.col("location.city")
    return df_clean.select(
        F.col("id").cast("long").alias("source_id"),
        name_col.alias("raw_name"),
        city_col.alias("raw_city"),
        F.col("currency").alias("raw_currency"),
        F.col("location.country").alias("raw_country"),
        F.col("rating.stars").cast("double").alias("star_rating_raw"),
        F.col("rating.review_score").cast("double").alias("review_score_raw"),
    ).withColumn(
        "property_name",
        _build_property_name(F.col("raw_name"), F.col("raw_city")),
    )


def _flatten_reviews(df: DataFrame) -> DataFrame:
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


def _enrich(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("id",
            F.concat(F.lit("GEN-"), F.col("source_id").cast("string")))
        .withColumn("feed_provider_id", F.col("source_id").cast("string"))
        .withColumn("property_slug", _make_slug(F.col("property_name")))
        .withColumn("country_code", F.upper(F.col("raw_country")))
        .withColumn("currency",
            F.when(
                F.col("raw_currency").isNull() | (F.trim(F.col("raw_currency")) == ""),
                F.lit("USD"),
            ).otherwise(F.col("raw_currency")))
        .withColumn("usd_price",    F.lit(0.0).cast("double"))
        .withColumn("star_rating",  F.coalesce(F.col("star_rating_raw"),  F.lit(0.0)))
        .withColumn("review_score", F.coalesce(F.col("review_score_raw"), F.lit(0.0)))
        .withColumn("published",    F.lit(True))
        .withColumn("data_quality_flag",
            F.when(
                F.col("property_name").isNull()
                | (F.trim(F.col("property_name")) == "")
                | F.col("country_code").isNull()
                | (F.trim(F.col("country_code")) == "")
                | (F.col("review_score") == 0.0)
                | (F.col("star_rating")  == 0.0),
                F.lit("NEEDS_REVIEW"),
            ).otherwise(F.lit("GOOD")))
    )


# ══════════════════════════════════════════════════════════════════════════════
# 3.  Build final DataFrame
# ══════════════════════════════════════════════════════════════════════════════
def get_final_dataframe(spark: SparkSession) -> DataFrame:
    df_prop_raw, df_rev_raw = _load_sources(spark)
    df_prop_flat = _flatten_properties(df_prop_raw)
    df_rev_flat  = _flatten_reviews(df_rev_raw)
    df_joined    = df_prop_flat.join(
        df_rev_flat,
        df_prop_flat["source_id"] == df_rev_flat["source_id_r"],
        how="left",
    )
    df_enriched = _enrich(df_joined)
    print(f"[IcebergWriter] Pipeline complete — {df_enriched.count()} rows")
    return df_enriched


# ══════════════════════════════════════════════════════════════════════════════
# 4.  Prepare Iceberg DataFrame — adds review_year partition column
# ══════════════════════════════════════════════════════════════════════════════
def prepare_iceberg_df(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "review_year",
            F.when(
                F.col("review_date").isNotNull(),
                F.year(F.to_date(F.col("review_date"), "yyyy-MM-dd")),
            ).otherwise(F.lit(None).cast("int")),
        )
        .select(
            F.col("source_id").cast(LongType())        .alias("property_id"),
            F.col("id")                                 .alias("gen_id"),
            F.col("property_name"),
            F.col("property_slug"),
            F.col("country_code"),                      # PARTITION 1
            F.col("currency"),
            F.col("star_rating")          .cast(DoubleType()),
            F.col("review_score")         .cast(DoubleType()),
            F.col("published")            .cast(BooleanType()),
            F.col("data_quality_flag"),
            F.col("review_id")            .cast(LongType()),
            F.col("review_date"),
            F.col("review_year")          .cast(IntegerType()),  # PARTITION 2
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
# 5.  Create table + write
# ══════════════════════════════════════════════════════════════════════════════
def create_iceberg_table(spark: SparkSession, df: DataFrame) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}.{DATABASE}")

    partition_cols = {"country_code", "review_year"}
    non_part = [f for f in df.schema.fields if f.name not in partition_cols]
    col_defs = ",\n  ".join(
        f"`{f.name}` {f.dataType.simpleString()}" for f in non_part
    )

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {FULL_TABLE} (
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
    print(f"[IcebergWriter] Table ready: {FULL_TABLE}")


def write_to_iceberg(spark: SparkSession, df: DataFrame) -> None:
    create_iceberg_table(spark, df)
    (
        df.writeTo(FULL_TABLE)
          .option("write.format.default", "parquet")
          .option("fanout-enabled", "true")
          .append()
    )
    print(f"[IcebergWriter] Data written → {FULL_TABLE}")


# ══════════════════════════════════════════════════════════════════════════════
# 6.  Maintenance
# ══════════════════════════════════════════════════════════════════════════════
def _ts_minus_1d() -> str:
    from datetime import datetime, timedelta, timezone
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")


def run_maintenance(spark: SparkSession) -> None:
    """
    Expire old snapshots and remove orphan files.

    Wrapped in try/except because CALL procedures require the Iceberg runtime
    JAR to exactly match the running Spark version.  A mismatch raises
    NoSuchMethodError inside the JVM.  The write has already succeeded by this
    point, so we log the problem and continue rather than aborting the job.
    """
    print("[IcebergWriter] Running maintenance …")
    cutoff = _ts_minus_1d()

    try:
        spark.sql(f"""
            CALL {CATALOG}.system.expire_snapshots(
              table       => '{DATABASE}.{TABLE}',
              older_than  => TIMESTAMP '{cutoff}',
              retain_last => 1
            )
        """).show(truncate=False)
        print("[IcebergWriter] expire_snapshots — done.")
    except Exception as e:
        print(f"[IcebergWriter] WARN expire_snapshots skipped: {type(e).__name__}: {e}")

    try:
        spark.sql(f"""
            CALL {CATALOG}.system.remove_orphan_files(
              table      => '{DATABASE}.{TABLE}',
              older_than => TIMESTAMP '{cutoff}'
            )
        """).show(truncate=False)
        print("[IcebergWriter] remove_orphan_files — done.")
    except Exception as e:
        print(f"[IcebergWriter] WARN remove_orphan_files skipped: {type(e).__name__}: {e}")

    print("[IcebergWriter] Maintenance step complete.")


# ══════════════════════════════════════════════════════════════════════════════
# 7.  Verification
# ══════════════════════════════════════════════════════════════════════════════
def verify_write(spark: SparkSession) -> None:
    print("\n[IcebergWriter] ── Rows per partition ──")
    spark.sql(f"""
        SELECT country_code, review_year, COUNT(*) AS cnt
        FROM   {FULL_TABLE}
        GROUP BY country_code, review_year
        ORDER BY country_code, review_year
    """).show(50, truncate=False)

    print("\n[IcebergWriter] ── Snapshot history ──")
    spark.sql(f"""
        SELECT snapshot_id, committed_at, operation
        FROM   {FULL_TABLE}.snapshots
    """).show(truncate=False)

    print("\n[IcebergWriter] ── Partition files ──")
    spark.sql(f"""
        SELECT partition, record_count, file_count
        FROM   {FULL_TABLE}.partitions
        ORDER BY partition
    """).show(50, truncate=False)


# ══════════════════════════════════════════════════════════════════════════════
# Entry point
# ══════════════════════════════════════════════════════════════════════════════
def main() -> None:
    spark      = build_spark_iceberg()
    df_final   = get_final_dataframe(spark)
    df_iceberg = prepare_iceberg_df(df_final)
    write_to_iceberg(spark, df_iceberg)
    run_maintenance(spark)
    verify_write(spark)
    print("\n[IcebergWriter] All done.")
    spark.stop()


if __name__ == "__main__":
    main()