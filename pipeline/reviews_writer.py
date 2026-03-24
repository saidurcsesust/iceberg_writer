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
import os, sys

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from spark_utils import build_spark
from logger import log, flush_logs
from pipeline.transform import (
    reviews_flatten_properties,
    reviews_flatten_reviews,
)
from pipeline.join_table import (
    reviews_join_properties_reviews,
)
from pipeline.modify_column import (
    reviews_enrich_joined_data,
    reviews_prepare_for_iceberg,
)


# -----------------------------------------------------------------------------
# 1.  Ingestion
# -----------------------------------------------------------------------------
def load_sources(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    log("READ", "Reading property.json", path=config.INPUT_DETAILS_FILE)
    df_prop = spark.read.option("multiLine", "true").json(config.INPUT_DETAILS_FILE)
    log("READ", "Reading reviews.json",  path=config.INPUT_REVIEWS_FILE)
    df_rev  = spark.read.option("multiLine", "true").json(config.INPUT_REVIEWS_FILE)
    log("READ", "Sources loaded",
        property_rows=df_prop.count(), review_groups=df_rev.count())
    return df_prop, df_rev


# -----------------------------------------------------------------------------
# 2.  Write to Iceberg
# -----------------------------------------------------------------------------
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


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
def run() -> None:
    log("INIT", "ReviewsWriter starting")

    spark = build_spark("ReviewsWriter")

    df_prop_raw, df_rev_raw = load_sources(spark)

    df_prop_flat = reviews_flatten_properties(df_prop_raw)
    df_rev_flat  = reviews_flatten_reviews(df_rev_raw)

    df_joined    = reviews_join_properties_reviews(df_prop_flat, df_rev_flat)
    df_enriched  = reviews_enrich_joined_data(df_joined)
    df_iceberg   = reviews_prepare_for_iceberg(df_enriched)

    write_to_iceberg(spark, df_iceberg)

    log("DONE", "ReviewsWriter complete")
    flush_logs()
    spark.stop()
