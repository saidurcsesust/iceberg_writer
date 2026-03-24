"""
pipeline/rental_writer.py
-------------------------
Reads property.json and search.json, builds the standardized
rental_property DataFrame, and writes it to the Iceberg catalog.

Table     : local.property_db.rental_property
Format    : Parquet
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
  7. Write  -->  single Parquet file  (output/final_output/)
  8. Write  -->  Iceberg table        (rental_property)
  9. Expire snapshots + remove orphan files
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import DoubleType, StringType, BooleanType, StructType, StructField
import os, sys

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_PROJECT_DIR = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_DIR not in sys.path:
    sys.path.insert(0, _PROJECT_DIR)

import config
from spark_utils import build_spark
from logger import log, flush_logs

from pipeline.transform import (
    rental_extract_details,
    rental_extract_search,
    rental_clean,
    rental_deduplicate,

)
from pipeline.join_table import (
    rental_join_details_search,
)

from pipeline.modify_column import rental_build_final_output


# Iceberg table schema 
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


# -----------------------------------------------------------------------------
# 1.  Ingestion
# -----------------------------------------------------------------------------
def load_sources(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    log("READ", "Reading property.json", path=config.INPUT_DETAILS_FILE)
    df_details = spark.read.option("multiline", "true").json(config.INPUT_DETAILS_FILE)
    log("READ", "Reading search.json",   path=config.INPUT_SEARCH_FILE)
    df_search  = spark.read.option("multiline", "true").json(config.INPUT_SEARCH_FILE)
    log("READ", "Sources loaded",
        details_rows=df_details.count(), search_rows=df_search.count())
    return df_details, df_search


# -----------------------------------------------------------------------------
# 2.  Create Iceberg Table
# -----------------------------------------------------------------------------
def _create_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS "
              f"{config.ICEBERG_CATALOG}.{config.ICEBERG_DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {config.ICEBERG_RENTALS_TABLE} (
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
    log("DDL", "rental_property table ready", table=config.ICEBERG_RENTALS_TABLE)


def write_to_iceberg(spark: SparkSession, df: DataFrame) -> None:
    _create_table(spark)
    (
        df.writeTo(config.ICEBERG_RENTALS_TABLE)
          .option("write.format.default", "parquet")
          .option("fanout-enabled", "true")
          .append()
    )
    log("WRITE", "Iceberg write complete", table=config.ICEBERG_RENTALS_TABLE)


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
def run() -> None:
    log("INIT", "RentalWriter starting")

    spark = build_spark(
        "RentalPropertyWriter",
        extra_conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.driver.memory": "2g",
        },
    )

    df_details, df_search = load_sources(spark)

    details_ext   = rental_extract_details(df_details)
    search_ext    = rental_extract_search(df_search)

    details_clean = rental_clean(details_ext)
    details_dedup = rental_deduplicate(details_clean, "source_id")

    matched, unmatched = rental_join_details_search(details_dedup, search_ext)
    log("INFO", "Unmatched properties", count=unmatched.count())

    final_df, defaulted = rental_build_final_output(matched)

    write_to_iceberg(spark, final_df)

    log("DONE", "RentalWriter complete", defaulted_prices=defaulted)
    flush_logs()
    spark.stop()
