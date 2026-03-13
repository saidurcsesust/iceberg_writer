"""
utils/iceberg_writer.py
-----------------------
Iceberg table writer for the PySpark property pipeline.
 
Handles final_output DataFrame only:
  1. Schema preparation  (StructType definition)
  2. Single-file write   (coalesce to 1 Parquet file)
  3. Partition-wise write (Iceberg, partitioned by country_code)
"""
 
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType,
)
 
import config
from logger import log
 
 
# ---------------------------------------------------------------------------
# 1. Schema
# ---------------------------------------------------------------------------
 
RENTAL_PROPERTY_SCHEMA = StructType([
    StructField("id",                StringType(),  False),  # GEN-<source_id>
    StructField("feed_provider_id",  StringType(),  True),
    StructField("property_name",     StringType(),  True),
    StructField("property_slug",     StringType(),  True),
    StructField("country_code",      StringType(),  True),   # partition key
    StructField("currency",          StringType(),  True),
    StructField("usd_price",         DoubleType(),  True),
    StructField("star_rating",       DoubleType(),  True),
    StructField("review_score",      DoubleType(),  True),
    StructField("commission",        DoubleType(),  True),
    StructField("meal_plan",         StringType(),  True),
    StructField("published",         BooleanType(), True),
    StructField("data_quality_flag", StringType(),  True),
])


# ---------------------------------------------------------------------------
# DDL helper
# ---------------------------------------------------------------------------
 
def _create_table_if_not_exists(spark):
    """
    Create the Iceberg rental_property table if it does not already exist.
    Partitioned by country_code — best field for geo-based classification.
    """
    spark.sql(
        f"CREATE DATABASE IF NOT EXISTS "
        f"{config.ICEBERG_CATALOG}.{config.ICEBERG_DATABASE}"
    )
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
        PARTITIONED BY (country_code)
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'snappy'
        )
    """)
    log("DDL", "rental_property Iceberg table ready",
        table=config.ICEBERG_PROPERTY_TABLE)


# ---------------------------------------------------------------------------
# 2. Single-file write
# ---------------------------------------------------------------------------
 
def write_single_file(final_df):
    """
    Write final_output to a single Parquet file by coalescing to 1 partition.
 
    Parameters
    ----------
    final_df : Final output DataFrame.
    """
    log("WRITE", "Single-file write starting", path=config.OUTPUT_FINAL_DIR)
    final_df.coalesce(1).write.mode("overwrite").parquet(config.OUTPUT_FINAL_DIR)
    log("WRITE", "Single-file write complete", path=config.OUTPUT_FINAL_DIR)


# ---------------------------------------------------------------------------
# 3. Partition-wise Iceberg write
# ---------------------------------------------------------------------------
 
def write_iceberg_partitioned(spark, final_df):
    """
    Write final_output to the Iceberg rental_property table,
    partitioned by country_code.
 
    Partition choice: country_code
    - Low cardinality categorical field (US, GB, AE, etc.)
    - Best for geo-based filtering and classification queries
    - Already cleaned (trimmed + uppercased) in transforms.py
 
    Parameters
    ----------
    spark    : Active SparkSession.
    final_df : Final output DataFrame.
    """
    log("WRITE", "Iceberg partitioned write starting",
        table=config.ICEBERG_PROPERTY_TABLE,
        partition=config.PARTITION_PROPERTY)
 
    # Enforce schema column order before writing
    ordered_df = final_df.select(
        [field.name for field in RENTAL_PROPERTY_SCHEMA.fields]
    )
 
    # Ensure table exists
    _create_table_if_not_exists(spark)
 
    # Write partitioned by country_code
    ordered_df.writeTo(config.ICEBERG_PROPERTY_TABLE).overwritePartitions()
 
    log("WRITE", "Iceberg partitioned write complete",
        table=config.ICEBERG_PROPERTY_TABLE)


# ---------------------------------------------------------------------------
# Main entry point called from main.py
# ---------------------------------------------------------------------------
 
def write_final_output(spark, final_df):
    """
    Orchestrate all writes for the final_output DataFrame:
      1. Single-file Parquet write
      2. Partition-wise Iceberg write (partitioned by country_code)
 
    Parameters
    ----------
    spark    : Active SparkSession.
    final_df : Final output DataFrame from build_final_output().
    """
    log("WRITE", "Writing final_output — starting all write steps")
    write_single_file(final_df)
    write_iceberg_partitioned(spark, final_df)
    log("WRITE", "All final_output write steps complete")
 