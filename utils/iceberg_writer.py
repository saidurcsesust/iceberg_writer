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