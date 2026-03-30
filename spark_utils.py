"""
spark_utils.py
--------------
Shared SparkSession builder for pipeline steps.
"""

from __future__ import annotations

from typing import Mapping

from pyspark.sql import SparkSession

import config


def build_spark(app_name: str, extra_conf: Mapping[str, str] | None = None) -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.jars.packages", config.ICEBERG_JAR)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{config.ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{config.ICEBERG_CATALOG}.type", "hadoop")
        .config(
            f"spark.sql.catalog.{config.ICEBERG_CATALOG}.warehouse",
            config.ICEBERG_WAREHOUSE,
        )
        .config("spark.sql.iceberg.write.format.default", "parquet")
        # Use Kryo for faster JVM-side serialization.
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Enable Arrow optimization for pandas UDFs when available.
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if extra_conf:
        for key, value in extra_conf.items():
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    print(f"[{app_name}] SparkSession ready")
    return spark
