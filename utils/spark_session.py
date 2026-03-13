"""
utils/spark_session.py
----------------------
SparkSession factory with Apache Iceberg support.
Uses the iceberg-spark-runtime JAR via the spark.jars.packages config.
"""

from pyspark.sql import SparkSession
import config


def create_spark_session(app_name):
    """
    Create and return a local SparkSession configured for Apache Iceberg.

    Iceberg integration
    -------------------
    - Adds iceberg-spark-runtime via spark.jars.packages
    - Registers the 'local' catalog backed by HadoopCatalog
    - Warehouse stored at config.ICEBERG_WAREHOUSE

    Parameters
    ----------
    app_name : Name shown in the Spark UI.

    Returns
    -------
    SparkSession
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")

        # ── Iceberg runtime JAR ──────────────────────────────────────────
        .config(
            "spark.jars.packages",
            "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
        )

        # ── Iceberg catalog ──────────────────────────────────────────────
        .config(
            f"spark.sql.catalog.{config.ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(
            f"spark.sql.catalog.{config.ICEBERG_CATALOG}.type",
            "hadoop",
        )
        .config(
            f"spark.sql.catalog.{config.ICEBERG_CATALOG}.warehouse",
            config.ICEBERG_WAREHOUSE,
        )

        # ── Iceberg SQL extensions ───────────────────────────────────────
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )

        # ── Spark tuning ─────────────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark