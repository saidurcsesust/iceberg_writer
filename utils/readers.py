"""
utils/readers.py
----------------
JSON file readers for the PySpark property pipeline.
"""

from logger import log


def read_json(spark, path, label):
    """
    Read a multiline JSON file into a Spark DataFrame.

    Parameters
    ----------
    spark : Active SparkSession.
    path  : Path to the JSON file.
    label : Human-readable label used in logs.

    Returns
    -------
    DataFrame
    """
    log("READ", f"Reading {label}", path=path)
    df = spark.read.option("multiline", "true").json(path)
    log("READ", f"{label} loaded", row_count=df.count(), columns=df.columns)
    return df