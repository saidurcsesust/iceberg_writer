from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from datetime import datetime
from logger import build_logger

import os

# Constants

_SCRIPT_DIR  = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR    = os.path.join(_SCRIPT_DIR, "data")
_OUTPUT_DIR  = os.path.join(_SCRIPT_DIR, "output")
_REPORT_PATH = os.path.join(_OUTPUT_DIR, "validation_report.txt")

_PATH_PROPERTY = os.path.join(_DATA_DIR, "property.json")
_PATH_REVIEWS  = os.path.join(_DATA_DIR, "reviews.json")

_FINAL_COLS = [
    "id", "feed_provider_id", "property_name", "property_slug",
    "country_code", "currency", "usd_price", "star_rating",
    "review_score", "published",
]

_NAME_PREFERENCE = ["en-us", "en", "de", "fr", "es"]

os.makedirs(_OUTPUT_DIR, exist_ok=True)
log, log_path = build_logger(__file__)


# Spark session

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("PropertyFeedPipeline")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    log.info("SparkSession created", extra={"step": "spark_init"})
    return spark


# Ingestion

def load_json(spark: SparkSession, path: str) -> DataFrame:
    return spark.read.option("multiLine", "true").json(path)


def load_sources(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    df_property = load_json(spark, _PATH_PROPERTY)
    df_reviews  = load_json(spark, _PATH_REVIEWS)

    log.info("source_a loaded — %d rows", df_property.count(), extra={"step": "load"})
    log.info("source_b loaded — %d rows", df_reviews.count(),  extra={"step": "load"})

    return df_property, df_reviews


# Transformation helpers

def _make_slug(col: F.Column) -> F.Column:
    """Convert a string column to a URL-safe slug."""
    return (
        F.regexp_replace(                        # strip leading/trailing dashes
            F.regexp_replace(                    # collapse multiple dashes to one
                F.regexp_replace(                # spaces and underscores to dash
                    F.regexp_replace(            # remove unwanted characters
                        F.trim(F.lower(col)),
                        r"[^\w\s-]", ""
                    ),
                    r"[\s_]+", "-"
                ),
                r"-{2,}", "-"
            ),
            r"^-|-$", ""
        )
    )


def _resolve_name_col(df: DataFrame) -> F.Column:
    """Pick the best available localised name, honouring language preference."""
    name_fields = [f.name for f in df.schema["name"].dataType.fields]
    ordered = (
        [f for f in _NAME_PREFERENCE if f in name_fields]
        + [f for f in name_fields if f not in _NAME_PREFERENCE]
    )
    cols = [F.col(f"`name`.`{field}`") for field in ordered]
    return F.coalesce(*cols) if len(cols) > 1 else cols[0]


def _build_property_name(name_col: F.Column, city_col: F.Column) -> F.Column:
    """
    Concatenate localised name and city as '<name> - <city>'.
    """
    name_ok = name_col.isNotNull() & (F.trim(name_col) != "")
    city_ok = city_col.isNotNull() & (F.trim(city_col) != "")

    return (
        F.when(name_ok & city_ok,
               F.concat(F.trim(name_col), F.trim(city_col)))
        .when(name_ok, F.trim(name_col))
        .otherwise(F.trim(city_col))
    )


# Flattening

def flatten_properties(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Filter rows without an id, then project and flatten property fields.
    Returns (flattened_df, n_dropped).
    """
    total    = df.count()
    df_clean = df.filter(F.col("id").isNotNull())
    dropped  = total - df_clean.count()

    log.info(
        "Null id filter — dropped %d row(s)", dropped,
        extra={"step": "filter", "dropped": dropped},
    )

    name_col = _resolve_name_col(df)
    city_col = F.col("location.city")

    return (
        df_clean.select(
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
    ), dropped


def flatten_reviews(df: DataFrame) -> DataFrame:
    """
    Explode the nested reviews array so each review becomes its own row.
    Properties with an empty reviews list produce one null-review row (explode_outer).
    """
    df_flat = (
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

    log.info("Reviews exploded — %d rows", df_flat.count(), extra={"step": "explode"})
    return df_flat


#Left Join

def join_property_reviews(
    df_property: DataFrame,
    df_reviews: DataFrame,
) -> DataFrame:
    """Left-join properties with exploded reviews on source_id."""
    df_joined = df_property.join(
        df_reviews,
        df_property["source_id"] == df_reviews["source_id_r"],
        how="left",
    )
    log.info("Join complete — %d rows", df_joined.count(), extra={"step": "join"})
    return df_joined


# Column enrichment

def enrich_columns(df: DataFrame) -> DataFrame:
    """Add/derive all output columns from the joined dataset."""
    return (
        df
        .withColumn("id",
            F.concat(F.lit("GEN-"), F.col("source_id").cast("string")))
        .withColumn("feed_provider_id",
            F.col("source_id").cast("string"))
        .withColumn("property_slug",
            _make_slug(F.col("property_name")))
        .withColumn("country_code",
            F.upper(F.col("raw_country")))
        .withColumn("currency",
            F.when(
                F.col("raw_currency").isNull() | (F.trim(F.col("raw_currency")) == ""),
                F.lit("USD"),
            ).otherwise(F.col("raw_currency")))
        .withColumn("usd_price",
            F.lit(0.0).cast("double"))
        .withColumn("star_rating",
            F.coalesce(F.col("star_rating_raw"), F.lit(0.0)).cast("double"))
        .withColumn("review_score",
            F.coalesce(F.col("review_score_raw"), F.lit(0.0)).cast("double"))
        .withColumn("published",
            F.lit(True))
    )


def add_data_quality_flag(df: DataFrame) -> DataFrame:
    """Append a GOOD / NEEDS_REVIEW flag based on key field completeness."""
    needs_review = (
        F.col("property_name").isNull()  | (F.trim(F.col("property_name")) == "")
        | F.col("country_code").isNull() | (F.trim(F.col("country_code")) == "")
        | (F.col("review_score") == 0.0)
        | (F.col("star_rating")  == 0.0)
    )
    return df.withColumn(
        "data_quality_flag",
        F.when(needs_review, F.lit("NEEDS_REVIEW")).otherwise(F.lit("GOOD")),
    )


# Reporting

def write_validation_report(
    count_a: int,
    count_b: int,
    count_joined: int,
    count_final: int,
    dropped: int,
    schema_str: str,
    n_columns: int,
) -> None:
    col_ok = "YES" if n_columns == 10 else "NO - MISMATCH"

    lines = [
        "=" * 60,
        "  VALIDATION REPORT",
        f"  Generated : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"  Script    : {os.path.basename(__file__)}",
        f"  Log file  : {log_path}",
        "=" * 60,
        "",
        "ROW COUNTS",
        "-" * 40,
        f"  source_a.json rows          : {count_a}",
        f"  source_b.json rows          : {count_b}",
        f"  Rows dropped (null id)      : {dropped}",
        f"  Rows after join             : {count_joined}",
        f"  Final output rows           : {count_final}",
        "",
        "COLUMN CHECK",
        "-" * 40,
        f"  Expected columns            : 10",
        f"  Actual columns              : {n_columns}",
        f"  Column count OK             : {col_ok}",
        "",
        "FINAL OUTPUT SCHEMA",
        "-" * 40,
        schema_str,
        "=" * 60,
    ]

    with open(_REPORT_PATH, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines))

    log.info("Validation report written -> %s", _REPORT_PATH, extra={"step": "report"})


# entry point

def main() -> None:
    spark = build_spark()

    # 1. Load
    df_property_raw, df_reviews_raw = load_sources(spark)
    count_a = df_property_raw.count()
    count_b = df_reviews_raw.count()

    # 2. Flatten
    df_property_flat, dropped = flatten_properties(df_property_raw)
    df_reviews_flat            = flatten_reviews(df_reviews_raw)

    # 3. Join
    df_joined    = join_property_reviews(df_property_flat, df_reviews_flat)
    count_joined = df_joined.count()

    # 4. Enrich
    df_output = enrich_columns(df_joined)
    log.info("Column enrichment complete", extra={"step": "transform"})

    # 5. Select final columns
    df_final    = df_output.select(*_FINAL_COLS)
    count_final = df_final.count()
    n_columns   = len(df_final.columns)

    schema_str = "\n".join(
        f"  |-- {f.name}: {f.dataType.simpleString()} (nullable = {f.nullable})"
        for f in df_final.schema.fields
    )

    # 6. Show & report
    print("\n=== FINAL OUTPUT ===")
    df_final.show(truncate=True)

    write_validation_report(
        count_a=count_a,
        count_b=count_b,
        count_joined=count_joined,
        count_final=count_final,
        dropped=dropped,
        schema_str=schema_str,
        n_columns=n_columns,
    )

if __name__ == "__main__":
    main()