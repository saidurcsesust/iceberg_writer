from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType

from logger import log
import config



def _make_slug(col: F.Column, ascii_only: bool = False) -> F.Column:
    if ascii_only:
        return F.lower(F.regexp_replace(F.trim(col), r"[^a-zA-Z0-9]+", "-"))

    return (
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(
                    F.regexp_replace(F.trim(F.lower(col)), r"[^\w\s-]", ""),
                    r"[\s_]+", "-"),
                r"-{2,}", "-"),
            r"^-|-$", "")
    )

def rental_build_final_output(matched_df: DataFrame) -> tuple[DataFrame, int]:
    """Produce the 13-column standardized DataFrame + data_quality_flag."""
    log("TRANSFORM", "Building final output")
    defaulted = matched_df.filter(F.col("usd_price").isNull()).count()

    df = matched_df.select(
        F.concat_ws("-", F.lit("GEN"), F.col("source_id")).alias("id"),
        F.col("source_id").alias("feed_provider_id"),
        F.col("property_name"),
        _make_slug(F.col("property_name"), ascii_only=True).alias("property_slug"),
        F.col("country_code"),
        F.coalesce(F.col("currency"), F.lit(config.DEFAULT_CURRENCY)).alias("currency"),
        F.coalesce(F.col("usd_price"), F.lit(config.DEFAULT_USD_PRICE))
         .cast(DoubleType()).alias("usd_price"),
        F.coalesce(F.col("star_rating"), F.lit(config.DEFAULT_STAR_RATING))
         .cast(DoubleType()).alias("star_rating"),
        F.coalesce(F.col("review_score"), F.lit(config.DEFAULT_REVIEW_SCORE))
         .cast(DoubleType()).alias("review_score"),
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

    log(
        "TRANSFORM",
        "Final output ready",
        rows=df.count(),
        columns=len(df.columns),
        defaulted_prices=defaulted,
    )
    return df, defaulted


def reviews_enrich_joined_data(df: DataFrame) -> DataFrame:
    """Add generated ids, normalized fields, and quality flags."""
    return (
        df
        .withColumn("gen_id", F.concat(F.lit("GEN-"), F.col("source_id").cast("string")))
        .withColumn("feed_provider_id", F.col("source_id").cast("string"))
        .withColumn("property_slug", _make_slug(F.col("property_name")))
        .withColumn("country_code", F.upper(F.col("raw_country")))
        .withColumn(
            "currency",
            F.when(
                F.col("raw_currency").isNull() | (F.trim(F.col("raw_currency")) == ""),
                F.lit(config.DEFAULT_CURRENCY),
            ).otherwise(F.col("raw_currency")),
        )
        .withColumn(
            "star_rating",
            F.coalesce(F.col("star_rating_raw"), F.lit(config.DEFAULT_STAR_RATING)),
        )
        .withColumn(
            "review_score",
            F.coalesce(F.col("review_score_raw"), F.lit(config.DEFAULT_REVIEW_SCORE)),
        )
        .withColumn("published", F.lit(config.DEFAULT_PUBLISHED).cast(BooleanType()))
        .withColumn(
            "data_quality_flag",
            F.when(
                F.col("property_name").isNull()
                | (F.trim(F.col("property_name")) == "")
                | F.col("country_code").isNull()
                | (F.col("review_score") == 0.0)
                | (F.col("star_rating") == 0.0),
                F.lit("NEEDS_REVIEW"),
            ).otherwise(F.lit("GOOD")),
        )
    )


def reviews_prepare_for_iceberg(df: DataFrame) -> DataFrame:
    """Select final review columns and derive the review_year partition."""
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
            F.col("source_id").cast(LongType()).alias("property_id"),
            F.col("gen_id"),
            F.col("property_name"),
            F.col("property_slug"),
            F.col("country_code"),
            F.col("currency"),
            F.col("star_rating").cast(DoubleType()),
            F.col("review_score").cast(DoubleType()),
            F.col("published").cast(BooleanType()),
            F.col("data_quality_flag"),
            F.col("review_id").cast(LongType()),
            F.col("review_date"),
            F.col("review_year").cast(IntegerType()),
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
