"""
transform.py
------------
Shared DataFrame transformation functions for pipeline steps.
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    StringType,
)

import config
from logger import log

REVIEWS_TABLE_GROUP_COLUMNS = [
    "property_id",
    "gen_id",
    "property_name",
    "property_slug",
    "country_code",
    "currency",
    "star_rating",
    "review_score",
    "published",
    "data_quality_flag",
]

JOINED_RENTAL_GROUP_COLUMNS = [
    "id",
    "feed_provider_id",
    "property_name",
    "property_slug",
    "country_code",
    "currency",
    "usd_price",
    "star_rating",
    "review_score",
    "commission",
    "meal_plan",
    "published",
    "data_quality_flag",
]


# -----------------------------------------------------------------------------
# Rental writer transformations
# -----------------------------------------------------------------------------

def _preferred_name_column(df: DataFrame) -> F.Column:
    name_fields = [f.name for f in df.schema["name"].dataType.fields]
    ordered = (
        [f for f in config.NAME_PREFERENCE if f in name_fields]
        + [f for f in name_fields if f not in config.NAME_PREFERENCE]
    )
    name_cols = [F.col(f"`name`.`{field}`") for field in ordered]
    return F.coalesce(*name_cols) if len(name_cols) > 1 else name_cols[0]




def rental_extract_details(df: DataFrame) -> DataFrame:
    """Flatten property.json --> source_id, property_name, country_code,
    currency, star_rating, review_score."""
    log("EXTRACT", "Extracting details fields")

    return df.select(
        F.col("id").cast(StringType()).alias("source_id"),
        _preferred_name_column(df).alias("property_name"),
        F.trim(F.upper(F.col("location.country"))).alias("country_code"),
        F.col("currency"),
        F.col("rating.stars").cast(DoubleType()).alias("star_rating"),
        F.col("rating.review_score").cast(DoubleType()).alias("review_score"),
    )


def rental_extract_search(df: DataFrame) -> DataFrame:
    """Flatten search.json --> search_id, usd_price, commission_pct,
    meal_plan, deep_link_url, search_currency."""
    log("EXTRACT", "Extracting search fields")
    return df.select(
        F.col("id").cast(StringType()).alias("search_id"),
        F.col("price.book").cast(DoubleType()).alias("usd_price"),
        F.col("commission.percentage").cast(DoubleType()).alias("commission_pct"),
        F.col("products").getItem(0)
         .getField("policies").getField("meal_plan").getField("meals")
         .cast(StringType()).alias("meal_plan"),
        F.col("deep_link_url"),
        F.col("currency").alias("search_currency"),
    )


def rental_clean(df: DataFrame) -> DataFrame:
    before = df.count()
    cleaned = df.filter(F.col("source_id").isNotNull() & (F.col("source_id") != ""))
    dropped = before - cleaned.count()
    log("CLEAN", "Dropped null source_id rows", dropped=dropped)
    return cleaned


def rental_deduplicate(df: DataFrame, key: str) -> DataFrame:
    before = df.count()
    df_dedup = df.dropDuplicates([key])
    log("DEDUP", f"Deduplicated on '{key}'", removed=before - df_dedup.count())
    return df_dedup



# -----------------------------------------------------------------------------
# Reviews writer transformations
# -----------------------------------------------------------------------------

def reviews_flatten_properties(df: DataFrame) -> DataFrame:
    """Filter nulls, resolve localised name, extract core fields."""
    log("EXTRACT", "Flattening properties")
    df_clean = df.filter(F.col("id").isNotNull())

    return df_clean.select(
        F.col("id").cast("long").alias("source_id"),
        _preferred_name_column(df).alias("raw_name"),
        F.col("location.city").alias("raw_city"),
        F.col("currency").alias("raw_currency"),
        F.col("location.country").alias("raw_country"),
        F.col("rating.stars").cast("double").alias("star_rating_raw"),
        F.col("rating.review_score").cast("double").alias("review_score_raw"),
    ).withColumn(
        "property_name",
        F.when(
            F.col("raw_name").isNotNull() & (F.trim(F.col("raw_name")) != ""),
            F.trim(F.col("raw_name")),
        ).otherwise(F.trim(F.col("raw_city"))),
    )


def reviews_flatten_reviews(df: DataFrame) -> DataFrame:
    """Explode the reviews array — one row per review."""
    log("EXTRACT", "Exploding reviews")
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


# -----------------------------------------------------------------------------
# Json generator transformations
# -----------------------------------------------------------------------------

_REVIEW_STRUCT_COLUMNS = (
    "review_id",
    "review_date",
    "review_year",
    "review_individual_score",
    "review_language",
    "review_summary",
    "review_positive",
    "review_negative",
    "reviewer_name",
    "reviewer_country",
    "reviewer_travel_purpose",
    "reviewer_type",
)


def deduplicate_reviews(df: DataFrame) -> DataFrame:
    before = df.count()
    df_dedup = df.dropDuplicates(["gen_id", "review_id"])
    dropped = before - df_dedup.count()
    if dropped:
        log("DEDUP", "Removed duplicate review rows", dropped=dropped)
    else:
        log("DEDUP", "No duplicates found", rows=df_dedup.count())
    return df_dedup


def deduplicate_joined_rental_reviews(df: DataFrame) -> DataFrame:
    before = df.count()
    df_dedup = df.dropDuplicates(["id", "review_id"])
    dropped = before - df_dedup.count()
    if dropped:
        log("DEDUP", "Removed duplicate joined rental-review rows", dropped=dropped)
    else:
        log("DEDUP", "No joined duplicates found", rows=df_dedup.count())
    return df_dedup


def aggregate_reviews_per_property(
    df: DataFrame, group_columns: list[str], order_column: str
) -> DataFrame:
    df_struct = df.withColumn(
        "review_struct",
        F.when(
            F.col("review_id").isNotNull(),
            F.struct(*[F.col(col) for col in _REVIEW_STRUCT_COLUMNS]),
        ).otherwise(F.lit(None)),
    )

    df_agg = (
        df_struct
        .groupBy(*group_columns)
        .agg(
            F.collect_list(
                F.when(F.col("review_struct").isNotNull(), F.col("review_struct"))
            ).alias("reviews")
        )
        .orderBy(order_column)
    )

    log("AGG", "Aggregated per property", distinct_properties=df_agg.count())
    return df_agg
