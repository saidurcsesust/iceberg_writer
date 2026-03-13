"""
utils/transforms.py
-------------------
Field extraction and output transformation functions
for the PySpark property pipeline.
"""

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, BooleanType

import config
from logger import log


# ---------------------------------------------------------------------------
# Details extraction
# ---------------------------------------------------------------------------

def extract_details_fields(df):
    """
    Flatten and rename the fields we need from details.json.

    Extracted columns
    -----------------
    source_id, property_name, country_code, currency,
    star_rating, review_score

    Parameters
    ----------
    df : Raw details DataFrame.

    Returns
    -------
    DataFrame
    """
    log("EXTRACT", "Extracting fields from details DataFrame")

    extracted = df.select(
        F.col("id").cast(StringType()).alias("source_id"),

        # name is a StructType - access en-us field using getField()
        F.col("name").getField("en-us").alias("property_name"),

        # location
        F.trim(F.upper(F.col("location.country"))).alias("country_code"),

        # currency
        F.col("currency"),

        # rating nested fields
        F.col("rating.stars").cast(DoubleType()).alias("star_rating"),
        F.col("rating.review_score").cast(DoubleType()).alias("review_score"),
    )

    log("EXTRACT", "Details extraction complete", columns=extracted.columns)
    return extracted


# ---------------------------------------------------------------------------
# Search extraction
# ---------------------------------------------------------------------------

def extract_search_fields(df):
    """
    Flatten and rename the fields we need from search.json.

    Extracted columns
    -----------------
    search_id, usd_price, commission_pct, meal_plan,
    deep_link_url, search_currency

    Parameters
    ----------
    df : Raw search DataFrame.

    Returns
    -------
    DataFrame
    """
    log("EXTRACT", "Extracting fields from search DataFrame")

    extracted = df.select(
        F.col("id").cast(StringType()).alias("search_id"),

        # usd_price: use price.book as the booker currency price
        F.col("price.book").cast(DoubleType()).alias("usd_price"),

        # commission percentage
        F.col("commission.percentage").cast(DoubleType()).alias("commission_pct"),

        # meal_plan: first product's meal_plan meals list -> cast as string
        F.col("products").getItem(0)
         .getField("policies")
         .getField("meal_plan")
         .getField("meals")
         .cast(StringType())
         .alias("meal_plan"),

        # deep_link_url for data quality checks
        F.col("deep_link_url"),

        # keep currency for reference
        F.col("currency").alias("search_currency"),
    )

    log("EXTRACT", "Search extraction complete", columns=extracted.columns)
    return extracted


# ---------------------------------------------------------------------------
# Slug helper
# ---------------------------------------------------------------------------

def make_slug(name_col):
    """
    Convert a property name column to a lowercase dash-separated slug.

    Parameters
    ----------
    name_col : Column expression referencing the property name.

    Returns
    -------
    Column expression
    """
    return F.lower(F.regexp_replace(F.trim(name_col), r"[^a-zA-Z0-9]+", "-"))


# ---------------------------------------------------------------------------
# Final output builder
# ---------------------------------------------------------------------------

def build_final_output(matched_df):
    """
    Apply all output-field rules to matched_details to produce the
    standardized 12-column final output plus the bonus data_quality_flag.

    Rules applied
    -------------
    - id               = 'GEN-' + source_id
    - feed_provider_id = source_id
    - property_name    = from details
    - property_slug    = lowercase dash-separated from property_name
    - country_code     = trimmed uppercase (flagged if != 2 chars)
    - currency         = 'USD' if missing
    - usd_price        = price.book, default 0.0
    - star_rating      = rating.stars, default 0.0
    - review_score     = rating.review_score, default 0.0
    - commission       = commission_pct from search
    - meal_plan        = from first product in search
    - published        = always true
    - data_quality_flag= GOOD or NEEDS_REVIEW

    Parameters
    ----------
    matched_df : Inner-joined details + search DataFrame.

    Returns
    -------
    (final_df, defaulted_usd_price_count)
    """
    log("TRANSFORM", "Building final standardized output")

    # Count rows where usd_price will be defaulted before applying the default
    defaulted_price_count = matched_df.filter(F.col("usd_price").isNull()).count()

    final = matched_df.select(
        F.concat_ws("-", F.lit("GEN"), F.col("source_id")).alias("id"),
        F.col("source_id").alias("feed_provider_id"),
        F.col("property_name"),
        make_slug(F.col("property_name")).alias("property_slug"),
        F.col("country_code"),
        F.coalesce(F.col("currency"), F.lit(config.DEFAULT_CURRENCY)).alias("currency"),
        F.coalesce(
            F.col("usd_price"), F.lit(config.DEFAULT_USD_PRICE)
        ).cast(DoubleType()).alias("usd_price"),
        F.coalesce(
            F.col("star_rating"), F.lit(config.DEFAULT_STAR_RATING)
        ).cast(DoubleType()).alias("star_rating"),
        F.coalesce(
            F.col("review_score"), F.lit(config.DEFAULT_REVIEW_SCORE)
        ).cast(DoubleType()).alias("review_score"),
        F.col("commission_pct").alias("commission"),
        F.col("meal_plan"),
        F.lit(config.DEFAULT_PUBLISHED).cast(BooleanType()).alias("published"),
    )

    # Bonus: data_quality_flag
    final = final.withColumn(
        "data_quality_flag",
        F.when(
            F.col("property_name").isNull()
            | F.col("usd_price").isNull()
            | (F.length(F.col("country_code")) != 2),
            F.lit("NEEDS_REVIEW"),
        ).otherwise(F.lit("GOOD")),
    )

    log(
        "TRANSFORM", "Final output built",
        columns=final.columns,
        total_columns=len(final.columns),
        defaulted_usd_price=defaulted_price_count,
    )

    return final, defaulted_price_count