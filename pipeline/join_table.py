from pyspark.sql import DataFrame

from logger import log

def rental_join_details_search(
    details_df: DataFrame, search_df: DataFrame
) -> tuple[DataFrame, DataFrame]:
    """Inner join + left-anti join on source_id == search_id."""
    log("JOIN", "Joining details × search")
    matched = details_df.join(
        search_df, details_df["source_id"] == search_df["search_id"], how="inner"
    )
    unmatched = details_df.join(
        search_df, details_df["source_id"] == search_df["search_id"], how="left_anti"
    )
    log("JOIN", "Join complete", matched=matched.count(), unmatched=unmatched.count())
    return matched, unmatched


def reviews_join_properties_reviews(
    df_prop: DataFrame, df_rev: DataFrame
) -> DataFrame:
    """Left-join properties and exploded reviews on source_id."""
    log("JOIN", "Joining properties * reviews")
    df_joined = df_prop.join(
        df_rev,
        df_prop["source_id"] == df_rev["source_id_r"],
        how="left",
    )
    log("JOIN", "Join complete", rows=df_joined.count())
    return df_joined
