"""
config.py
---------
Single source of truth for all paths, Iceberg identifiers,
Spark configuration, and pipeline defaults.
"""

import os

# ── Application ────────────────────────────────────────────────────────────────
APP_NAME = "PropertyPipeline"

# ── Directories ────────────────────────────────────────────────────────────────
_PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))

DATA_DIR   = os.path.join(_PROJECT_DIR, "data")
RAW_DATA_DIR = os.path.join(DATA_DIR, "raw_data")
OUTPUT_DIR = os.path.join(_PROJECT_DIR, "output")
LOG_DIR    = os.path.join(_PROJECT_DIR, "logs")

# ── Input files ────────────────────────────────────────────────────────────────
INPUT_DETAILS_FILE = os.path.join(RAW_DATA_DIR, "property.json")
INPUT_SEARCH_FILE  = os.path.join(RAW_DATA_DIR, "search.json")
INPUT_REVIEWS_FILE = os.path.join(RAW_DATA_DIR, "reviews.json")

# ── Output paths ───────────────────────────────────────────────────────────────
OUTPUT_FINAL_DIR    = os.path.join(OUTPUT_DIR, "final_output")   # single Parquet
OUTPUT_PROPERTY_DIR = os.path.join(DATA_DIR,   "property_data")  # json_generator
OUTPUT_FINAL_JSON   = os.path.join(DATA_DIR,   "final_data")     # property_joiner

# ── Iceberg catalog ────────────────────────────────────────────────────────────
ICEBERG_CATALOG   = "local"
ICEBERG_WAREHOUSE = os.path.join(_PROJECT_DIR, "warehouse")
ICEBERG_DATABASE  = "property_db"

# ── Iceberg tables ─────────────────────────────────────────────────────────────
ICEBERG_RENTALS_TABLE = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.rental_property"
ICEBERG_REVIEWS_TABLE  = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.property_reviews"
ICEBERG_RENTALS_REVIEWS_TABLE   = f"{ICEBERG_CATALOG}.{ICEBERG_DATABASE}.rentals_reviews"

# ── Partition keys ─────────────────────────────────────────────────────────────
PARTITION_PROPERTY = "country_code"
PARTITION_REVIEWS_1 = "country_code"
PARTITION_REVIEWS_2 = "review_year"

# ── Spark ──────────────────────────────────────────────────────────────────────
ICEBERG_JAR = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2"

# ── Maintenance ────────────────────────────────────────────────────────────────
# Default retention window for snapshot cleanup helpers.
MAINTENANCE_RETENTION_DAYS = 1
# None uses snapshot.py's default cutoff of 1 day ago.
MAINTENANCE_EXPIRE_OLDER_THAN = None
MAINTENANCE_RETAIN_LAST = 1
MAINTENANCE_REMOVE_ORPHAN_OLDER_THAN = None
MAINTENANCE_METADATA_DELETE_AFTER_COMMIT = "true"
MAINTENANCE_METADATA_PREVIOUS_VERSIONS_MAX = 1

# ── Field defaults ─────────────────────────────────────────────────────────────
DEFAULT_CURRENCY     = "USD"
DEFAULT_USD_PRICE    = 0.0
DEFAULT_STAR_RATING  = 0.0
DEFAULT_REVIEW_SCORE = 0.0
DEFAULT_PUBLISHED    = True

# ── Language preference for name resolution ────────────────────────────────────
NAME_PREFERENCE = ["en-us", "en", "de", "fr", "es"]

# ── Ensure runtime directories exist ──────────────────────────────────────────
for _d in (OUTPUT_DIR, LOG_DIR, RAW_DATA_DIR, OUTPUT_FINAL_DIR,
           OUTPUT_PROPERTY_DIR, OUTPUT_FINAL_JSON):
    os.makedirs(_d, exist_ok=True)
