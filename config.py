"""
config.py
---------
Central configuration for the PySpark property pipeline.
"""

# ---------------------------------------------------------------------------
# Input paths
# ---------------------------------------------------------------------------
INPUT_DETAILS_FILE = "data/property.json"
INPUT_SEARCH_FILE  = "data/search.json"

# ---------------------------------------------------------------------------
# Default values
# ---------------------------------------------------------------------------
DEFAULT_CURRENCY     = "USD"
DEFAULT_USD_PRICE    = 0.0
DEFAULT_STAR_RATING  = 0.0
DEFAULT_REVIEW_SCORE = 0.0
DEFAULT_PUBLISHED    = True

# ---------------------------------------------------------------------------
# Output paths
# ---------------------------------------------------------------------------
OUTPUT_FINAL_DIR       = "data/output/final_output"
OUTPUT_UNMATCHED_DIR   = "data/output/unmatched_details"
VALIDATION_REPORT_PATH = "validation_report.txt"

# ---------------------------------------------------------------------------
# Iceberg configuration
# ---------------------------------------------------------------------------
ICEBERG_WAREHOUSE      = "data/iceberg_warehouse"
ICEBERG_CATALOG        = "local"
ICEBERG_DATABASE       = "property_db"
ICEBERG_PROPERTY_TABLE = "local.property_db.rental_property"
PARTITION_PROPERTY     = "country_code"

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_BASE_DIR = "logs"

# ---------------------------------------------------------------------------
# App name
# ---------------------------------------------------------------------------
APP_NAME    = "PropertySearchPipeline"
SCRIPT_NAME = "main"