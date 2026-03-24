"""
main.py
-------
Orchestrates the full property data pipeline end-to-end.

Steps
-----
  1. rental_writer   — read property.json + search.json
                       → write rental_property Iceberg table

  2. reviews_writer  — read property.json + reviews.json
                       → write property_reviews Iceberg table

  3. property_joiner — join rental_property × property_reviews
                       → write data/final_data/GEN-<id>.json

Usage
-----
  # Run the full pipeline
  python main.py
"""

from logger import log, flush_logs
import config
from pipeline.rental_writer import run as run_rental_writer
from pipeline.reviews_writer import run as run_reviews_writer
from pipeline.property_joiner import run as run_property_joiner


def main() -> None:
    log("INIT", "Pipeline starting", app=config.APP_NAME)

    log("STEP", "Step 1 — rental_writer")
    run_rental_writer()

    log("STEP", "Step 2 — reviews_writer")
    run_reviews_writer()

    log("STEP", "Step 3 — property_joiner")
    run_property_joiner()

    log("DONE", "Full pipeline complete")
    flush_logs()


if __name__ == "__main__":
    main()
