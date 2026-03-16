"""
main.py
-------
Orchestrates the full property data pipeline end-to-end.

Run each step independently or all at once.

Steps
-----
  1. rental_writer   — read property.json + search.json
                       → write rental_property Iceberg table

  2. reviews_writer  — read property.json + reviews.json
                       → write property_reviews Iceberg table

  3. json_generator  — read property_reviews Iceberg table
                       → write data/property_data/GEN-<id>.json

  4. property_joiner — join rental_property × property_reviews
                       → write data/final_data/GEN-<id>.json

Usage
-----
  # Run the full pipeline
  python main.py

  # Run a single step
  python rental_writer.py
  python reviews_writer.py
  python json_generator.py
  python property_joiner.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from logger import log, flush_logs
import config


def main() -> None:
    log("INIT", "Pipeline starting", app=config.APP_NAME)

    # ── Step 1: write rental_property Iceberg table ────────────────────────────
    log("STEP", "Step 1 — rental_writer")
    from rental_writer import main as rental_main
    rental_main()

    # ── Step 2: write property_reviews Iceberg table ───────────────────────────
    log("STEP", "Step 2 — reviews_writer")
    from reviews_writer import main as reviews_main
    reviews_main()

    # ── Step 3: generate property_data JSON files ──────────────────────────────
    log("STEP", "Step 3 — json_generator")
    from json_generator import main as json_main
    json_main()

    # ── Step 4: join tables → final_data JSON files ────────────────────────────
    log("STEP", "Step 4 — property_joiner")
    from property_joiner import main as joiner_main
    joiner_main()

    log("DONE", "Full pipeline complete")
    flush_logs()


if __name__ == "__main__":
    main()
