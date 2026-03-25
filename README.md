# Property Pipeline — PySpark + Apache Iceberg

Reads property and review JSON feeds, writes Iceberg tables in Parquet format,
and generates one enriched JSON file per property.

---

## Project structure

```
property-pipeline/
│
├── config.py                        paths, Iceberg identifiers, defaults
├── logger.py                        structured logger (stdout + JSONL file)
├── spark_utils.py                   shared SparkSession builder
├── main.py                          full pipeline orchestrator
│
├── pipeline/
│   ├── rental_writer.py             step 1 — writes rental_property table
│   ├── reviews_writer.py            step 2 — writes property_reviews table
│   ├── json_generator.py            step 3 — generates property_data/*.json
│   ├── property_joiner.py           step 4 — joins tables → final_data/*.json
│   └── transform.py                 shared transformation functions
│
├── notebooks/
│   └── iceberg_viewer.ipynb         pyspark.sql explorer (12 queries)
│
├── data/
│   ├── raw_data/
│   │   ├── property.json            source — 99 properties
│   │   ├── search.json              source — search/pricing feed
│   │   └── reviews.json             source — 160 reviews
│   ├── property_data/               step 3 output (gitignored)
│   └── final_data/                  step 4 output (gitignored)
│
├── output/
│   └── final_output/                single Parquet backup (gitignored)
│
├── warehouse/                       Iceberg Hadoop catalog (gitignored)
│   └── property_db/
│       ├── rental_property/
│       └── property_reviews/
│       |__ rentals_reviews/
|
├── logs/                            pipeline.jsonl + per-run logs (gitignored)
├── .gitignore
└── README.md
```

---

## Iceberg tables

| Table | Partition | Format | Description |
|-------|-----------|--------|-------------|
| `local.property_db.rental_property` | `country_code` | Parquet/Snappy | Property details + pricing |
| `local.property_db.property_reviews` | `country_code`, `review_year` | Parquet/Snappy | Property details + reviews (one row per review) |

---

## Pipeline steps

### Step 1 — `rental_writer.py`

Reads `property.json` and `search.json`. Extracts, cleans, deduplicates,
and inner-joins both sources. Builds a 13-column standardized DataFrame
and writes it to the `rental_property` Iceberg table partitioned by `country_code`.
Also writes a single-file Parquet backup to `output/final_output/`.

### Step 2 — `reviews_writer.py`

Reads `property.json` and `reviews.json`. Flattens the nested reviews array
(one row per review), left-joins with property fields, enriches with
`gen_id`, `property_slug`, and `data_quality_flag`. Writes to the
`property_reviews` Iceberg table partitioned by `country_code` and `review_year`.

### Step 3 — `json_generator.py`

Reads the `property_reviews` Iceberg table. Aggregates all reviews per property
into a nested list. Uses Spark RDD `.map()` to write one JSON file per property
to `data/property_data/`.

### Step 4 — `property_joiner.py`

Inner-joins `rental_property` × `property_reviews` on `id == gen_id`.
Deduplicates review rows, aggregates per property, and uses Spark RDD `.map()`
to write one fully-enriched JSON file per property to `data/final_data/`.
Runs `expire_snapshots` and `remove_orphan_files` on both source tables.

---

## Run

```bash
# Full pipeline (all 4 steps in order)
python main.py

# Jupyter notebook
jupyter notebook notebooks/iceberg_viewer.ipynb
```

---

## Install

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install pyspark==3.5.0 jupyter matplotlib pandas
# Iceberg JAR downloads automatically from Maven on first Spark run
```
