"""
PySpark job: raw JSON from the data lake -> one clean table (staging.stg_event).

What it does, in order:
  1. Finds the raw response.json files of a run (USGS and EONET).
  2. Reads them with an explicit schema (no automatic guessing).
  3. Turns both sources into the same columns (the "data contract").
  4. Maps every event to one of our hazard types.
  5. Rejects bad rows and counts why (missing id, quarry blast, ...).
  6. Removes duplicates: for the same event_key the newest version wins.
  7. Writes the result to PostgreSQL (or to Parquet files for tests).

Run it:
  spark-submit --master local[*] --jars /opt/postgresql.jar /opt/airflow/src/transform.py \
      --input-dir /opt/airflow/work/<safe_run_id>/raw --run-id <safe_run_id> --output jdbc

The input folder must keep the lake layout, for example:
  <input-dir>/source=usgs/ingest_date=.../run_id=.../response.json
  <input-dir>/source=eonet/ingest_date=.../run_id=.../response.json
"""

import argparse
import glob
import json
import os

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


# =====================================================================
# Schemas (written by hand, so a strange file fails instead of being guessed)
# =====================================================================

USGS_SCHEMA = StructType([
    StructField("features", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("properties", StructType([
            StructField("time", LongType()),      # milliseconds since 1970
            StructField("updated", LongType()),   # milliseconds since 1970
            StructField("place", StringType()),
            StructField("mag", DoubleType()),
            StructField("type", StringType()),
        ])),
        StructField("geometry", StructType([
            StructField("coordinates", ArrayType(DoubleType())),  # lon, lat, depth
        ])),
    ]))),
])

# EONET coordinates are a point [lon, lat] for some events and nested lists for
# polygons, so we read them as plain text first and open only the points later.
EONET_SCHEMA = StructType([
    StructField("events", ArrayType(StructType([
        StructField("id", StringType()),
        StructField("title", StringType()),
        StructField("closed", StringType()),
        StructField("categories", ArrayType(StructType([
            StructField("id", StringType()),
        ]))),
        StructField("geometry", ArrayType(StructType([
            StructField("date", StringType()),
            StructField("type", StringType()),
            StructField("coordinates", StringType()),
        ]))),
    ]))),
])


# =====================================================================
# Hazard mapping (source, original category, our hazard type)
# =====================================================================

HAZARD_MAP = [
    ("usgs",  "earthquake",   "earthquake"),
    ("eonet", "earthquakes",  "earthquake"),
    ("eonet", "volcanoes",    "volcano"),
    ("eonet", "landslides",   "landslide"),
    ("eonet", "wildfires",    "wildfire"),
    ("eonet", "drought",      "drought"),
    ("eonet", "tempExtremes", "temperature_extreme"),
    ("eonet", "severeStorms", "severe_storm"),
    ("eonet", "dustHaze",     "dust_haze"),
    ("eonet", "snow",         "snow"),
    ("eonet", "floods",       "flood"),
    ("eonet", "seaLakeIce",   "sea_lake_ice"),
    ("eonet", "waterColor",   "water_color"),
]

# Columns of staging.stg_event, in table order.
COLUMNS = [
    "event_key", "source", "source_event_id", "hazard_type", "source_category",
    "title", "event_time_utc", "latitude", "longitude", "magnitude", "depth_km",
    "event_status", "source_updated_utc", "has_valid_point", "geometry_count",
    "raw_object_path", "run_id", "ingested_at_utc",
]


# =====================================================================
# Reading the raw files
# =====================================================================

def find_raw_files(input_dir):
    """Find every response.json in the run folder and sort them by source."""
    usgs_files, eonet_files = [], []
    pattern = os.path.join(input_dir, "**", "response.json")
    for path in sorted(glob.glob(pattern, recursive=True)):
        normal = path.replace("\\", "/")
        if "source=usgs" in normal:
            usgs_files.append(path)
        elif "source=eonet" in normal:
            eonet_files.append(path)
    return usgs_files, eonet_files


def _read_json(spark, files, schema):
    return spark.read.schema(schema).option("multiLine", True).json(files)


def read_usgs(spark, files):
    """USGS earthquakes -> one row per earthquake, in the shared columns."""
    raw = _read_json(spark, files, USGS_SCHEMA).select(
        F.input_file_name().alias("raw_object_path"),
        F.explode("features").alias("f"),
    )
    lon = F.col("f.geometry.coordinates")[0]
    lat = F.col("f.geometry.coordinates")[1]
    good_point = lon.between(-180, 180) & lat.between(-90, 90)

    return raw.select(
        F.lit("usgs").alias("source"),
        F.col("f.id").alias("source_event_id"),
        F.col("f.properties.type").alias("source_category"),
        F.col("f.properties.place").alias("title"),
        F.timestamp_millis(F.col("f.properties.time")).alias("event_time_utc"),
        F.when(good_point, lat).alias("latitude"),     # empty if out of range
        F.when(good_point, lon).alias("longitude"),
        F.col("f.properties.mag").alias("magnitude"),  # can be empty
        F.col("f.geometry.coordinates")[2].alias("depth_km"),
        F.lit(None).cast("string").alias("event_status"),
        F.timestamp_millis(F.col("f.properties.updated")).alias("source_updated_utc"),
        F.lit(1).alias("geometry_count"),
        "raw_object_path",
    )


def read_eonet(spark, files):
    """
    EONET events -> one row per event, in the shared columns.

    Location rule: use the most recent valid point. If the event only has
    polygons, latitude and longitude stay empty (we never invent a center).
    """
    raw = _read_json(spark, files, EONET_SCHEMA).select(
        F.input_file_name().alias("raw_object_path"),
        F.explode("events").alias("e"),
    )

    points = F.filter(F.col("e.geometry"), lambda g: g["type"] == "Point")
    latest_point = F.sort_array(
        F.transform(points, lambda g: F.struct(g["date"].alias("d"), g["coordinates"].alias("c"))),
        asc=False,
    )[0]
    coords = F.from_json(latest_point["c"], ArrayType(DoubleType()))
    lon, lat = coords[0], coords[1]
    good_point = lon.between(-180, 180) & lat.between(-90, 90)

    latest_date = F.to_timestamp(F.array_max(F.transform(F.col("e.geometry"), lambda g: g["date"])))

    return raw.select(
        F.lit("eonet").alias("source"),
        F.col("e.id").alias("source_event_id"),
        F.col("e.categories")[0]["id"].alias("source_category"),
        F.col("e.title").alias("title"),
        latest_date.alias("event_time_utc"),
        F.when(good_point, lat).alias("latitude"),
        F.when(good_point, lon).alias("longitude"),
        F.lit(None).cast("double").alias("magnitude"),
        F.lit(None).cast("double").alias("depth_km"),
        F.when(F.col("e.closed").isNotNull(), "closed").otherwise("open").alias("event_status"),
        latest_date.alias("source_updated_utc"),
        F.size("e.geometry").alias("geometry_count"),
        "raw_object_path",
    )


# =====================================================================
# Cleaning
# =====================================================================

def build_staging(spark, input_dir, run_id):
    """
    Read a run folder and return (clean_dataframe, stats).

    stats is a small dict: rows read, rejected (with reasons), duplicates, staged.
    """
    usgs_files, eonet_files = find_raw_files(input_dir)

    parts = []
    if usgs_files:
        parts.append(read_usgs(spark, usgs_files))
    if eonet_files:
        parts.append(read_eonet(spark, eonet_files))
    if not parts:
        raise ValueError(f"No response.json files found under {input_dir}")

    rows = parts[0]
    for extra in parts[1:]:
        rows = rows.unionByName(extra)

    # Map the original category to our hazard type.
    mapping = spark.createDataFrame(HAZARD_MAP, ["source", "source_category", "mapped_hazard"])
    rows = rows.join(F.broadcast(mapping), ["source", "source_category"], "left")

    # Mark rows we must not load. The first matching reason wins.
    reject_reason = (
        F.when(F.col("source_event_id").isNull(), "missing_id")
         .when(F.col("event_time_utc").isNull(), "missing_time")
         .when((F.col("source") == "usgs") & (F.col("source_category") != "earthquake"), "not_an_earthquake")
         .when((F.col("source") == "eonet") & (F.col("source_category") == "manmade"), "manmade_event")
    )
    rows = rows.withColumn("reject_reason", reject_reason).cache()

    rejected = {r["reject_reason"]: r["count"]
                for r in rows.filter(F.col("reject_reason").isNotNull())
                             .groupBy("reject_reason").count().collect()}

    good = rows.filter(F.col("reject_reason").isNull())

    # Build the final columns.
    good = (
        good
        .withColumn("event_key", F.concat_ws(":", "source", "source_event_id"))
        .withColumn("hazard_type", F.coalesce("mapped_hazard", F.lit("other")))
        .withColumn("has_valid_point", F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .withColumn("run_id", F.lit(run_id))
        .withColumn("ingested_at_utc", F.current_timestamp())
    )

    # Same event twice: keep the version with the newest source_updated_utc.
    newest_first = Window.partitionBy("event_key").orderBy(
        F.col("source_updated_utc").desc_nulls_last(),
        F.col("event_time_utc").desc(),
    )
    staged = (
        good.withColumn("_rank", F.row_number().over(newest_first))
            .filter(F.col("_rank") == 1)
            .select(*COLUMNS)
    ).cache()

    rows_read = rows.count()
    rows_good = good.count()
    rows_staged = staged.count()
    stats = {
        "run_id": run_id,
        "rows_read": rows_read,
        "rows_rejected": sum(rejected.values()),
        "rejected_by_reason": rejected,
        "duplicates_removed": rows_good - rows_staged,
        "rows_staged": rows_staged,
        "rows_without_point": staged.filter(~F.col("has_valid_point")).count(),
        "unmapped_category": good.filter(F.col("mapped_hazard").isNull()).count(),
    }
    return staged, stats


# =====================================================================
# Writing the result
# =====================================================================

def write_jdbc(df):
    """Overwrite staging.stg_event in PostgreSQL (truncate keeps the table definition)."""
    host = os.getenv("POSTGRES_HOST", "postgres")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "disasterlens")
    (df.write.format("jdbc")
        .option("url", f"jdbc:postgresql://{host}:{port}/{database}")
        .option("dbtable", "staging.stg_event")
        .option("user", os.environ["POSTGRES_USER"])
        .option("password", os.environ["POSTGRES_PASSWORD"])
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode("overwrite")
        .save())


def make_spark():
    return (SparkSession.builder
            .appName("disasterlens_transform")
            .config("spark.sql.session.timeZone", "UTC")   # all times are UTC
            .config("spark.sql.shuffle.partitions", "8")   # our data is small
            .getOrCreate())


def main():
    parser = argparse.ArgumentParser(description="DisasterLens Spark transformation")
    parser.add_argument("--input-dir", required=True, help="folder with the run's raw files")
    parser.add_argument("--run-id", required=True, help="safe run id of this pipeline run")
    parser.add_argument("--output", default="jdbc", help="'jdbc' or 'parquet:<folder>'")
    args = parser.parse_args()

    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    staged, stats = build_staging(spark, args.input_dir, args.run_id)

    if args.output == "jdbc":
        write_jdbc(staged)
    elif args.output.startswith("parquet:"):
        staged.write.mode("overwrite").parquet(args.output.split(":", 1)[1])
    else:
        raise ValueError("--output must be 'jdbc' or 'parquet:<folder>'")

    # One summary line; if anything above failed, the job exits with an error instead.
    print("SPARK_SUMMARY " + json.dumps(stats))
    spark.stop()


if __name__ == "__main__":
    main()