"""
Spark Structured Streaming job: Kafka -> Spark -> MongoDB

Two streaming queries run side by side:
  1. RAW : every parsed reading is written to  <db>.readings
  2. AGG : 1-minute tumbling-window aggregates (avg/min/max/count per
           location + sensor + metric) are upserted into  <db>.weather_agg

Run (Spark 3.5.x, Scala 2.12):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_job.py

or, with pyspark installed via pip:
    python spark_job.py          # pulls the Kafka package automatically
"""

import argparse
import os
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import ASCENDING, MongoClient, ReplaceOne
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

load_dotenv(override=True)  # .env wins over system variables

for var in ("JAVA_HOME", "HADOOP_HOME"):
    if os.getenv(var):
        os.environ["PATH"] = os.path.join(os.environ[var], "bin") + os.pathsep + os.environ["PATH"]
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

# ------------------------------------------------------------------ constants
KAFKA_PACKAGE = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
TRIGGER_INTERVAL = "10 seconds"
WATERMARK = "2 minutes"
ISO_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'"
AGG_KEY_FIELDS = ("window_start", "sensor_id", "metric_type")

SCHEMA = StructType([
    StructField("timestamp", StringType()),
    StructField("value", DoubleType()),
    StructField("metric_type", StringType()),
    StructField("sensor_id", StringType()),
    StructField("location", StringType()),
    StructField("unit", StringType()),
    StructField("condition", StringType()),
    StructField("last_updated", StringType()),
])


# ------------------------------------------------------------------- helpers
def log(tag, message):
    print(f"[{tag}] {message}")


def parse_args():
    p = argparse.ArgumentParser(description="Kafka -> Spark -> MongoDB streaming job")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC", "streaming-data"))
    p.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    p.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "weather_stream"))
    p.add_argument("--starting-offsets", default="earliest", choices=["earliest", "latest"])
    p.add_argument("--window", default="1 minute", help="Aggregation window, e.g. '1 minute'")
    p.add_argument("--checkpoint-dir", default="./checkpoints")
    return p.parse_args()


def iso_to_utc(text):
    """Parse an ISO-8601 string (with or without 'Z') into a UTC datetime."""
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc)


# --------------------------------------------------------------------- mongo
class MongoStore:
    """Creates one MongoClient on first use (foreachBatch runs on the driver)."""

    def __init__(self, uri, db_name):
        self._uri = uri
        self._db_name = db_name
        self._db = None

    @property
    def db(self):
        if self._db is None:
            db = MongoClient(self._uri, serverSelectionTimeoutMS=5000)[self._db_name]
            db.readings.create_index([("timestamp", ASCENDING)])
            db.readings.create_index([("location", ASCENDING), ("metric_type", ASCENDING)])
            db.weather_agg.create_index(
                [(field, ASCENDING) for field in AGG_KEY_FIELDS], unique=True
            )
            self._db = db
        return self._db


def make_batch_writer(tag, prepare_row, save):
    """
    Build a foreachBatch function.
      prepare_row(row_dict) -> item to store
      save(items)           -> summary text for the log line
    """
    def write(batch_df, batch_id):
        rows = [r.asDict() for r in batch_df.collect()]
        if not rows:
            return
        items = [prepare_row(row) for row in rows]
        try:
            log(tag, f"batch {batch_id}: {save(items)}")
        except Exception as e:
            log(tag, f"batch {batch_id} failed: {e}")
    return write


def raw_writer(store):
    def prepare(row):
        row["timestamp"] = iso_to_utc(row["timestamp"])
        return row

    def save(docs):
        store.db.readings.insert_many(docs, ordered=False)
        return f"wrote {len(docs)} readings"

    return make_batch_writer("raw", prepare, save)


def agg_writer(store):
    def prepare(row):
        row["window_start"] = iso_to_utc(row["window_start"])
        row["window_end"] = iso_to_utc(row["window_end"])
        key = {field: row[field] for field in AGG_KEY_FIELDS}
        return ReplaceOne(key, row, upsert=True)

    def save(ops):
        store.db.weather_agg.bulk_write(ops, ordered=False)
        return f"upserted {len(ops)} windows"

    return make_batch_writer("agg", prepare, save)


# --------------------------------------------------------------------- spark
def build_spark():
    spark = (
        SparkSession.builder.appName("WeatherStreamToMongo")
        .config("spark.jars.packages", KAFKA_PACKAGE)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_kafka(spark, args):
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )


def parse_events(raw):
    """Kafka bytes -> typed columns, dropping rows without a timestamp or value."""
    return (
        raw.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("d"))
        .select("d.*")
        .where(F.col("timestamp").isNotNull() & F.col("value").isNotNull())
        .withColumn("ts", F.to_timestamp("timestamp"))
    )


def build_aggregates(events, window):
    return (
        events.withWatermark("ts", WATERMARK)
        .groupBy(F.window("ts", window), "location", "sensor_id", "metric_type", "unit")
        .agg(
            F.avg("value").alias("avg_value"),
            F.min("value").alias("min_value"),
            F.max("value").alias("max_value"),
            F.count("*").alias("n"),
        )
        .select(
            F.date_format("window.start", ISO_FORMAT).alias("window_start"),
            F.date_format("window.end", ISO_FORMAT).alias("window_end"),
            "location", "sensor_id", "metric_type", "unit",
            "avg_value", "min_value", "max_value", "n",
        )
    )


def start_query(df, writer, checkpoint, output_mode):
    return (
        df.writeStream.outputMode(output_mode)
        .foreachBatch(writer)
        .option("checkpointLocation", checkpoint)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .start()
    )


# ---------------------------------------------------------------------- main
def main():
    args = parse_args()
    store = MongoStore(args.mongo_uri, args.mongo_db)
    spark = build_spark()

    events = parse_events(read_kafka(spark, args))
    aggregates = build_aggregates(events, args.window)

    queries = [
        start_query(
            events.drop("ts"), raw_writer(store),
            os.path.join(args.checkpoint_dir, "raw"), "append",
        ),
        start_query(
            aggregates, agg_writer(store),
            os.path.join(args.checkpoint_dir, "agg"), "update",
        ),
    ]

    print("Spark streaming started. Ctrl+C to stop.")
    try:
        spark.streams.awaitAnyTermination()
    except KeyboardInterrupt:
        print("Stopping...")
        for query in queries:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
