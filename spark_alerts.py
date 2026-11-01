import glob
import os
import shutil
import threading

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from configs import (
    ALERTS_CONDITIONS_FILE,
    ALERTS_TOPIC,
    BOOTSTRAP_SERVERS,
    BUILDING_SENSORS_TOPIC,
)

CHECKPOINT_DIR = "checkpoints"

WINDOW_DURATION = "1 minute"
SLIDING_INTERVAL = "30 seconds"
WATERMARK_DELAY = "10 seconds"

builder = (
    SparkSession.builder
    .appName("IoTStreamingAlerts")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.ui.showConsoleProgress", "false")
)

# Windows without winutils.exe: spark.jars.packages and the default checkpoint manager
# both need it. The Kafka connector jars from jars/ (fetched by download_jars.py) go
# straight to the classpath, and checkpoints are written through a local file system
# that skips permission changes (see windows_fs/NoPermissionLocalFileSystem.java)
if os.name == "nt":
    jars = os.pathsep.join(glob.glob(os.path.abspath("jars/*.jar")))
    builder = builder \
        .config("spark.driver.extraClassPath", jars) \
        .config("spark.executor.extraClassPath", jars) \
        .config("spark.hadoop.fs.file.impl", "local.windows.NoPermissionLocalFileSystem") \
        .config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager",
        )
else:
    builder = builder.config(
        "spark.jars.packages", f"org.apache.spark:spark-sql-kafka-0-10_2.13:{pyspark.__version__}"
    )

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Start from a clean state on every run so that old offsets do not interfere with the demo
shutil.rmtree(CHECKPOINT_DIR, ignore_errors=True)

# Kafka security options are passed only when the broker requires authentication
kafka_options = {"kafka.bootstrap.servers": BOOTSTRAP_SERVERS}
if os.getenv("KAFKA_SASL_MECHANISM"):
    kafka_options.update({
        "kafka.security.protocol": os.getenv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT"),
        "kafka.sasl.mechanism": os.getenv("KAFKA_SASL_MECHANISM"),
        "kafka.sasl.jaas.config": (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{os.getenv("KAFKA_USERNAME")}" password="{os.getenv("KAFKA_PASSWORD")}";'
        ),
    })

# 1. Sensor data stream from Kafka
sensor_schema = StructType([
    StructField("id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", StringType()),
])

sensors_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .option("subscribe", BUILDING_SENSORS_TOPIC)
    .option("startingOffsets", "latest")
    .load()
    .select(F.from_json(F.col("value").cast("string"), sensor_schema).alias("data"))
    .select("data.*")
    .withColumn("timestamp", F.to_timestamp("timestamp"))
)

# 2. Sliding window aggregation: 1 minute window, 30 seconds slide, 10 seconds watermark
averages_df = (
    sensors_df
    .withWatermark("timestamp", WATERMARK_DELAY)
    .groupBy(F.window("timestamp", WINDOW_DURATION, SLIDING_INTERVAL))
    .agg(
        F.round(F.avg("temperature"), 2).alias("t_avg"),
        F.round(F.avg("humidity"), 2).alias("h_avg"),
    )
)

# 3. Alert conditions from the CSV file; -999 means the bound is not used
conditions_df = spark.read.csv(ALERTS_CONDITIONS_FILE, header=True, inferSchema=True)


def within_bounds(value, low, high):
    return ((F.col(low) == -999) | (F.col(value) >= F.col(low))) & \
           ((F.col(high) == -999) | (F.col(value) <= F.col(high)))


# 4. Cross join every window average with every condition and keep the matches
alerts_df = (
    averages_df
    .crossJoin(conditions_df)
    .where(
        within_bounds("t_avg", "temperature_min", "temperature_max")
        & within_bounds("h_avg", "humidity_min", "humidity_max")
    )
    .select(
        "window",
        "t_avg",
        "h_avg",
        F.col("code").cast("string").alias("code"),
        "message",
        F.current_timestamp().alias("timestamp"),
    )
)

# 5. Write alerts to the output Kafka topic as JSON
kafka_query = (
    alerts_df
    .select(
        F.expr("uuid()").alias("key"),
        F.to_json(
            F.struct("window", "t_avg", "h_avg", "code", "message", "timestamp"),
            {"timestampFormat": "yyyy-MM-dd HH:mm:ss"},
        ).alias("value"),
    )
    .writeStream
    .format("kafka")
    .options(**kafka_options)
    .option("topic", ALERTS_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/kafka")
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

# Both console queries run in parallel; the lock keeps each printed block in one piece
print_lock = threading.Lock()


def print_batch(title):
    def show(batch_df, batch_id):
        with print_lock:
            if batch_df.isEmpty():
                print(f"===== {title} | batch {batch_id}: no rows =====")
                return
            print(f"\n===== {title} | batch {batch_id} =====")
            batch_df.orderBy("window").show(truncate=False)

    return show


# Window averages and alerts are also printed to the console to see what is going on
averages_query = (
    averages_df
    .writeStream
    .foreachBatch(print_batch("Window averages"))
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/averages")
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

console_query = (
    alerts_df
    .writeStream
    .foreachBatch(print_batch(f"Alerts sent to '{ALERTS_TOPIC}'"))
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/console")
    .outputMode("update")
    .trigger(processingTime="10 seconds")
    .start()
)

print(f"Reading '{BUILDING_SENSORS_TOPIC}', writing alerts to '{ALERTS_TOPIC}'")
spark.streams.awaitAnyTermination()
