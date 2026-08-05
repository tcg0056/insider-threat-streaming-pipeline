"""Consume Kafka telemetry, calculate explainable risk, and write micro-batches to BigQuery."""
import os

from pyspark.sql import SparkSession, functions as F, types as T

PROJECT = os.environ["GCP_PROJECT_ID"]
DATASET = os.getenv("BIGQUERY_DATASET", "insider_risk")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = os.getenv("KAFKA_TOPIC", "security-events")

schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.StringType()),
    T.StructField("user_id", T.StringType()),
    T.StructField("department", T.StringType()),
    T.StructField("event_type", T.StringType()),
    T.StructField("source_ip", T.StringType()),
    T.StructField("country", T.StringType()),
    T.StructField("device_id", T.StringType()),
    T.StructField("resource", T.StringType()),
    T.StructField("data_classification", T.StringType()),
    T.StructField("bytes_transferred", T.LongType()),
    T.StructField("privileged_account", T.BooleanType()),
    T.StructField("is_known_device", T.BooleanType()),
    T.StructField("label", T.StringType()),
])

spark = SparkSession.builder.appName("insider-risk-stream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC)
       .option("startingOffsets", "latest").load())

events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_time", F.to_timestamp("event_time"))
          .withColumn("off_hours", (F.hour("event_time") < 6) | (F.hour("event_time") >= 22))
          .withColumn(
              "risk_score",
              F.when(F.col("bytes_transferred") >= 500_000_000, 35).otherwise(0)
              + F.when(F.col("country").isin("KP", "RU", "CN"), 25).otherwise(0)
              + F.when(~F.col("is_known_device"), 15).otherwise(0)
              + F.when(F.col("privileged_account"), 10).otherwise(0)
              + F.when(F.col("data_classification") == "restricted", 10).otherwise(0)
              + F.when(F.col("off_hours"), 5).otherwise(0))
          .withColumn("severity", F.when(F.col("risk_score") >= 70, "HIGH")
                      .when(F.col("risk_score") >= 40, "MEDIUM").otherwise("LOW"))
          .withColumn("processed_at", F.current_timestamp()))


def write_bigquery(batch_df, batch_id: int) -> None:
    if not batch_df.isEmpty():
        (batch_df.dropDuplicates(["event_id"]).write.format("bigquery")
         .option("table", f"{PROJECT}.{DATASET}.normalized_events")
         .option("writeMethod", "direct").mode("append").save())


query = (events.writeStream.foreachBatch(write_bigquery)
         .option("checkpointLocation", "checkpoints/normalized-events")
         .trigger(processingTime="10 seconds").start())
query.awaitTermination()

