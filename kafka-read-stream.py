#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType


BUCKET = "s3-etl"

KAFKA_BOOTSTRAP_SERVERS = "rc1b-57tqk155nnv81p73.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "topic-1"

OUTPUT_PATH = f"s3a://{BUCKET}/task3/output"


def main():
    spark = SparkSession.builder \
        .appName("dataproc-kafka-read-stream-app") \
        .getOrCreate()

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("address", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("website", StringType(), True)
    ])

    query = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option(
            "kafka.sasl.jaas.config",
            "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) AS value") \
        .writeStream \
        .trigger(once=True) \
        .queryName("received_messages") \
        .format("memory") \
        .start()

    query.awaitTermination()

    df = spark.sql("SELECT value FROM received_messages")

    result_df = df \
        .select(from_json(col("value"), schema).alias("json")) \
        .select("json.*")

    result_df.write \
        .mode("overwrite") \
        .parquet(OUTPUT_PATH)

    spark.stop()


if __name__ == "__main__":
    main()