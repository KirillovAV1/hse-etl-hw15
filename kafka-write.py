#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct


BUCKET = "s3-etl"

INPUT_PATH = f"s3a://{BUCKET}/task3/input/20mb.json"

KAFKA_BOOTSTRAP_SERVERS = "rc1b-57tqk155nnv81p73.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "topic-1"


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

    df = spark.read.option("multiLine", "true").json(INPUT_PATH)

    kafka_df = df.select(to_json(struct("*")).alias("value"))

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC) \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1 "
                ";") \
        .save()

    spark.stop()


if __name__ == "__main__":
    main()