# Работа с топиками Apache Kafka® с помощью PySpark заданий в Yandex Data Processing.

## 1. Подготовить архитектуру

Этапы 1-9 из [инструкции](https://yandex.cloud/ru/docs/managed-kafka/tutorials/data-processing) были сделаны ранее.

Создание кластера Apache Kafka:

<img width="683" height="892" alt="image" src="https://github.com/user-attachments/assets/87a12041-9893-41f6-86a8-0ed22afb3b34" />

Создание топика: 

<img width="566" height="311" alt="image" src="https://github.com/user-attachments/assets/73057a17-3621-4cdb-94fe-763dbb752312" />

Создание пользователя: 

<img width="692" height="370" alt="image" src="https://github.com/user-attachments/assets/6e6000aa-d6f3-4074-9fb0-e936360597b9" />

---

## 2. Создать задания PySpark

В качестве данных взял (данный example JSON)[https://examplefile.com/code/json/20-mb-json]

<img width="854" height="240" alt="image" src="https://github.com/user-attachments/assets/b5c45824-c474-494c-9531-5cc8ea0d9fc7" />

Создам `kafka-write.py` скрипт, который разобьет исходный JSON на отдельные записи и отправит сообщения в топик Кафки:

```python
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
```

И скрипт `kafka-read-stream.py` для чтения из топика и для потоковой обработки:

```python
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
```

Загрузил скрипты в бакет в папку kafka:

<img width="922" height="279" alt="image" src="https://github.com/user-attachments/assets/d7ccba0c-0996-4543-8cb8-74f45007c362" />

Результат загрузки данных в топик: 

<img width="867" height="489" alt="image" src="https://github.com/user-attachments/assets/e5913fc9-2643-4cd6-b924-f023b0aeba76" />

Результат считывания данных из топика:

<img width="685" height="484" alt="image" src="https://github.com/user-attachments/assets/b3f826b8-293e-4298-84e7-db721a18b0a2" />

---

### 3. Разложить JSON в плоский вид

Конвертнул результат в csv для просмотра и вот, что получилось:

<img width="993" height="916" alt="image" src="https://github.com/user-attachments/assets/bbdb77e1-8c50-4664-8fb5-85365d6e9e5c" />
