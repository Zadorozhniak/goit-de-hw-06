import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

def main(bootstrap_servers, username, password, identifier, conditions_path):
    input_topic = f"{identifier}_building_sensors"
    output_topic = f"{identifier}_alerts"

    # Kafka options
    # kafka_opts = {
    #     "kafka.bootstrap.servers": bootstrap_servers,
    #     "kafka.security.protocol": "SASL_PLAINTEXT",
    #     "kafka.sasl.mechanism": "PLAIN",
    #     "kafka.sasl.username": username,
    #     "kafka.sasl.password": password,
    #     "subscribe": input_topic,
    #     "startingOffsets": "earliest",
    #     "failOnDataLoss": "false"
    # }
    kafka_opts = {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.security.protocol": "SASL_PLAINTEXT",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";',
        "subscribe": input_topic,
        "startingOffsets": "earliest",
        "failOnDataLoss": "false"
    }

    # Create Spark session
    # spark = SparkSession.builder.appName(f"{identifier}_AlertAggregator").config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3").config("spark.sql.streaming.checkpointLocation", f"checkpoints/{identifier}").getOrCreate()
    spark = SparkSession.builder\
    .appName(f"{identifier}_AlertAggregator")\
    .config("spark.sql.streaming.checkpointLocation", f"checkpoints/{identifier}")\
    .getOrCreate()

    # 1. Read stream
    raw = spark.readStream.format("kafka").options(**kafka_opts).load()

    # 2. Define schema
    sensor_schema = StructType([
        StructField("sensor_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("temperature", DoubleType()),
        StructField("humidity", DoubleType())
    ])

    # 3. Parse JSON and convert timestamp
    parsed = raw.select(
        from_json(col("value").cast("string"), sensor_schema).alias("data")
    ).select(
        col("data.sensor_id"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.timestamp").cast(TimestampType()).alias("event_time")
    ).withWatermark("event_time", "10 seconds")   # watermark 10 sec

    # 4. Sliding window aggregation (1 minute length, 30 seconds slide)
    aggregated = parsed.groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("sensor_id")
    ).agg(
        avg("temperature").alias("avg_temperature"),
        avg("humidity").alias("avg_humidity")
    ).select(
        col("sensor_id"),
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("avg_temperature"),
        col("avg_humidity")
    )

    # 5. Read alert conditions from CSV
    conditions_df = spark.read.option("header", True).option("inferSchema", True)\
        .csv(conditions_path)\
        .withColumnRenamed("min_humidity", "min_humidity")\
        .withColumnRenamed("max_humidity", "max_humidity")\
        .withColumnRenamed("min_temperature", "min_temperature")\
        .withColumnRenamed("max_temperature", "max_temperature")\
        .withColumnRenamed("code", "alert_code")

    # 6. Cross join with broadcast (small table) and filter
    alerts = aggregated.crossJoin(conditions_df).filter(
        ((col("min_temperature") == -999) | (col("avg_temperature") >= col("min_temperature"))) &
        ((col("max_temperature") == -999) | (col("avg_temperature") <= col("max_temperature"))) &
        ((col("min_humidity") == -999) | (col("avg_humidity") >= col("min_humidity"))) &
        ((col("max_humidity") == -999) | (col("avg_humidity") <= col("max_humidity")))
    ).select(
        "sensor_id", "window_start", "window_end",
        "avg_temperature", "avg_humidity",
        col("alert_code").alias("alert_code"),
        col("message").alias("message")
    )

    # 7. Convert to JSON for Kafka output
    output = alerts.select(
        to_json(struct(
            "sensor_id", "window_start", "window_end",
            "avg_temperature", "avg_humidity", "alert_code", "message"
        )).alias("value")
    )

    # 8. Write to Kafka
    # query = output.writeStream \
    #     .format("kafka") \
    #     .option("kafka.bootstrap.servers", bootstrap_servers) \
    #     .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    #     .option("kafka.sasl.mechanism", "PLAIN") \
    #     .option("kafka.sasl.username", username) \
    #     .option("kafka.sasl.password", password) \
    #     .option("topic", output_topic) \
    #     .option("checkpointLocation", f"checkpoints/{identifier}/kafka_sink") \
    #     .outputMode("append") \
    #     .start()

    query = output.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.username", username) \
    .option("kafka.sasl.password", password) \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{username}" password="{password}";') \
    .option("topic", output_topic) \
    .option("checkpointLocation", f"checkpoints/{identifier}/kafka_sink") \
    .outputMode("append") \
    .start()

    print(f"Streaming started. Output topic: {output_topic}")
    query.awaitTermination()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="77.81.230.104:9092")
    parser.add_argument("--username", default="admin")
    parser.add_argument("--password", default="VawEzo1ikLtrA8Ug8THa")
    parser.add_argument("--identifier", default="Khrystyna_Z")
    parser.add_argument("--conditions-path", default="alerts_conditions.csv")
    args = parser.parse_args()
    main(args.bootstrap_servers, args.username, args.password, args.identifier, args.conditions_path)