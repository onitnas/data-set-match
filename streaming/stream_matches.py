import os

from pyspark.sql import SparkSession


if __name__ == "__main__":
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "tennis-points")
    checkpoint = os.environ.get("SPARK_CHECKPOINT_LOCATION", "/app/checkpoints/kafka-reader")

    spark = (
        SparkSession.builder.appName("TennisLiveKafkaReader")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"Lettura del topic {topic} da {bootstrap_servers}", flush=True)

        # earliest si applica alla prima esecuzione; poi si riparte dal checkpoint.
        events = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )

        messages = events.selectExpr(
            "CAST(key AS STRING) AS match_id",
            "CAST(value AS STRING) AS payload",
            "partition", "offset", "timestamp"
        )

        query = (
            messages.writeStream.format("console")
            .queryName("tennis-kafka-reader")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("numRows", "10")
            .option("truncate", "true")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()
