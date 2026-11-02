import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType


SET_SCORE_SCHEMA = StructType([
    StructField("score_set", StringType()),
    StructField("score_first", StringType()),
    StructField("score_second", StringType())
])


MATCH_SCHEMA = StructType([
    StructField("event_key", StringType()),
    StructField("event_first_player", StringType()),
    StructField("event_second_player", StringType()),
    StructField("tournament_key", StringType()),
    StructField("tournament_name", StringType()),
    StructField("event_status", StringType()),
    StructField("event_final_result", StringType()),
    StructField("event_game_result", StringType()),
    StructField("event_serve", StringType()),
    StructField("event_qualification", StringType()),
    StructField("scores", ArrayType(SET_SCORE_SCHEMA))
])

EVENT_SCHEMA = StructType([
    StructField("match", MATCH_SCHEMA),
    StructField("_corrupt_record", StringType())
])


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

        parsed = messages.withColumn(
            "data", F.from_json("payload", EVENT_SCHEMA, {"mode": "PERMISSIVE"})
        ).withColumn(
            "current_set_number",
            F.regexp_extract("data.match.event_status", r"(?i)^Set\s+(\d+)", 1)
        )
        # Durante il gioco scegliamo la voce scores del set indicato da event_status.
        # Per stati come Finished mostriamo l'ultima voce ricevuta, con il suo numero.
        set_score = F.when(
            F.col("current_set_number") != "",
            F.try_element_at(
                F.filter("data.match.scores",
                         lambda score: score["score_set"] == F.col("current_set_number")),
                F.lit(-1)
            )
        ).otherwise(
            F.try_element_at("data.match.scores", F.lit(-1))
        )
        parsed = parsed.withColumn("set_score", set_score)
        server = (
            F.when(F.col("data.match.event_serve") == "First Player",
                   F.col("data.match.event_first_player"))
            .when(F.col("data.match.event_serve") == "Second Player",
                  F.col("data.match.event_second_player"))
            .otherwise(F.col("data.match.event_serve"))
        )
        readable_matches = parsed.select(
            "match_id",
            F.col("data.match.event_first_player").alias("player_1"),
            F.col("data.match.event_second_player").alias("player_2"),
            F.col("data.match.tournament_key").alias("tournament_id"),
            F.col("data.match.tournament_name").alias("tournament"),
            F.col("data.match.event_status").alias("status"),
            F.col("data.match.event_final_result").alias("sets"),
            F.col("set_score.score_set").alias("games_set"),
            F.col("set_score.score_first").alias("games_1"),
            F.col("set_score.score_second").alias("games_2"),
            F.col("data.match.event_game_result").alias("point_score"),
            server.alias("server"),
            F.col("data.match.event_qualification").alias("qualification"),
            (F.col("data._corrupt_record").isNull()
             & F.col("data.match.event_key").isNotNull()).alias("parse_ok"),
            "partition", "offset", "timestamp"
        )

        query = (
            readable_matches.writeStream.format("console")
            .queryName("tennis-kafka-reader")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .option("numRows", "10")
            .option("truncate", "false")
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()
