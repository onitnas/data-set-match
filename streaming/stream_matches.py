import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

from streaming.predict_matches import LivePredictor


POINT_SCHEMA = StructType([
    StructField("number_point", StringType()),
    StructField("score", StringType())
])

GAME_SCHEMA = StructType([
    StructField("set_number", StringType()),
    StructField("number_game", StringType()),
    StructField("player_served", StringType()),
    StructField("serve_winner", StringType()),
    StructField("score", StringType()),
    StructField("points", ArrayType(POINT_SCHEMA))
])


SET_SCORE_SCHEMA = StructType([
    StructField("score_set", StringType()),
    StructField("score_first", StringType()),
    StructField("score_second", StringType())
])


MATCH_SCHEMA = StructType([
    StructField("event_key", StringType()),
    StructField("event_date", StringType()),
    StructField("first_player_key", StringType()),
    StructField("second_player_key", StringType()),
    StructField("event_first_player", StringType()),
    StructField("event_second_player", StringType()),
    StructField("tournament_key", StringType()),
    StructField("tournament_name", StringType()),
    StructField("event_status", StringType()),
    StructField("event_type_type", StringType()),
    StructField("event_winner", StringType()),
    StructField("event_final_result", StringType()),
    StructField("event_game_result", StringType()),
    StructField("event_serve", StringType()),
    StructField("event_qualification", StringType()),
    StructField("scores", ArrayType(SET_SCORE_SCHEMA)),
    StructField("pointbypoint", ArrayType(GAME_SCHEMA))
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
        SparkSession.builder.appName("TennisLiveMatchPrediction")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        predictor = LivePredictor(
            model_dir=os.environ.get("SPARK_MODEL_DIR", "/app/training/artifacts/model"),
            history_path=os.environ.get("TENNIS_HISTORY_PATH", "/app/training/data/processed/player_history.json"),
            players_dir=os.environ.get("TENNIS_PLAYERS_DIR", "/app/training/data/players"),
            tournaments_path=os.environ.get("TENNIS_TOURNAMENTS_PATH", "/app/training/config/tournaments.json"),
            history_dir=os.path.join(checkpoint, "player-history")
        )
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
        )
        parsed = parsed.withColumn("parse_ok",
            F.col("data._corrupt_record").isNull() & F.col("data.match.event_key").isNotNull()
        )

        query = (
            parsed.writeStream.foreachBatch(predictor.process_batch)
            .queryName("tennis-live-predictions")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()
