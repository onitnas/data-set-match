import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType
from pyspark.ml import PipelineModel
import json
from pyspark.ml.functions import vector_to_array

from streaming.snapshot_features import build_snapshot_features

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


def write_predictions(predictions, batch_id, model_id, source_topic, elasticsearch_index):
    documents = (
        predictions.withColumn("document_id", F.concat_ws(":", F.lit(source_topic), "partition", "offset"))
        .withColumn("kafka_topic", F.lit(source_topic))
        .withColumn("batch_id", F.lit(batch_id).cast("long"))
        .withColumn("model_id", F.lit(model_id))
    )
    (
        documents.write.format("es")
        .option("es.mapping.id", "document_id")
        .option("es.spark.dataframe.write.null", "true")
        .mode("append")
        .save(elasticsearch_index)
    )
    print(f"Batch {batch_id}: previsioni salvate in {elasticsearch_index}", flush=True)


if __name__ == "__main__":
    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.environ.get("KAFKA_TOPIC", "tennis-points")
    elasticsearch_index = os.environ.get("ELASTICSEARCH_INDEX", "tennis-predictions")
    checkpoint = os.environ.get("SPARK_CHECKPOINT_LOCATION", "/app/checkpoints/kafka-reader")

    spark = (
        SparkSession.builder.appName("TennisLiveMatchPrediction")
        .config("spark.sql.shuffle.partitions", "4")
        .config("es.nodes", os.environ.get("ELASTICSEARCH_NODES", "elasticsearch"))
        .config("es.port", os.environ.get("ELASTICSEARCH_PORT", "9200"))
        .config("es.nodes.wan.only", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        tournaments = (
            spark.read
            .option("multiLine", True)
            .json("/app/training/config/tournaments.json")
            .select(
                F.col("tournament_key").cast("string").alias("config_tournament_key"),
                F.col("tournament_surface").alias("surface")
            )
        )
        
        players = (
            spark.read
            .option("multiLine", True)
            .json("/app/training/data/players/*.json")
        )

        rankings = (
            players
            .withColumn("stat", F.explode("stats"))
            .filter(F.col("stat.type") == "singles")
            .select(
                F.col("player_key").cast("string").alias("player_id"),
                F.expr("try_cast(stat.season AS INT)").alias("season"),
                F.expr("try_cast(stat.rank AS INT)").alias("rank")
            )
            .filter(F.col("rank") > 0)
        )
        
        model_dir = os.environ.get(
            "SPARK_MODEL_DIR",
            "/app/training/artifacts/model"
        )

        with open(f"{model_dir}/schema.json", encoding="utf-8") as file:
            schema = json.load(file)

        if len(schema["feature_columns"]) != 17:
            raise ValueError("Il modello deve avere 17 feature")

        model = PipelineModel.load(f"{model_dir}/pipeline")

        print("Modello caricato correttamente")
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

        def process_batch(batch, batch_id):

            valid = batch.filter(
                F.col("parse_ok") &
                F.col("data.match.event_status").rlike(r"^Set [1-5]$") &
                (
                    F.lower(
                        F.coalesce(
                            F.col("data.match.event_qualification"),
                            F.lit("false")
                        )
                    ) != "true"
                )
            )

            features = build_snapshot_features(
                valid,
                tournaments,
                rankings
            )

            features = features.dropna(
                subset=schema["feature_columns"]
            )

            predictions = model.transform(features)

            output = predictions.select(
                "timestamp",
                "match_id",
                "player_a_is_serving",
                F.col("data.match.event_first_player").alias("player_a"),
                F.col("data.match.event_second_player").alias("player_b"),
                "sets_a",
                "sets_b",
                "games_a",
                "games_b",
                "is_tiebreak",
                "tiebreak_target",
                "tiebreak_score_a",
                "tiebreak_score_b",
                "partition",
                "offset",
                vector_to_array("probability")[1].alias("player_a_win_probability"),
                (1 - vector_to_array("probability")[1]).alias("player_b_win_probability"),
                "api_score"
            )

            print(f"Batch {batch_id}", flush=True)
            
            write_predictions(
                predictions=output,
                batch_id=batch_id,
                model_id=model.uid,
                source_topic=topic,
                elasticsearch_index=elasticsearch_index,
            )
            
            output.show(20, truncate=False)

        query = (
            parsed.writeStream.foreachBatch(process_batch)
            .queryName("data-set-match-predictions")
            .outputMode("append")
            .option("checkpointLocation", checkpoint)
            .trigger(processingTime="5 seconds")
            .start()
        )
        query.awaitTermination()
    finally:
        spark.stop()
