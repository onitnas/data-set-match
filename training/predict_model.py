import argparse
import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql import SparkSession, functions as F

from training.tennis_rules import PLAYER_ORIENTATION


class MatchPredictor:
    def __init__(self, model_dir):
        model_dir = Path(model_dir)
        with (model_dir / "schema.json").open(encoding="utf-8") as file:
            self.schema = json.load(file)
        if self.schema.get("target") != "match_winner":
            raise ValueError("La pipeline deve prevedere il vincitore del match")
        if self.schema.get("player_orientation") != PLAYER_ORIENTATION:
            raise ValueError("Rigenerare dataset e modello con player_a = 0 e player_b = 1")
        self.model = PipelineModel.load(str((model_dir / "pipeline").resolve()))

    def transform(self, matches):
        # Richiede feature già calcolate; non richiede label o sample_weight.
        missing = set(self.schema["feature_columns"]) - set(matches.columns)
        if missing:
            raise ValueError(f"Feature mancanti: {', '.join(sorted(missing))}")
        predictions = self.model.transform(matches).withColumn(
            "player_a_win_probability", vector_to_array(F.col("probability"))[1]
        ).withColumn("player_b_win_probability", 1 - F.col("player_a_win_probability"))
        if {"player_a_id", "player_b_id"} <= set(matches.columns):
            predictions = predictions.withColumn(
                "predicted_winner_id",
                F.when(F.col("prediction") == 1, F.col("player_a_id")).otherwise(F.col("player_b_id"))
            )
        return predictions


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Probabilità live di vittoria del match")
    parser.add_argument("--model-dir", type=Path, default=Path("training/artifacts/model"))
    parser.add_argument("--input", type=Path, required=True, help="JSON con le feature live già calcolate")
    arguments = parser.parse_args()
    if not arguments.input.is_file():
        parser.error(f"File di input non trovato: {arguments.input}")
    spark = SparkSession.builder.appName("TennisMatchPrediction").master("local[2]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        predictor = MatchPredictor(arguments.model_dir)
        matches = spark.read.option("multiLine", True).json(str(arguments.input))
        predictions = predictor.transform(matches)
        metadata = [column for column in predictor.schema["metadata_columns"] if column in matches.columns]
        columns = metadata + ["prediction", "player_a_win_probability", "player_b_win_probability"]
        if "predicted_winner_id" in predictions.columns:
            columns.append("predicted_winner_id")
        for row in predictions.select(*columns).toJSON().toLocalIterator():
            print(row)
    finally:
        spark.stop()
