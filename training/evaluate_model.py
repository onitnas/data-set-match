import argparse
import json
import math
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.functions import array_to_vector, vector_to_array
from pyspark.sql import SparkSession, functions as F

from training.prepare_dataset import PLAYER_ORIENTATION


def evaluate_predictions(predictions):
    evaluator = MulticlassClassificationEvaluator(labelCol="label", weightCol="sample_weight")
    probability = vector_to_array(F.col("probability"))[1]

    brier_score = predictions.select(
        (F.sum(F.col("sample_weight") * F.pow(probability - F.col("label"), 2))
         / F.sum("sample_weight")).alias("brier_score")
    ).first()["brier_score"]

    metrics = {
        "accuracy": evaluator.setMetricName("accuracy").evaluate(predictions),
        "log_loss": evaluator.setMetricName("logLoss").evaluate(predictions),
        "brier_score": brier_score,
        "roc_auc": BinaryClassificationEvaluator(
            labelCol="label", rawPredictionCol="probability", weightCol="sample_weight",
            metricName="areaUnderROC"
        ).evaluate(predictions)
    }

    if not all(math.isfinite(value) for value in metrics.values()):
        raise ValueError("Metriche non valide: controllare dati e probabilità")
    return metrics


def evaluate_match_start(predictions):
    start = predictions.filter(F.col("point_index") == 0).withColumn("sample_weight", F.lit(1.0))
    return evaluate_predictions(start)


def evaluate_ranking_baseline(dataset):
    """Confronta il vincitore reale con il giocatore dal ranking migliore."""
    dataset = dataset.filter(F.col("point_index") == 0)
    known = (
        (F.col("player_a_rank_missing") == 0) & (F.col("player_b_rank_missing") == 0)
        & (F.col("player_a_rank") != F.col("player_b_rank"))
    )
    correct = ((F.col("player_a_rank") < F.col("player_b_rank")).cast("double") == F.col("label"))
    result = dataset.agg(
        F.count("*").alias("matches"), F.sum(known.cast("int")).alias("covered_matches"),
        F.avg(F.when(known, correct.cast("double"))).alias("accuracy")
    ).first().asDict()
    return result


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Valutazione del modello fissato sul 2026")
    parser.add_argument("--data-dir", type=Path, default=Path("training/data/processed"))
    parser.add_argument("--model-dir", type=Path, default=Path("training/artifacts/model"))

    arguments = parser.parse_args()

    with (arguments.data_dir / "schema.json").open(encoding="utf-8") as file:
        schema = json.load(file)

    if schema.get("target") != "match_winner" or schema.get("dataset_stage") != "live":
        raise ValueError("Il test deve contenere gli stati live per il target match_winner")

    if schema.get("player_orientation") != PLAYER_ORIENTATION:
        raise ValueError("Rigenerare i dataset con player_a = 0 e player_b = 1")

    with (arguments.model_dir / "schema.json").open(encoding="utf-8") as file:
        if json.load(file) != schema:
            raise ValueError("Lo schema del test non coincide con quello del modello")

    spark = SparkSession.builder \
        .appName("data-set-match-Evaluation") \
        .master("local[2]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        test = spark.read.parquet(str(arguments.data_dir / "test.parquet")) \
            .withColumn("label", F.col("label").cast("double"))

        invalid = (
            F.col("match_date").isNull()
            | ~F.col("match_date").startswith("2026-")
            | F.col("label").isNull()
            | ~F.col("label").isin(0.0, 1.0)
            | F.col("sample_weight").isNull()
            | (F.col("sample_weight") <= 0)
        )

        if test.filter(invalid).limit(1).count():
            raise ValueError("Date, label o pesi non validi nel test 2026")

        if test.select("label").distinct().count() != 2:
            raise ValueError("Il test deve contenere entrambe le classi")

        model = PipelineModel.load(
            str((arguments.model_dir / "pipeline").resolve())
        )

        predictions = model \
            .transform(test) \
            .select(
            "label",
            "prediction",
            "probability",
            "sample_weight",
            "point_index"
            ) \
            .cache()
        try:
            print("Metriche sul 2026:")
            print(json.dumps(evaluate_predictions(predictions), indent=2))
            print("Metriche a inizio incontro:")
            print(json.dumps(evaluate_match_start(predictions), indent=2))
        finally:
            predictions.unpersist()

        # Baseline costante stimata sul 2024, per confronto con il modello fissato sul 2026.
        training = spark.read.parquet(str(arguments.data_dir / "train.parquet"))
        invalid_training = (
            F.col("match_date").isNull()
            | ~F.col("match_date").startswith("2024-")
            | F.col("label").isNull()
            | ~F.col("label").isin(0.0, 1.0)
            | F.col("sample_weight").isNull()
            | (F.col("sample_weight") <= 0)
        )

        if training.filter(invalid_training).limit(1).count():
            raise ValueError("Date, label o pesi non validi nel training 2024")

        probability = training.agg(
            F.sum(F.col("label") * F.col("sample_weight")) / F.sum("sample_weight")
        ).first()[0]

        if probability is None:
            raise ValueError("Training 2024 vuoto: impossibile calcolare la baseline")

        baseline = test.select("label", "sample_weight") \
        .withColumn("probability",
            array_to_vector(
                F.array(
                    F.lit(1 - probability),
                    F.lit(probability)
                )
            )
        ).withColumn(
            "prediction",
            F.lit(float(probability > 0.5))
        )

        print("Baseline costante stimata sul 2024:")
        print(json.dumps(evaluate_predictions(baseline), indent=2))
        print("Baseline ranking a inizio incontro:")
        print(json.dumps(evaluate_ranking_baseline(test), indent=2))
    finally:
        spark.stop()
