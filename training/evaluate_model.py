import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import array_to_vector
from pyspark.sql import SparkSession, functions as F

from training.train_model import evaluate_match_start, evaluate_predictions, evaluate_ranking_baseline


def model_fingerprint(model_dir):
    digest = hashlib.sha256()
    for path in sorted(model_dir.rglob("*")):
        if path.is_file():
            digest.update(path.relative_to(model_dir).as_posix().encode())
            digest.update(path.read_bytes())
    return digest.hexdigest()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Valutazione definitiva del modello fissato sul 2026")
    parser.add_argument("--data-dir", type=Path, default=Path("training/data/processed"))
    parser.add_argument("--model-dir", type=Path)
    parser.add_argument("--output", type=Path)
    arguments = parser.parse_args()
    with (arguments.data_dir / "schema.json").open(encoding="utf-8") as file:
        data_schema = json.load(file)
    if data_schema.get("target") != "match_winner":
        raise ValueError("Rigenerare i dataset per il target match_winner")
    if data_schema.get("dataset_stage") != "live":
        raise ValueError("Il test deve contenere gli stati live dell’incontro")
    model_dir = arguments.model_dir or Path("training/artifacts/model")
    output = arguments.output or model_dir.parent / "evaluation_report.json"

    # Il modello e il suo schema devono essere fissati prima di leggere il test.
    with (model_dir / "schema.json").open(encoding="utf-8") as file:
        schema = json.load(file)
    with (arguments.data_dir / "schema.json").open(encoding="utf-8") as file:
        if json.load(file) != schema:
            raise ValueError("Lo schema del test non coincide con quello del modello")
    with (model_dir / "training_report.json").open(encoding="utf-8") as file:
        training_report = json.load(file)
    if training_report["final_training_years"] != [2024, 2025]:
        raise ValueError("Il modello finale deve essere addestrato soltanto su 2024 e 2025")

    fingerprint = model_fingerprint(model_dir)
    test_path = arguments.data_dir / "test.parquet"
    test_fingerprint = hashlib.sha256(test_path.read_bytes()).hexdigest()
    if output.exists():
        with output.open(encoding="utf-8") as file:
            previous = json.load(file)
        if previous["model_sha256"] != fingerprint or previous["test_sha256"] != test_fingerprint:
            raise ValueError("Il report esiste già e si riferisce a un modello o test diverso")
        print("Valutazione già registrata:")
        print(json.dumps(previous["test_metrics"], indent=2))
    else:
        spark = SparkSession.builder.appName("TennisMatchEvaluation").master("local[2]").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        try:
            print("Lettura del test 2026...")
            test = spark.read.parquet(str(test_path)).withColumn("label", F.col("label").cast("double"))
            invalid = (
                F.col("match_date").isNull() | ~F.col("match_date").startswith("2026-")
                | F.col("label").isNull() | ~F.col("label").isin(0.0, 1.0)
            )
            if test.filter(invalid).limit(1).count():
                raise ValueError("Date o label non valide nel test 2026")
            summary = test.agg(
                F.count("*").alias("rows"), F.countDistinct("match_id").alias("matches"),
                F.countDistinct("label").alias("classes"),
                F.min("match_date").alias("date_start"), F.max("match_date").alias("date_end")
            ).first().asDict()
            if summary.pop("classes") != 2:
                raise ValueError("Il test deve contenere entrambe le classi")
            model = PipelineModel.load(str((model_dir / "pipeline").resolve()))
            predictions = model.transform(test).select(
                "label", "prediction", "probability", "sample_weight", "point_index"
            ).cache()
            try:
                test_metrics = evaluate_predictions(predictions)
                match_start_metrics = evaluate_match_start(predictions)
            finally:
                predictions.unpersist()

            # La baseline è quella del training 2024, fissata prima della validation.
            probability = training_report["baseline_probability"]
            baseline = test.select("label", "sample_weight").withColumn(
                "probability", array_to_vector(F.array(F.lit(1 - probability), F.lit(probability)))
            ).withColumn("prediction", F.lit(float(probability > 0.5)))
            baseline_metrics = evaluate_predictions(baseline)
            report = {
                "created_at": datetime.now(timezone.utc).isoformat(),
                "spark_version": spark.version,
                "model_sha256": fingerprint,
                "test_sha256": test_fingerprint,
                "test_year": 2026,
                "target": "match_winner",
                "dataset_stage": schema["dataset_stage"],
                "weighting": schema["weighting"],
                "evaluation_context": schema["evaluation"],
                "test_coverage": summary,
                "final_training_years": training_report["final_training_years"],
                "feature_columns": schema["feature_columns"],
                "selected_parameters": training_report["best_parameters"],
                "selection_data": "validation 2025 only",
                "classification_threshold": training_report["classification_threshold"],
                "test_metrics": test_metrics,
                "test_match_start_metrics": match_start_metrics,
                "baseline_training_year": 2024,
                "baseline_probability": probability,
                "baseline_test_metrics": baseline_metrics,
                "ranking_baseline_test": evaluate_ranking_baseline(test)
            }
            output.parent.mkdir(parents=True, exist_ok=True)
            temporary = output.with_suffix(".tmp")
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(report, file, indent=2, ensure_ascii=False, allow_nan=False)
            temporary.replace(output)
            print("Metriche definitive sul 2026:")
            print(json.dumps(test_metrics, indent=2))
            print(f"Report salvato in {output}")
        finally:
            spark.stop()
