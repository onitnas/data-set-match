"""Coordina stato e feature nel driver; applica la pipeline con Spark ML."""

import hashlib
import json
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array
from pyspark.sql.types import BooleanType, DoubleType, LongType, StringType, StructField, StructType

from streaming.match_features import FEATURE_COLUMNS, FeatureBuilder
from streaming.match_state_store import MatchStateStore
from streaming.match_state import decode_live_state
from streaming.player_history import PlayerHistory
from training.player_rankings import load_rankings
from training.tennis_rules import PLAYER_ORIENTATION


FEATURE_SCHEMA = StructType([
    StructField("request_id", LongType())
] + [StructField(column, StringType() if column == "surface" else DoubleType()) for column in FEATURE_COLUMNS])

OUTPUT_COLUMNS = [
    "match_id", "player_a", "player_b", "status", "api_score", "prediction_status",
    "qualification_status", "sets_a", "sets_b", "games_a", "games_b",
    "player_a_win_probability",
    "player_b_win_probability", "predicted_winner", "parse_ok", "state_ok", "state_source",
    "skip_reason", "partition", "offset"
]
OUTPUT_SCHEMA = StructType([
    StructField(column, BooleanType() if column in ["parse_ok", "state_ok"] else
                LongType() if column in ["partition", "offset"] else
                DoubleType() if column in FEATURE_COLUMNS or column.endswith("win_probability") else StringType())
    for column in OUTPUT_COLUMNS
])


class LivePredictor:
    def __init__(self, model_dir, history_path, players_dir, tournaments_path, history_dir):
        model_dir = Path(model_dir)
        with (model_dir / "schema.json").open(encoding="utf-8") as file:
            schema = json.load(file)
        if (schema.get("target") != "match_winner" or schema.get("dataset_stage") != "live"
                or schema.get("feature_columns") != FEATURE_COLUMNS):
            raise ValueError("Il modello deve usare le 21 feature live per il vincitore del match")
        if schema.get("player_orientation") != PLAYER_ORIENTATION:
            raise ValueError("Rigenerare dataset e modello con player_a = 0 e player_b = 1")
        self.model = PipelineModel.load(str((model_dir / "pipeline").resolve()))
        # La cache vale soltanto per questa pipeline, anche dopo un riavvio.
        digest = hashlib.sha256((model_dir / "schema.json").read_bytes())
        for path in sorted((model_dir / "pipeline").rglob("*")):
            if path.is_file():
                digest.update(str(path.relative_to(model_dir)).encode())
                digest.update(path.read_bytes())
        self.model_id = digest.hexdigest()
        with Path(tournaments_path).open(encoding="utf-8") as file:
            self.tournaments = {str(item["tournament_key"]): item for item in json.load(file)}
        self.rankings = load_rankings(players_dir)
        self.history = PlayerHistory(history_path, history_dir, self.tournaments)
        self.match_states = MatchStateStore(Path(history_dir).parent / "match_states.json")
        if self.history.training_year != schema.get("training_year"):
            raise ValueError("L'anno del prior storico non coincide con quello del modello")
        print(f"Cache live: {len(self.match_states.predictions)} match; ultimo batch {self.match_states.last_batch_id}", flush=True)
        print(f"Modello caricato da {model_dir}: inferenza, senza riaddestramento.", flush=True)

    def predict_features(self, spark, requests):
        """Un DataFrame con i soli input nuovi; nessuna UDF o join degli stati."""
        frame = spark.createDataFrame(requests, FEATURE_SCHEMA)
        return self.model.transform(frame).select(
            "request_id", vector_to_array("probability")[1].alias("probability_a"), "prediction"
        ).collect()

    def process_records(self, records, batch_id, spark):
        replay = self.match_states.replay(records, batch_id, self.model_id)
        if replay is not None:
            return replay
        states = [decode_live_state(row["match"]) if row["parse_ok"] else
                  {"state_ok": False, "state_reason": "JSON non valido"} for row in records]

        # Aggiornamenti idempotenti per ID; consultabili solo dal giorno successivo.
        changed = False
        for row, state in sorted(zip(records, states), key=lambda item: (
                item[0]["timestamp"], item[0]["partition"], item[0]["offset"])):
            if row["parse_ok"] and state.get("state_ok") and state.get("is_finished"):
                try:
                    changed = self.history.update_finished_match(
                        row["match"], row["match_id"], row["partition"], row["offset"], row["timestamp"]
                    ) or changed
                except (ValueError, KeyError, TypeError, IndexError) as error:
                    print(f"Storico non aggiornato a {row['partition']}:{row['offset']}: {error}", flush=True)
        if changed:
            self.history.rebuild_timelines()

        builder = FeatureBuilder(self.tournaments, self.rankings,
                                 self.history.timelines, self.history.training_prior)
        cache = dict(self.match_states.predictions) if self.match_states.model_id == self.model_id else {}
        requests, pending, outputs = [], {}, []
        for row, state in zip(records, states):
            features = builder.build(row["match_id"], row["match"], state, row["parse_ok"])
            output = {column: features.get(column) for column in OUTPUT_COLUMNS}
            output.update(match_id=features.get("canonical_match_id", row["match_id"]),
                          status=(row["match"] or {}).get("event_status"), parse_ok=row["parse_ok"],
                          state_ok=state.get("state_ok"), state_source=state.get("state_source"),
                          partition=row["partition"], offset=row["offset"])
            if features["prediction_status"] == "finished":
                cache.pop(output["match_id"], None)
            reference = None
            if features["features_ok"]:
                values = [features[column] if column == "surface" else float(features[column])
                          for column in FEATURE_COLUMNS]
                match_id = output["match_id"]
                previous = cache.get(match_id)
                if previous is not None and previous["features"] == values:
                    reference = previous
                    output["prediction_status"] = "unchanged"
                else:
                    # Riutilizza anche input uguali ripetuti all'interno del batch.
                    key = (match_id, tuple(values))
                    if key not in pending:
                        request_id = len(requests)
                        requests.append((request_id, *values))
                        pending[key] = {"features": values, "request_id": request_id}
                    reference = pending[key]
                    cache[match_id] = reference
            outputs.append((output, reference))

        if requests:
            results = {row["request_id"]: row for row in self.predict_features(spark, requests)}
            for reference in pending.values():
                result = results[reference.pop("request_id")]
                reference.update(probability_a=result["probability_a"], prediction=result["prediction"])

        output_rows = []
        for output, reference in outputs:
            if reference is not None:
                probability = reference["probability_a"]
                output.update(player_a_win_probability=probability, player_b_win_probability=1 - probability,
                              predicted_winner=output["player_a"] if reference["prediction"] == 1 else output["player_b"])
            # Schema esplicito: PySpark DoubleType richiede float anche per i conteggi.
            for column in FEATURE_COLUMNS:
                if column in output and output[column] is not None:
                    output[column] = float(output[column])
            output_rows.append(output)

        self.match_states.commit(records, batch_id, cache, output_rows, self.model_id)
        print(f"Batch {batch_id}: {len(records)} eventi, {len(requests)} nuovi input al modello", flush=True)
        return output_rows

    def process_batch(self, batch, batch_id):
        # Kafka mantiene l'ordine degli offset per partizione. Ogni match deve
        # continuare a usare la stessa chiave/partizione, come nell'ingestion.
        records = [row.asDict(recursive=True) for row in batch.select(
            "match_id", "data.match", "parse_ok", "partition", "offset", "timestamp"
        ).orderBy("partition", "offset").toLocalIterator()]
        if not records:
            return
        output = self.process_records(records, batch_id, batch.sparkSession)
        print(f"\nBatch: {batch_id}", flush=True)
        batch.sparkSession.createDataFrame(output, OUTPUT_SCHEMA).show(10, truncate=False)
