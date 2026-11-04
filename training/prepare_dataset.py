import argparse
import json
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory

import pyarrow as pa
import pyarrow.parquet as parquet

from training import historical_features, player_rankings, tennis_rules


FEATURE_COLUMNS = ["best_of", "surface"] + historical_features.FEATURE_COLUMNS + player_rankings.FEATURE_COLUMNS + [
    "sets_a", "sets_b", "games_a", "games_b", "regular_score_a", "regular_score_b",
    "is_tiebreak", "tiebreak_target", "tiebreak_score_a", "tiebreak_score_b",
    "player_a_is_serving"
]
METADATA_COLUMNS = ["match_id", "match_date", "player_a_id", "player_b_id", "point_index", "is_inferred"]
ORIENTATION = tennis_rules.PLAYER_ORIENTATION


class PrepareDataset:
    def __init__(self, input_dir, output_dir, players_dir="training/data/players"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.split_years = {"train": 2024, "validation": 2025, "test": 2026}
        self.rankings = player_rankings.load_rankings(players_dir)
        with Path("training/config/tournaments.json").open(encoding="utf-8") as file:
            self.tournaments = {str(item["tournament_key"]): item for item in json.load(file)}

    def read_matches(self):
        matches = {}
        source_years = {}

        for year in self.split_years.values():
            year_dir = self.input_dir / str(year)
            if not year_dir.is_dir():
                raise FileNotFoundError(f"Directory annuale mancante: {year_dir}")

            for path in sorted(year_dir.glob("*.json")):
                with path.open(encoding="utf-8") as file:
                    data = json.load(file)

                if not isinstance(data, dict) or not isinstance(data.get("tournament"), list):
                    raise ValueError(f"Formato JSON non valido: {path}")

                for match in data["tournament"]:
                    if not isinstance(match, dict) or match.get("event_key") is None:
                        raise ValueError(f"Identificativo incontro mancante: {path}")

                    match_id = str(match["event_key"])

                    if match_id in matches:
                        if source_years[match_id] != year or matches[match_id] != match:
                            raise ValueError(f"Versioni o annate diverse per l'incontro {match_id}")

                    matches[match_id] = match
                    source_years[match_id] = year

        accepted = []

        for match_id, match in matches.items():

            tournament_key = str(match.get("tournament_key"))

            if match.get("event_status") != "Finished":
                continue

            match_date = date.fromisoformat(match["event_date"])

            if match_date.year != source_years[match_id] or match_date.isoformat() != match["event_date"]:
                raise ValueError(f"Data non coerente per l'incontro {match_id}")

            if match_date.year not in tennis_rules.VALID_YEARS:
                raise ValueError("Regole supportate soltanto per le annate 2023-2026")

            try:
                player_ids = [match["first_player_key"], match["second_player_key"]]

                if None in player_ids or str(player_ids[0]) == str(player_ids[1]):
                    raise ValueError("Identificativi giocatori mancanti o uguali")

                player_ids = list(map(str, player_ids))

                points = tennis_rules.decode_match(match)

                winner = tennis_rules.get_player(match["event_winner"])

            except (ValueError, KeyError, TypeError) as error:
                print(f"Incontro {match_id} scartato: {error}")
                continue

            # A è sempre il primo giocatore dell'API, B il secondo.
            player_a, player_b = 0, 1

            features = {
                "best_of": points[0]["best_of"],
                "surface": self.tournaments[tournament_key]["tournament_surface"]
            }

            for role, index in [("a", player_a), ("b", player_b)]:
                rank = self.rankings.get((player_ids[index], match_date.year - 1), 0)

                features[f"player_{role}_rank"] = rank
                features[f"player_{role}_rank_missing"] = int(rank == 0)

            accepted.append({
                "id": match_id, "date": match["event_date"], "tournament_key": tournament_key,
                "player_ids": player_ids,
                "label": int(winner == player_a), "points": points, "features": features
            })
        return sorted(accepted, key=lambda match: (match["date"], match["id"]))

    def create_rows(self, match):
        player_a, player_b = 0, 1
        common = {
            "match_id": match["id"], "match_date": match["date"],
            "player_a_id": match["player_ids"][player_a],
            "player_b_id": match["player_ids"][player_b],
            **match["features"],
            "label": match["label"]
        }
        live = []
        for point in match["points"]:
            is_tiebreak = point["tiebreak_target"] > 0
            # Le feature sono lo stato PRIMA del punto, il target è sempre il match.
            live.append({
                **common,
                "point_index": point["point_index"],
                "is_inferred": point["is_inferred"],
                "sets_a": point["sets"][player_a],
                "sets_b": point["sets"][player_b],
                "games_a": point["games"][player_a],
                "games_b": point["games"][player_b],
                "regular_score_a": 0 if is_tiebreak else point["score"][player_a],
                "regular_score_b": 0 if is_tiebreak else point["score"][player_b],
                "is_tiebreak": int(is_tiebreak),
                "tiebreak_target": point["tiebreak_target"],
                "tiebreak_score_a": point["score"][player_a] if is_tiebreak else 0,
                "tiebreak_score_b": point["score"][player_b] if is_tiebreak else 0,
                "player_a_is_serving": int(point["server"] == player_a),
                "sample_weight": 1.0 / len(match["points"])
            })
        return live

    def prepare_dataset(self, history_only=False):
        if self.output_dir.exists() and not history_only:
            raise FileExistsError(f"La directory {self.output_dir} esiste già: scegliere un nuovo --output-dir")

        matches = self.read_matches()

        if not matches:
            raise ValueError("Nessun match accettato per preparare dataset e storico")

        history_snapshot = historical_features.add_historical_features(
            matches,
            training_year=self.split_years["train"]
        )

        self.output_dir.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".dataset-", dir=self.output_dir.parent) as temporary:
            staging = Path(temporary) / "processed"
            staging.mkdir()
            history_path = staging / "player_history.json"

            with history_path.open("w", encoding="utf-8") as file:
                json.dump(history_snapshot, file, indent=2, ensure_ascii=False, allow_nan=False)

            if history_only:
                self.output_dir.mkdir(parents=True, exist_ok=True)
                destination = self.output_dir / "player_history.json"
                history_path.replace(destination)
                print(f"Storico esportato: {len(matches)} incontri, "
                      f"{len(history_snapshot['players'])} giocatori, file {destination}")
                return

            for split, year in self.split_years.items():
                year_matches = [match for match in matches if int(match["date"][:4]) == year]
                if not year_matches:
                    raise ValueError(f"Nessun incontro valido per {split} ({year})")
                live_rows = []

                for match in year_matches:
                    live_rows.extend(self.create_rows(match))

                parquet.write_table(
                    pa.Table.from_pylist(live_rows),
                    staging / f"{split}.parquet",
                    compression="snappy"
                )

                print(f"{split} ({year}): {len(year_matches)} incontri, {len(live_rows)} stati live")

            schema = {
                "target": "match_winner", "dataset_stage": "live", "schema_version": 4,
                "feature_columns": FEATURE_COLUMNS,
                "metadata_columns": METADATA_COLUMNS,
                "target_columns": ["label"], "weight_column": "sample_weight",
                "categorical_columns": ["surface"], "player_orientation": ORIENTATION,
                "training_year": self.split_years["train"]
            }

            with (staging / "schema.json").open("w", encoding="utf-8") as file:
                json.dump(schema, file, indent=2)
            staging.rename(self.output_dir)
        print(f"Dataset del vincitore del match salvati in {self.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dataset live per prevedere il vincitore dell'incontro")
    parser.add_argument("--input-dir", default="training/data/raw")
    parser.add_argument("--output-dir", default="training/data/processed")
    parser.add_argument("--players-dir", default="training/data/players")
    parser.add_argument("--history-only", action="store_true",
                        help="Crea o aggiorna solo player_history.json, senza riscrivere i dataset")
    arguments = parser.parse_args()
    PrepareDataset(arguments.input_dir, arguments.output_dir, arguments.players_dir).prepare_dataset(
        history_only=arguments.history_only
    )
