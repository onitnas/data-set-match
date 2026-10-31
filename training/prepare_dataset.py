import argparse
import hashlib
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
    "player_a_is_serving", "player_a_momentum", "player_b_momentum"
]
METADATA_COLUMNS = ["match_id", "match_date", "player_a_id", "player_b_id", "point_index", "is_inferred"]
ORIENTATION = "player_a_index = sha256(match_id).digest()[0] % 2; fixed_for_entire_match"


class PrepareDataset:
    def __init__(self, input_dir, output_dir, players_dir="training/data/players"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.split_years = {"train": 2024, "validation": 2025, "test": 2026}
        self.rankings = player_rankings.load_rankings(players_dir)
        with Path("training/config/tournaments.json").open(encoding="utf-8") as file:
            self.tournaments = {str(item["tournament_key"]): item for item in json.load(file)}

    def read_matches(self):
        self.report = {
            "target": "match_winner", "input_files": 0, "matches_read": 0,
            "identical_duplicates": 0, "filtered_matches": 0, "filtered_by_reason": {},
            "rejected_matches": 0, "rejected_by_reason": {}, "rejected_details": [],
            "ranking": player_rankings.POLICY,
            "player_orientation": ORIENTATION, "splits": {},
            "tournaments": {
                key: {"name": item["tournament_name"], "read": 0, "filtered": 0, "rejected": 0, "accepted": 0}
                for key, item in self.tournaments.items()
            }
        }
        matches = {}
        source_years = {}
        for year in self.split_years.values():
            year_dir = self.input_dir / str(year)
            if not year_dir.is_dir():
                raise FileNotFoundError(f"Directory annuale mancante: {year_dir}")
            for path in sorted(year_dir.glob("*.json")):
                self.report["input_files"] += 1
                with path.open(encoding="utf-8") as file:
                    data = json.load(file)
                if not isinstance(data, dict) or not isinstance(data.get("tournament"), list):
                    raise ValueError(f"Formato JSON non valido: {path}")
                for match in data["tournament"]:
                    self.report["matches_read"] += 1
                    if not isinstance(match, dict) or match.get("event_key") is None:
                        raise ValueError(f"Identificativo incontro mancante: {path}")
                    match_id = str(match["event_key"])
                    if match_id in matches:
                        if source_years[match_id] != year or matches[match_id] != match:
                            raise ValueError(f"Versioni o annate diverse per l'incontro {match_id}")
                        self.report["identical_duplicates"] += 1
                    matches[match_id] = match
                    source_years[match_id] = year

        self.report["unique_matches"] = len(matches)
        accepted = []
        for match_id, match in matches.items():
            tournament_key = str(match.get("tournament_key"))
            tournament_report = self.report["tournaments"].setdefault(tournament_key, {
                "name": match.get("tournament_name"), "read": 0, "filtered": 0, "rejected": 0, "accepted": 0
            })
            tournament_report["read"] += 1
            qualification = match.get("event_qualification")
            reason = None
            if qualification is not False and qualification != "False":
                reason = "Qualificazioni o indicatore qualificazioni non valido"
            elif match.get("event_status") != "Finished":
                reason = "Incontro non terminato"
            elif match.get("event_type_type") != "Atp Singles":
                reason = "Tipo incontro non ammesso"
            elif tournament_key not in self.tournaments:
                reason = "Torneo non configurato"
            if reason:
                self.report["filtered_matches"] += 1
                counts = self.report["filtered_by_reason"]
                counts[reason] = counts.get(reason, 0) + 1
                tournament_report["filtered"] += 1
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
                reason = str(error)
                self.report["rejected_matches"] += 1
                counts = self.report["rejected_by_reason"]
                counts[reason] = counts.get(reason, 0) + 1
                self.report["rejected_details"].append({
                    "match_id": match_id, "tournament_key": tournament_key, "reason": reason
                })
                tournament_report["rejected"] += 1
                continue

            # L'orientamento non dipende da ranking, battitore o risultato finale.
            player_a = hashlib.sha256(match_id.encode()).digest()[0] % 2
            features = {
                "best_of": points[0]["best_of"],
                "surface": self.tournaments[tournament_key]["tournament_surface"]
            }
            for role, index in [("a", player_a), ("b", 1 - player_a)]:
                rank = self.rankings.get((player_ids[index], match_date.year - 1), 0)
                features[f"player_{role}_rank"] = rank
                features[f"player_{role}_rank_missing"] = int(rank == 0)
            accepted.append({
                "id": match_id, "date": match["event_date"], "tournament_key": tournament_key,
                "player_ids": player_ids, "player_a_index": player_a,
                "label": int(winner == player_a), "points": points, "features": features
            })
            tournament_report["accepted"] += 1
        self.report["accepted_matches"] = len(accepted)
        return sorted(accepted, key=lambda match: (match["date"], match["id"]))

    def create_rows(self, match):
        player_a = match["player_a_index"]
        player_b = 1 - player_a
        common = {
            "match_id": match["id"], "match_date": match["date"],
            "player_a_id": match["player_ids"][player_a],
            "player_b_id": match["player_ids"][player_b],
            **match["features"], "label": match["label"]
        }
        live = []
        momentum = [0, 0]
        for point in match["points"]:
            is_tiebreak = point["tiebreak_target"] > 0
            # Le feature sono lo stato PRIMA del punto, il target è sempre il match.
            live.append({
                **common, "point_index": point["point_index"], "is_inferred": point["is_inferred"],
                "sets_a": point["sets"][player_a], "sets_b": point["sets"][player_b],
                "games_a": point["games"][player_a], "games_b": point["games"][player_b],
                "regular_score_a": 0 if is_tiebreak else point["score"][player_a],
                "regular_score_b": 0 if is_tiebreak else point["score"][player_b],
                "is_tiebreak": int(is_tiebreak), "tiebreak_target": point["tiebreak_target"],
                "tiebreak_score_a": point["score"][player_a] if is_tiebreak else 0,
                "tiebreak_score_b": point["score"][player_b] if is_tiebreak else 0,
                "player_a_is_serving": int(point["server"] == player_a),
                "player_a_momentum": momentum[player_a], "player_b_momentum": momentum[player_b],
                "sample_weight": 1.0 / len(match["points"])
            })
            winner = point["winner"]
            momentum[winner] += 1
            momentum[1 - winner] = 0
        return live

    def prepare_dataset(self):
        if self.output_dir.exists():
            raise FileExistsError(f"La directory {self.output_dir} esiste già: scegliere un nuovo --output-dir")
        matches = self.read_matches()
        self.report["historical_features"] = historical_features.add_historical_features(matches)
        self.output_dir.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=".dataset-", dir=self.output_dir.parent) as temporary:
            staging = Path(temporary) / "processed"
            staging.mkdir()
            for split, year in self.split_years.items():
                year_matches = [match for match in matches if int(match["date"][:4]) == year]
                if not year_matches:
                    raise ValueError(f"Nessun incontro valido per {split} ({year})")
                live_rows = []
                for match in year_matches:
                    live_rows.extend(self.create_rows(match))
                parquet.write_table(
                    pa.Table.from_pylist(live_rows), staging / f"{split}.parquet", compression="snappy"
                )
                self.report["splits"][split] = {
                    "year": year, "matches": len(year_matches),
                    "live_rows": len(live_rows), "inferred_points": sum(row["is_inferred"] for row in live_rows),
                    "player_a_wins": sum(match["label"] for match in year_matches),
                    "player_a_rank_missing_matches": sum(match["features"]["player_a_rank_missing"] for match in year_matches),
                    "player_b_rank_missing_matches": sum(match["features"]["player_b_rank_missing"] for match in year_matches),
                    "date_start": year_matches[0]["date"], "date_end": year_matches[-1]["date"],
                    "tournaments": {
                        key: sum(match["tournament_key"] == key for match in year_matches) for key in self.tournaments
                    }
                }
                print(f"{split} ({year}): {len(year_matches)} incontri, {len(live_rows)} stati live")

            schema = {
                "target": "match_winner", "dataset_stage": "live", "schema_version": 2,
                "label_meaning": "1 = player A wins the match; 0 = player B wins the match",
                "feature_columns": FEATURE_COLUMNS,
                "metadata_columns": METADATA_COLUMNS,
                "target_columns": ["label"], "weight_column": "sample_weight",
                "weighting": "each_match_total_weight_one",
                "categorical_columns": ["surface"], "player_orientation": ORIENTATION,
                "momentum": "consecutive_points_won_before_current_point; reset_opponent_on_win",
                "ranking": player_rankings.POLICY, "score_encoding": tennis_rules.GAME_POINTS,
                "historical_features": self.report["historical_features"]["policy"],
                "snapshot": "before_each_point_excluding_completed_match",
                "evaluation": "2026_reserved_for_match_target_evaluation; previously_inspected_for_point_target"
            }
            with (staging / "schema.json").open("w", encoding="utf-8") as file:
                json.dump(schema, file, indent=2)
            with (staging / "report.json").open("w", encoding="utf-8") as file:
                json.dump(self.report, file, indent=2, ensure_ascii=False, allow_nan=False)
            staging.rename(self.output_dir)
        print(f"Accettati: {self.report['accepted_matches']}; scartati: {self.report['rejected_matches']}")
        print(f"Dataset del vincitore del match salvati in {self.output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dataset live per prevedere il vincitore dell'incontro")
    parser.add_argument("--input-dir", default="training/data/raw")
    parser.add_argument("--output-dir", default="training/data/processed")
    parser.add_argument("--players-dir", default="training/data/players")
    arguments = parser.parse_args()
    PrepareDataset(arguments.input_dir, arguments.output_dir, arguments.players_dir).prepare_dataset()
