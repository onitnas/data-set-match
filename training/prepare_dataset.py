import json
import os
from datetime import date

import pyarrow as pa
import pyarrow.parquet as parquet

from training import tennis_rules


class PrepareDataset:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.split_years = {"train": 2024, "validation": 2025, "test": 2026}
        self.feature_columns = [
            "best_of", "sets_server", "sets_receiver", "games_server", "games_receiver",
            "regular_score_server", "regular_score_receiver", "is_tiebreak",
            "tiebreak_target", "tiebreak_score_server", "tiebreak_score_receiver"
        ]
        self.metadata_columns = [
            "match_id", "match_date", "server_id", "receiver_id", "point_index", "is_inferred"
        ]
        self.tournaments = []
        self.tournament_names = {}
        with open("training/config/tournaments.json", "r", encoding="utf-8") as f:
            tournaments = json.load(f)
        for tournament in tournaments:
            tournament_key = str(tournament["tournament_key"])
            self.tournaments.append(tournament_key)
            self.tournament_names[tournament_key] = tournament["tournament_name"]

    def create_row(self, match, point):
        games = point["games"]
        sets = point["sets"]
        target = point["tiebreak_target"]
        server = point["server"]
        receiver = 1 - server
        player_ids = [str(match["first_player_key"]), str(match["second_player_key"])]
        row = {
            "match_id": str(match["event_key"]),
            "match_date": match["event_date"],
            "server_id": player_ids[server],
            "receiver_id": player_ids[receiver],
            "point_index": point["point_index"],
            "is_inferred": point["is_inferred"],
            "best_of": point["best_of"],
            "sets_server": sets[server],
            "sets_receiver": sets[receiver],
            "games_server": games[server],
            "games_receiver": games[receiver],
            "regular_score_server": 0,
            "regular_score_receiver": 0,
            "is_tiebreak": int(target > 0),
            "tiebreak_target": target,
            "tiebreak_score_server": 0,
            "tiebreak_score_receiver": 0,
            "label": int(point["winner"] == server)
        }
        if target > 0:
            row["tiebreak_score_server"] = point["score"][server]
            row["tiebreak_score_receiver"] = point["score"][receiver]
        else:
            row["regular_score_server"] = point["score"][server]
            row["regular_score_receiver"] = point["score"][receiver]
        return row

    def read_matches(self):
        self.report = {
            "input_files": 0,
            "matches_read": 0,
            "unique_matches": 0,
            "identical_duplicates": 0,
            "filtered_matches": 0,
            "filtered_by_reason": {},
            "rejected_matches": 0,
            "rejected_by_reason": {},
            "rejected_details": [],
            "accepted_matches": 0,
            "tournaments": {
                key: {"name": name, "read": 0, "filtered": 0, "rejected": 0, "accepted": 0}
                for key, name in self.tournament_names.items()
            },
            "splits": {}
        }
        matches = {}
        source_years = {}
        input_files = []
        for year in self.split_years.values():
            year_dir = os.path.join(self.input_dir, str(year))
            for filename in os.listdir(year_dir):
                if filename.endswith(".json"):
                    input_files.append((year, os.path.join(year_dir, filename)))
        for year, filename in input_files:
            self.report["input_files"] += 1
            with open(filename, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or not isinstance(data.get("tournament"), list):
                raise ValueError(f"Formato JSON non valido: {filename}")
            for match in data["tournament"]:
                self.report["matches_read"] += 1
                if not isinstance(match, dict) or match.get("event_key") is None:
                    raise ValueError(f"Identificativo incontro mancante: {filename}")
                match_id = str(match["event_key"])
                if match_id in matches:
                    if source_years[match_id] != year:
                        raise ValueError(f"Incontro {match_id} presente in più annate")
                    if matches[match_id] != match:
                        raise ValueError(f"Versioni diverse dell'incontro {match_id}")
                    self.report["identical_duplicates"] += 1
                matches[match_id] = match
                source_years[match_id] = year

        self.report["unique_matches"] = len(matches)
        cleaned_matches = []
        for match in matches.values():
            tournament_key = str(match.get("tournament_key"))
            tournament_report = self.report["tournaments"].setdefault(
                tournament_key,
                {"name": match.get("tournament_name"), "read": 0, "filtered": 0, "rejected": 0, "accepted": 0}
            )
            tournament_report["read"] += 1
            qualification = match.get("event_qualification")
            filter_reason = None
            if qualification is not False and qualification != "False":
                filter_reason = "Qualificazioni o indicatore qualificazioni non valido"
            elif match.get("event_status") != "Finished":
                filter_reason = "Incontro non terminato"
            elif match.get("event_type_type") != "Atp Singles":
                filter_reason = "Tipo incontro non ammesso"
            elif tournament_key not in self.tournaments:
                filter_reason = "Torneo non configurato"
            if filter_reason is not None:
                self.report["filtered_matches"] += 1
                reasons = self.report["filtered_by_reason"]
                reasons[filter_reason] = reasons.get(filter_reason, 0) + 1
                tournament_report["filtered"] += 1
                continue
            match_date = date.fromisoformat(match["event_date"])
            if match_date.year not in tennis_rules.VALID_YEARS:
                raise ValueError("Verificare le regole per anni diversi dal 2023-2026")
            if match_date.year != source_years[str(match["event_key"])]:
                raise ValueError(f"Incontro {match['event_key']}: data non coerente con la cartella annuale")
            try:
                if match.get("first_player_key") is None or match.get("second_player_key") is None:
                    raise ValueError("Identificativi giocatori mancanti")
                if str(match["first_player_key"]) == str(match["second_player_key"]):
                    raise ValueError("Giocatori uguali")
                if match_date.isoformat() != match["event_date"]:
                    raise ValueError("La data deve avere formato YYYY-MM-DD")
                points = tennis_rules.decode_match(match)
                rows = []
                for point in points:
                    rows.append(self.create_row(match, point))
            except (ValueError, KeyError, TypeError) as error:
                print(f"Incontro {match['event_key']} escluso: {error}")
                reason = str(error)
                self.report["rejected_matches"] += 1
                reasons = self.report["rejected_by_reason"]
                reasons[reason] = reasons.get(reason, 0) + 1
                self.report["rejected_details"].append({
                    "match_id": str(match["event_key"]),
                    "tournament_key": tournament_key,
                    "reason": reason
                })
                tournament_report["rejected"] += 1
                continue
            cleaned_matches.append({
                "date": match["event_date"], "id": str(match["event_key"]),
                "tournament_key": tournament_key, "rows": rows
            })
            self.report["accepted_matches"] += 1
            tournament_report["accepted"] += 1
        cleaned_matches.sort(key=lambda match: (match["date"], match["id"]))
        return cleaned_matches

    def create_splits(self, matches):
        splits = {name: [] for name in self.split_years}
        split_by_year = {year: name for name, year in self.split_years.items()}
        for match in matches:
            year = date.fromisoformat(match["date"]).year
            splits[split_by_year[year]].append(match)
        for name, split_matches in splits.items():
            if not split_matches:
                raise ValueError(f"Nessun incontro valido per {name} ({self.split_years[name]})")
        return splits

    def prepare_dataset(self):
        if os.path.exists(self.output_dir):
            raise FileExistsError("La directory di output esiste già: scegli un nuovo percorso")
        # 1. Leggiamo e ricostruiamo gli incontri validi.
        matches = self.read_matches()
        split_matches = self.create_splits(matches)
        # 2. Ogni stagione viene assegnata interamente al proprio split.
        datasets = {
            name: [row for match in year_matches for row in match["rows"]]
            for name, year_matches in split_matches.items()
        }

        # 3. Salviamo i Parquet e la lista delle colonne.
        os.makedirs(self.output_dir)
        for name in datasets:
            rows = datasets[name]
            self.report["splits"][name] = {
                "year": self.split_years[name],
                "matches": len(split_matches[name]),
                "points": len(rows),
                "inferred_points": sum(row["is_inferred"] for row in rows),
                "date_start": min(row["match_date"] for row in rows),
                "date_end": max(row["match_date"] for row in rows),
                "tournaments": {
                    key: sum(match["tournament_key"] == key for match in split_matches[name])
                    for key in self.tournaments
                }
            }
            table = pa.Table.from_pylist(datasets[name])
            filename = os.path.join(self.output_dir, name + ".parquet")
            parquet.write_table(table, filename, compression="snappy")
            print(f"{name}: {len(datasets[name])} punti")
        schema = {
            "feature_columns": self.feature_columns,
            "metadata_columns": self.metadata_columns,
            "target_columns": ["label"],
            "score_encoding": tennis_rules.GAME_POINTS
        }
        with open(os.path.join(self.output_dir, "schema.json"), "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=2)
        with open(os.path.join(self.output_dir, "report.json"), "w", encoding="utf-8") as f:
            json.dump(self.report, f, indent=2, ensure_ascii=False)
        print(
            f"Incontri unici: {self.report['unique_matches']}, "
            f"accettati: {self.report['accepted_matches']}, "
            f"filtrati: {self.report['filtered_matches']}, "
            f"scartati: {self.report['rejected_matches']}"
        )
        print(f"Dataset salvato in {self.output_dir}")


if __name__ == "__main__":
    dataset = PrepareDataset(
        "training/data/raw",
        "training/data/processed/"
    )
    dataset.prepare_dataset()
