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
        self.feature_columns = [
            "best_of", "sets_server", "sets_receiver", "games_server", "games_receiver",
            "regular_score_server", "regular_score_receiver", "is_tiebreak",
            "tiebreak_target", "tiebreak_score_server", "tiebreak_score_receiver"
        ]
        self.metadata_columns = [
            "match_id", "match_date", "server_id", "receiver_id", "point_index", "is_inferred"
        ]
        self.tournaments = []
        with open("training/config/tournaments.json", "r", encoding="utf-8") as f:
            tournaments = json.load(f)
        for tournament in tournaments:
            self.tournaments.append(str(tournament["tournament_key"]))

    def create_row(self, match, point):
        # Tutti i punteggi descrivono la situazione prima del punto.
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
        matches = {}
        for filename in sorted(os.listdir(self.input_dir)):
            if not filename.endswith(".json"):
                continue
            with open(os.path.join(self.input_dir, filename), "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict) or not isinstance(data.get("tournament"), list):
                raise ValueError(f"Formato JSON non valido: {filename}")
            for match in data["tournament"]:
                if not isinstance(match, dict) or match.get("event_key") is None:
                    raise ValueError(f"Identificativo incontro mancante: {filename}")
                match_id = str(match["event_key"])
                if match_id in matches and matches[match_id] != match:
                    raise ValueError(f"Versioni diverse dell'incontro {match_id}")
                matches[match_id] = match

        cleaned_matches = []
        for match in matches.values():
            qualification = match.get("event_qualification")
            if qualification is not False and qualification != "False":
                continue
            if match.get("event_status") != "Finished" or match.get("event_type_type") != "Atp Singles":
                continue
            if str(match.get("tournament_key")) not in self.tournaments:
                continue
            match_date = date.fromisoformat(match["event_date"])
            if match_date.year not in tennis_rules.VALID_YEARS:
                raise ValueError("Verificare le regole per anni diversi dal 2023-2026")
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
                continue
            cleaned_matches.append({"date": match["event_date"], "id": str(match["event_key"]), "rows": rows})
        cleaned_matches.sort(key=lambda match: (match["date"], match["id"]))
        return cleaned_matches

    def create_splits(self, matches):
        day_counts = {}
        for match in matches:
            day = match["date"]
            if day not in day_counts:
                day_counts[day] = 0
            day_counts[day] += 1
        days = sorted(day_counts)
        if len(days) < 3:
            raise ValueError("Servono almeno tre giornate valide per creare i tre split")

        cumulative = []
        count = 0
        for day in days:
            count += day_counts[day]
            cumulative.append(count)
        train_end = 0
        for index in range(len(days) - 2):
            if abs(cumulative[index] - len(matches) * 0.70) < abs(cumulative[train_end] - len(matches) * 0.70):
                train_end = index
        validation_end = train_end + 1
        for index in range(train_end + 1, len(days) - 1):
            if abs(cumulative[index] - len(matches) * 0.85) < abs(cumulative[validation_end] - len(matches) * 0.85):
                validation_end = index
        return days[train_end], days[validation_end]

    def prepare_dataset(self):
        if os.path.exists(self.output_dir):
            raise FileExistsError("La directory di output esiste già: scegli un nuovo percorso")
        # 1. Leggiamo e ricostruiamo gli incontri validi.
        matches = self.read_matches()
        train_end, validation_end = self.create_splits(matches)
        # 2. Raccogliamo le righe nei tre gruppi, senza dividere le giornate.
        datasets = {"train": [], "validation": [], "test": []}
        for match in matches:
            if match["date"] <= train_end:
                datasets["train"].extend(match["rows"])
            elif match["date"] <= validation_end:
                datasets["validation"].extend(match["rows"])
            else:
                datasets["test"].extend(match["rows"])

        # 3. Salviamo i Parquet e la lista delle colonne.
        os.makedirs(self.output_dir)
        for name in datasets:
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
        print(f"Dataset salvato in {self.output_dir}")


if __name__ == "__main__":
    year = input("Inserisci l'anno del dataset (es. 2024): ").strip()
    dataset = PrepareDataset(
        f"training/data/raw/{year}",
        f"training/data/processed/{year}"
    )
    dataset.prepare_dataset()
