import json
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from training.player_rankings import load_rankings


GAME_POINTS = {"0": 0, "15": 1, "30": 2, "40": 3, "A": 4}
BEST_OF_5 = {"1236", "2155", "2053", "1217"}

FEATURE_COLUMNS = [
    "best_of", "surface",
    "player_a_rank", "player_b_rank",
    "player_a_rank_missing", "player_b_rank_missing",
    "sets_a", "sets_b",
    "games_a", "games_b",
    "regular_score_a", "regular_score_b",
    "is_tiebreak", "tiebreak_target",
    "tiebreak_score_a", "tiebreak_score_b",
    "player_a_is_serving"
]

METADATA_COLUMNS = [
    "match_id", "match_date",
    "player_a_id", "player_b_id", "point_index"
]

PLAYER_ORIENTATION = "player_a_index = 0; player_b_index = 1; fixed_for_entire_match"


def score_values(score, tiebreak=False):
    if score is None:
        raise ValueError("Punteggio mancante")
    a, b = [value.strip() for value in score.split("-")]

    if tiebreak:
        return [int(a), int(b)]

    return [GAME_POINTS[a], GAME_POINTS[b]]


def prepare_match(match, tournaments, rankings):
    if match["event_status"] != "Finished":
        return []

    match_date = match["event_date"]
    year = int(match_date[:4])

    player_a = str(match["first_player_key"])
    player_b = str(match["second_player_key"])

    rank_a = rankings.get((player_a, year - 1), 0)
    rank_b = rankings.get((player_b, year - 1), 0)

    tournament_key = str(match["tournament_key"])
    best_of = 5 if tournament_key in BEST_OF_5 else 3

    label = 1 if match["event_winner"] == "First Player" else 0

    common = {
        "match_id": str(match["event_key"]),
        "match_date": match_date,
        "player_a_id": player_a,
        "player_b_id": player_b,
        "best_of": best_of,
        "surface": tournaments[tournament_key],
        "player_a_rank": rank_a,
        "player_b_rank": rank_b,
        "player_a_rank_missing": int(rank_a == 0),
        "player_b_rank_missing": int(rank_b == 0),
        "label": label
    }

    records = sorted(
        match["pointbypoint"],
        key=lambda record: (
            int(record["set_number"].split()[1]),
            "TieBreak" in record["set_number"],
            int(record["number_game"])
        )
    )

    # Il feed può aggiungere un game vuoto dopo la fine del match.
    if records and records[-1]["score"] is None and not records[-1].get("points"):
        records.pop()
    if not records:
        return []

    rows = [{
        **common,
        "point_index": 0,
        "sets_a": 0,
        "sets_b": 0,
        "games_a": 0,
        "games_b": 0,
        "regular_score_a": 0,
        "regular_score_b": 0,
        "is_tiebreak": 0,
        "tiebreak_target": 0,
        "tiebreak_score_a": 0,
        "tiebreak_score_b": 0,
        "player_a_is_serving": int(records[0]["player_served"] == "First Player")
    }]
    sets = [0, 0]
    games = [0, 0]
    current_set = 1
    final_tiebreak_games = None

    for record in records:
        set_number = int(record["set_number"].split()[1])
        is_tiebreak = "TieBreak" in record["set_number"]

        # Quando cambia il set, aggiorniamo i set vinti.
        if set_number != current_set:
            if final_tiebreak_games is not None:
                games = final_tiebreak_games

            winner = 0 if games[0] > games[1] else 1
            sets[winner] += 1

            games = [0, 0]
            final_tiebreak_games = None
            current_set = set_number

        # Il game 13 è il riepilogo finale del tie-break.
        if not is_tiebreak and int(record["number_game"]) == 13:
            final_tiebreak_games = score_values(record["score"], True)
            continue

        # Punti del tie-break.
        if is_tiebreak:
            point_records = [record]
            tiebreak_target = 10 if best_of == 5 and sum(sets) == 4 else 7

        # Punti dei game normali.
        else:
            point_records = sorted(
                record["points"],
                key=lambda point: int(point["number_point"])
            )
            tiebreak_target = 0

        server = 0 if record["player_served"] == "First Player" else 1
        # Dopo un punto dispari del tie-break cambia il battitore.
        if is_tiebreak and int(record["number_game"]) % 2:
            server = 1 - server

        for point in point_records:
            if point["score"] is None:
                continue
            score = score_values(point["score"], is_tiebreak)

            # Il riepilogo del game 13 dà già il risultato del set.
            if is_tiebreak and max(score) >= tiebreak_target and abs(score[0] - score[1]) >= 2:
                continue

            rows.append({
                **common,
                "point_index": len(rows),
                "sets_a": sets[0],
                "sets_b": sets[1],
                "games_a": games[0],
                "games_b": games[1],
                "regular_score_a": 0 if is_tiebreak else score[0],
                "regular_score_b": 0 if is_tiebreak else score[1],
                "is_tiebreak": int(is_tiebreak),
                "tiebreak_target": tiebreak_target,
                "tiebreak_score_a": score[0] if is_tiebreak else 0,
                "tiebreak_score_b": score[1] if is_tiebreak else 0,
                "player_a_is_serving": int(server == 0)
            })

        # Il risultato del game è già presente nel JSON.
        if not is_tiebreak:
            games = score_values(record["score"], True)

    if len(rows) == 1:
        return []

    # Ultimo set.
    if final_tiebreak_games is not None:
        games = final_tiebreak_games

    winner = 0 if games[0] > games[1] else 1
    sets[winner] += 1

    # Unico controllo di coerenza sul risultato finale.
    if sets != score_values(match["event_final_result"], True):
        raise ValueError("Risultato finale non coerente")

    # Ogni match ha peso complessivo pari a 1.
    weight = 1.0 / len(rows)

    for row in rows:
        row["sample_weight"] = weight

    return rows


def main():
    input_dir = Path("training/data/raw")
    output_dir = Path("training/data/processed")

    if output_dir.exists():
        raise FileExistsError(
            f"{output_dir} esiste già. "
            "Rinominala o eliminala prima di rigenerare i dataset."
        )

    output_dir.mkdir(parents=True)

    rankings = load_rankings("training/data/players")

    with open("training/config/tournaments.json", encoding="utf-8") as file:
        tournaments = {
            str(item["tournament_key"]): item["tournament_surface"]
            for item in json.load(file)
        }

    datasets = {
        2024: "train",
        2025: "validation",
        2026: "test"
    }

    for year, name in datasets.items():
        writer = None
        total_matches = 0
        total_points = 0
        seen_matches = set()

        try:
            for path in sorted((input_dir / str(year)).glob("*.json")):
                with path.open(encoding="utf-8") as file:
                    data = json.load(file)

                for match in data["tournament"]:
                    match_id = str(match["event_key"])

                    if match_id in seen_matches:
                        continue
                    seen_matches.add(match_id)

                    try:
                        rows = prepare_match(match, tournaments, rankings)
                    except (KeyError, ValueError, TypeError) as error:
                        print(f"Match {match_id} scartato: {error}")
                        continue

                    if not rows:
                        continue

                    if writer is None:
                        table = pa.Table.from_pylist(rows)
                        writer = pq.ParquetWriter(
                            output_dir / f"{name}.parquet",
                            table.schema,
                            compression="snappy"
                        )
                    else:
                        table = pa.Table.from_pylist(rows, schema=writer.schema)

                    writer.write_table(table)
                    
                    total_matches += 1
                    total_points += len(rows)

        finally:
            if writer is not None:
                writer.close()

        print(
            f"{name} ({year}): "
            f"{total_matches} match, {total_points} stati"
        )

    schema = {
        "target": "match_winner",
        "dataset_stage": "live",
        "schema_version": 6,
        "feature_columns": FEATURE_COLUMNS,
        "metadata_columns": METADATA_COLUMNS,
        "target_columns": ["label"],
        "weight_column": "sample_weight",
        "categorical_columns": ["surface"],
        "player_orientation": PLAYER_ORIENTATION,
        "training_year": 2024
    }

    with (output_dir / "schema.json").open("w", encoding="utf-8") as file:
        json.dump(schema, file, indent=2)

    print("Preparazione completata.")


if __name__ == "__main__":
    main()
