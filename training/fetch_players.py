import argparse
import json
import os
from pathlib import Path

import requests

def collect_player_ids(input_dir):
    player_ids = set()
    for year in [2024, 2025, 2026]:
        for path in sorted((input_dir / str(year)).glob("*.json")):
            with path.open(encoding="utf-8") as file:
                matches = json.load(file)["tournament"]
            for match in matches:
                players_id = [match.get("first_player_key") , match.get("second_player_key")]
                for player_id in players_id:
                    if player_id is None:
                        continue
                    player_id = str(player_id)
                    if not player_id:
                        continue
                    player_ids.add(player_id)

    if not player_ids:
        raise ValueError("Nessun giocatore trovato negli incontri")
    return sorted(player_ids)


def fetch_players(api_key, input_dir, output_dir):
    if not api_key or not api_key.strip():
        raise ValueError("Imposta la variabile d'ambiente API_TENNIS_KEY")
    player_ids = collect_player_ids(input_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session:
        for index, player_id in enumerate(player_ids, 1):
            path = output_dir / f"{player_id}.json"
            try:
                response = session.get(
                    "https://api.api-tennis.com/tennis/",
                    params={"method": "get_players", "APIkey": api_key, "player_key": player_id},
                    timeout=60
                )
                response.raise_for_status()
                data = response.json()
            except (requests.RequestException, ValueError):
                raise RuntimeError(f"Download del profilo {player_id} fallito; rilanciare per riprovare") from None
            if (
                not isinstance(data, dict)
                or data.get("success") != 1
                or not isinstance(data.get("result"), list)
            ):
                raise ValueError(f"Risposta get_players non valida per {player_id}")

            players = [
                player
                for player in data["result"]
                if isinstance(player, dict) and str(player.get("player_key")) == player_id
            ]

            if len(players) != 1 or not isinstance(players[0].get("stats"), list):
                raise ValueError(f"Profilo o statistiche mancanti per il giocatore {player_id}")

            temporary = path.with_suffix(".tmp")
            with temporary.open("w", encoding="utf-8") as file:
                json.dump(players[0], file, indent=2, ensure_ascii=False)
            temporary.replace(path)

            print(f"Profilo {index}/{len(player_ids)}: {player_id} salvato", flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download dei profili per i ranking della stagione precedente")
    parser.add_argument("--input-dir", type=Path, default=Path("training/data/raw"))
    parser.add_argument("--output-dir", type=Path, default=Path("training/data/players"))
    arguments = parser.parse_args()
    fetch_players(os.environ.get("API_TENNIS_KEY"), arguments.input_dir, arguments.output_dir)
