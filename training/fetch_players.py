import argparse
import json
import os
from pathlib import Path

import requests

from training import tennis_rules


def collect_player_ids(input_dir):
    with open("training/config/tournaments.json", encoding="utf-8") as file:
        tournaments = {str(tournament["tournament_key"]) for tournament in json.load(file)}
    player_ids = set()
    for year in [2024, 2025, 2026]:
        for path in sorted((input_dir / str(year)).glob("*.json")):
            with path.open(encoding="utf-8") as file:
                matches = json.load(file)["tournament"]
            for match in matches:
                qualification = match.get("event_qualification")
                if qualification is not False and qualification != "False":
                    continue
                if match.get("event_status") != "Finished" or match.get("event_type_type") != "Atp Singles":
                    continue
                if str(match.get("tournament_key")) not in tournaments:
                    continue
                try:
                    tennis_rules.decode_match(match)
                    first = match["first_player_key"]
                    second = match["second_player_key"]
                    if first is None or second is None or str(first) == str(second):
                        continue
                except (ValueError, KeyError, TypeError):
                    continue
                player_ids.update([str(first), str(second)])
    if not player_ids:
        raise ValueError("Nessun giocatore trovato negli incontri validi")
    return sorted(player_ids)


def fetch_players(api_key, input_dir, output_dir):
    if not api_key or not api_key.strip():
        raise ValueError("Imposta la variabile d'ambiente API_TENNIS_KEY")
    player_ids = collect_player_ids(input_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with requests.Session() as session:
        for index, player_id in enumerate(player_ids, 1):
            path = output_dir / f"{player_id}.json"
            if path.exists():
                print(f"Profilo {index}/{len(player_ids)}: {player_id} già presente", flush=True)
                continue
            try:
                response = session.get(
                    "https://api.api-tennis.com/tennis/",
                    params={"method": "get_players", "APIkey": api_key, "player_key": player_id},
                    timeout=60
                )
                response.raise_for_status()
                data = response.json()
            except (requests.RequestException, ValueError):
                # Le eccezioni requests possono contenere l'URL con la chiave API.
                raise RuntimeError(f"Download del profilo {player_id} fallito; rilanciare per riprendere") from None
            if not isinstance(data, dict) or data.get("success") != 1 or not isinstance(data.get("result"), list):
                raise ValueError(f"Risposta get_players non valida per {player_id}")
            players = [player for player in data["result"]
                       if isinstance(player, dict) and str(player.get("player_key")) == player_id]
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
