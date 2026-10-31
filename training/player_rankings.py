import json
from pathlib import Path


FEATURE_COLUMNS = ["player_a_rank", "player_b_rank", "player_a_rank_missing", "player_b_rank_missing"]
POLICY = {
    "source": "API Tennis get_players stats, singles only",
    "season": "match_year_minus_one",
    "missing_value": 0,
    "missing_indicators": ["player_a_rank_missing", "player_b_rank_missing"]
}


def load_rankings(profiles_dir):
    paths = sorted(path for path in Path(profiles_dir).glob("*.json") if path.stem.isdecimal())
    if not paths:
        raise FileNotFoundError("Profili giocatori assenti: eseguire prima python -m training.fetch_players")
    rankings = {}
    for path in paths:
        with path.open(encoding="utf-8") as file:
            player = json.load(file)
        player_id = str(player["player_key"])
        for stats in player["stats"]:
            if stats.get("type") != "singles":
                continue
            season = str(stats.get("season", ""))
            rank = str(stats.get("rank", ""))
            if not season.isdecimal() or not rank.isdecimal() or int(rank) < 1:
                continue
            key = (player_id, int(season))
            if key in rankings and rankings[key] != int(rank):
                raise ValueError(f"Ranking discordanti per il giocatore {player_id}, stagione {season}")
            rankings[key] = int(rank)
    if not rankings:
        raise ValueError("Nessun ranking singles valido nei profili giocatori")
    return rankings
