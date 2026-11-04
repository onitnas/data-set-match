from itertools import groupby


FEATURE_COLUMNS = [
    "player_a_serve_win_rate", "player_a_return_win_rate",
    "player_b_serve_win_rate", "player_b_return_win_rate"
]


def add_historical_features(matches, training_year):
    player_history = {}
    training_points = 0
    training_serve_wins = 0

    ordered = sorted(matches, key=lambda match: (match["date"], match["id"]))
    snapshot = {
        "schema_version": 2,
        "training_year": training_year,
        "match_ids": [match["id"] for match in ordered],
        "players": {}, "training_prior": []
    }
    for day, day_matches in groupby(ordered, key=lambda match: match["date"]):
        day_matches = list(day_matches)
        year = int(day[:4])
        serve_prior = training_serve_wins / training_points if training_points else 0.5

        for match in day_matches:
            # Aggiungo la regola per la quale player_a è sempre 0 e player_b è sempre 1, indipendentemente da chi sia il server.
            for role, index in [("a", 0), ("b", 1)]:
                history = player_history.get(match["player_ids"][index], {})
                for kind, prior in [("serve", serve_prior), ("return", 1 - serve_prior)]:
                    column = f"player_{role}_{kind}_win_rate"
                    total = history.get(kind + "_points", 0)
                    if total:
                        match["features"][column] = history[kind + "_wins"] / total
                    else:
                        match["features"][column] = prior

        # Gli esiti dei punti aggiornano SOLO lo storico futuro.
        day_players = set()
        for match in day_matches:
            for point in match["points"]:
                for index, kind in [(point["server"], "serve"), (1 - point["server"], "return")]:
                    history = player_history.setdefault(match["player_ids"][index], {
                        "serve_points": 0, "serve_wins": 0, "return_points": 0, "return_wins": 0
                    })
                    history[kind + "_points"] += 1
                    history[kind + "_wins"] += int(point["winner"] == index)
                    day_players.add(match["player_ids"][index])
                if year == training_year:
                    training_points += 1
                    training_serve_wins += int(point["winner"] == point["server"])

        # Copie dei conteggi a fine giornata, pronte per la consultazione live.
        for player_id in sorted(day_players):
            snapshot["players"].setdefault(player_id, []).append({
                "date": day, **player_history[player_id]
            })
        if year == training_year:
            snapshot["training_prior"].append({
                "date": day, "points": training_points, "serve_wins": training_serve_wins
            })

    return snapshot
