from itertools import groupby


FEATURE_COLUMNS = [
    "player_a_serve_win_rate", "player_a_return_win_rate",
    "player_b_serve_win_rate", "player_b_return_win_rate"
]


def add_historical_features(matches, training_year=2024):
    player_history = {}
    training_points = 0
    training_serve_wins = 0
    fallbacks = {}

    ordered = sorted(matches, key=lambda match: (match["date"], match["id"]))
    for day, day_matches in groupby(ordered, key=lambda match: match["date"]):
        day_matches = list(day_matches)
        year = int(day[:4])
        counts = fallbacks.setdefault(str(year), {column: 0 for column in FEATURE_COLUMNS})
        serve_prior = training_serve_wins / training_points if training_points else 0.5

        # Tutti gli incontri della giornata ricevono lo storico dei giorni precedenti.
        for match in day_matches:
            for role, index in [("a", match["player_a_index"]), ("b", 1 - match["player_a_index"])]:
                history = player_history.get(match["player_ids"][index], {})
                for kind, prior in [("serve", serve_prior), ("return", 1 - serve_prior)]:
                    column = f"player_{role}_{kind}_win_rate"
                    total = history.get(kind + "_points", 0)
                    if total:
                        match["features"][column] = history[kind + "_wins"] / total
                    else:
                        match["features"][column] = prior
                        counts[column] += 1

        # Gli esiti dei punti aggiornano SOLO lo storico futuro.
        # Il target del nuovo modello resta il vincitore del match.
        for match in day_matches:
            for point in match["points"]:
                for index, kind in [(point["server"], "serve"), (1 - point["server"], "return")]:
                    history = player_history.setdefault(match["player_ids"][index], {
                        "serve_points": 0, "serve_wins": 0, "return_points": 0, "return_wins": 0
                    })
                    history[kind + "_points"] += 1
                    history[kind + "_wins"] += int(point["winner"] == index)
                if year == training_year:
                    training_points += 1
                    training_serve_wins += int(point["winner"] == point["server"])

    serve_prior = training_serve_wins / training_points if training_points else 0.5
    return {
        "policy": {
            "history_cutoff": "previous_calendar_days_only",
            "source": "accepted_matches_including_inferred_points",
            "training_year": training_year,
            "fallback": "expanding_training_prior_then_frozen_after_training_year",
            "initial_serve_win_rate": 0.5,
            "initial_return_win_rate": 0.5,
            "player_history_updates": "after_each_day_including_validation_and_test"
        },
        "training_prior": {
            "points": training_points,
            "serve_win_rate": serve_prior,
            "return_win_rate": 1 - serve_prior
        },
        "players_with_history": len(player_history),
        "fallback_matches_by_year": fallbacks
    }
