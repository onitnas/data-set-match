"""Regole del tennis e lettura del point-by-point dell'API."""

GAME_POINTS = {"0": 0, "15": 1, "30": 2, "40": 3, "A": 4}
BEST_OF_5_TOURNAMENTS = ["1236", "2155", "2053", "1217"]
VALID_YEARS = [2023, 2024, 2025, 2026]
PLAYER_ORIENTATION = "player_a_index = 0; player_b_index = 1; fixed_for_entire_match"

def require(condition, message):
    if not condition:
        raise ValueError(message)


def get_player(name):
    if name == "First Player":
        return 0
    if name == "Second Player":
        return 1
    raise ValueError("Giocatore non valido")


def get_best_of(match):
    return 5 if str(match["tournament_key"]) in BEST_OF_5_TOURNAMENTS else 3


def get_tiebreak_target(games, sets, best_of):
    if games != [6, 6]:
        return 0
    set_number = sum(sets) + 1
    return 10 if best_of == 5 and set_number == 5 else 7


def split_score(value, is_tiebreak=False):
    require(isinstance(value, str), "Punteggio non valido")
    parts = [part.strip() for part in value.split("-")]
    require(len(parts) == 2, "Punteggio non valido")

    if is_tiebreak:
        require(all(part.isdecimal() for part in parts), "Punteggio numerico non valido")
        return [int(parts[0]), int(parts[1])]

    require(all(part in GAME_POINTS for part in parts), "Punteggio del game non valido")
    score = [GAME_POINTS[parts[0]], GAME_POINTS[parts[1]]]
    require(4 not in score or score in [[4, 3], [3, 4]], "Vantaggio non valido")
    return score

def ordered_records(records, field):
    require(isinstance(records, list) and bool(records), "Sequenza vuota")

    for record in records:
        require(isinstance(record, dict), "Record non valido")

        number = str(record.get(field, ""))

        require(number.isdecimal() and int(number) > 0, "Numero di punto o game non valido")

    records = sorted(records, key=lambda record: int(record[field]))

    for expected, record in enumerate(records, 1):
        require(int(record[field]) == expected, "Numerazione mancante o duplicata")
    return records


def next_state(score, games, sets, winner, best_of):
    new_score = score.copy()
    new_games = games.copy()
    new_sets = sets.copy()
    game_result = None
    tiebreak_result = None

    # Tie-break sul 6-6.
    if games == [6, 6]:
        target = get_tiebreak_target(games, sets, best_of)
        new_score[winner] += 1
        if new_score[winner] >= target and new_score[winner] - new_score[1 - winner] >= 2:
            new_games[winner] += 1
            game_result = new_games.copy()
            tiebreak_result = new_score.copy()
            new_sets[winner] += 1
            new_games = [0, 0]
            new_score = [0, 0]

    # Game normale: 0, 15, 30, 40, vantaggio sono codificati come 0, 1, 2, 3, 4.
    else:
        own = score[winner]
        other = score[1 - winner]
        game_finished = False

        if own == 4 or (own == 3 and other < 3):
            game_finished = True
        elif own == 3 and other == 4:
            new_score = [3, 3]
        else:
            new_score[winner] += 1

        if game_finished:
            new_games[winner] += 1
            game_result = new_games.copy()
            new_score = [0, 0]
            if new_games[winner] >= 6 and new_games[winner] - new_games[1 - winner] >= 2:
                new_sets[winner] += 1
                new_games = [0, 0]

    return {
        "score": new_score,
        "games": new_games,
        "sets": new_sets,
        "game_result": game_result,
        "tiebreak_result": tiebreak_result
    }


def add_point(points, state, winner, server, best_of, is_inferred=False):
    points.append({
        "score": state["score"].copy(),
        "games": state["games"].copy(),
        "sets": state["sets"].copy(),
        "server": server,
        "winner": winner,
        "best_of": best_of,
        "tiebreak_target": get_tiebreak_target(state["games"], state["sets"], best_of),
        "is_inferred": is_inferred,
        "point_index": len(points) + 1
    })


def decode_game(game, state, best_of, points):
    server = get_player(game.get("player_served"))
    for record in ordered_records(game.get("points"), "number_point"):
        before = state["score"]
        observed = split_score(record.get("score"))

        if observed[0] > before[0]:
            winner = 0
        elif observed[1] > before[1]:
            winner = 1
        elif before == [4, 3] and observed == [3, 3]:
            winner = 1  # Il secondo giocatore annulla il vantaggio del primo.
        elif before == [3, 4] and observed == [3, 3]:
            winner = 0
        else:
            raise ValueError("Passaggio di punteggio non valido")

        add_point(points, state, winner, server, best_of)
        # Il record riporta il punteggio dopo il punto; salviamo prima lo stato precedente.
        state = {**state, "score": observed}

    # Il feed omette il punto conclusivo: lo aggiungiamo solo se chiude il game.
    winner = get_player(game.get("serve_winner"))
    updated = next_state(state["score"], state["games"], state["sets"], winner, best_of)
    require(updated["game_result"] is not None, "Impossibile ricostruire il punto conclusivo")
    add_point(points, state, winner, server, best_of, is_inferred=True)
    return updated


def decode_tiebreak(records, state, best_of, points):
    records = ordered_records(records, "number_game")
    first_server = get_player(records[0].get("player_served"))
    starting_sets = state["sets"].copy()
    target = get_tiebreak_target(state["games"], state["sets"], best_of)

    for index, record in enumerate(records):
        require(state["sets"] == starting_sets, "Punti successivi alla fine del tie-break")
        server = get_player(record.get("player_served"))
        expected_server = first_server if ((index + 1) // 2) % 2 == 0 else 1 - first_server
        require(server == expected_server, "Servizio del tie-break non valido")

        winner = get_player(record.get("serve_winner"))
        observed = split_score(record.get("score"), True)
        add_point(points, state, winner, server, best_of)
        if max(observed) >= target and abs(observed[0] - observed[1]) >= 2:
            state = next_state(state["score"], state["games"], state["sets"], winner, best_of)
            require(state["game_result"] is not None, "Tie-break incompleto")
        else:
            state = {**state, "score": observed}

    require(state["sets"] != starting_sets, "Tie-break incompleto")
    return state, first_server


def group_sets(match, best_of):
    records = match.get("pointbypoint")
    require(isinstance(records, list) and bool(records), "Punti assenti")
    grouped = {}
    for record in records:
        require(isinstance(record, dict), "Record del match non valido")
        parts = str(record.get("set_number", "")).split()
        require(len(parts) in [2, 3] and parts[0] == "Set" and parts[1].isdecimal(),
                "Numero set non valido")
        number = int(parts[1])
        require(1 <= number <= best_of, "Set fuori dal formato previsto")
        if number not in grouped:
            grouped[number] = {"number": number, "games": [], "tiebreak": []}
        if len(parts) == 2:
            grouped[number]["games"].append(record)
        else:
            require(parts[2] == "TieBreak", "Tipo set non valido")
            grouped[number]["tiebreak"].append(record)

    require(sorted(grouped) == list(range(1, len(grouped) + 1)), "Sequenza set non valida")
    return [grouped[number] for number in sorted(grouped)]


def check_final_scores(match, final_scores):
    scores = match.get("scores")
    require(isinstance(scores, list), "Punteggi finali assenti")
    seen = set()
    for record in scores:
        number = int(record["score_set"])
        require(number > 0 and number not in seen, "Punteggi set duplicati o non validi")
        seen.add(number)
        observed = [str(record["score_first"]), str(record["score_second"])]
        if number in final_scores:
            require(observed == final_scores[number], "Punteggio finale del set non coerente")
        else:
            require(number > len(final_scores) and observed == ["0", "0"], "Set aggiuntivo non valido")
    require(all(number in seen for number in final_scores), "Punteggio finale del set mancante")


def decode_match(match):
    """Legge set e game, restituendo gli stati precedenti a ogni punto."""
    best_of = get_best_of(match)
    sets_to_win = best_of // 2 + 1
    state = {"score": [0, 0], "games": [0, 0], "sets": [0, 0]}
    points = []
    final_scores = {}
    next_server = None

    for set_data in group_sets(match, best_of):
        require(max(state["sets"]) < sets_to_win, "Set successivo alla fine del match")
        starting_sets = state["sets"].copy()
        tiebreak_played = False

        for game in ordered_records(set_data["games"], "number_game"):
            require(state["sets"] == starting_sets, "Game successivo alla fine del set")
            if state["games"] == [6, 6]:
                # Il tredicesimo game è un riepilogo; i punti sono nei record TieBreak.
                placeholder = game.get("points")
                require(isinstance(placeholder, list) and len(placeholder) == 1,
                        "Riepilogo tie-break non valido")
                require(isinstance(placeholder[0], dict)
                        and placeholder[0].get("score") == " - "
                        and str(placeholder[0].get("number_point")) == "1",
                        "Riepilogo tie-break non valido")
                state, server = decode_tiebreak(set_data["tiebreak"], state, best_of, points)
                tiebreak_played = True
            else:
                server = get_player(game.get("player_served"))
                state = decode_game(game, state, best_of, points)

            require(next_server is None or server == next_server, "Alternanza del servizio non valida")
            require(get_player(game.get("serve_winner")) == points[-1]["winner"],
                    "Vincitore del game non coerente")
            require(split_score(game.get("score"), True) == state["game_result"],
                    "Punteggio dei game non coerente")
            next_server = 1 - server

        require(not set_data["tiebreak"] or tiebreak_played, "Tie-break senza un game a 6-6")
        require(state["sets"] != starting_sets, "Set incompleto")
        result = [str(value) for value in state["game_result"]]
        if tiebreak_played:
            result = [f"{game}.{point}" for game, point in zip(result, state["tiebreak_result"])]
        final_scores[set_data["number"]] = result

    require(max(state["sets"]) == sets_to_win, "Match incompleto")
    check_final_scores(match, final_scores)
    require(split_score(match.get("event_final_result"), True) == state["sets"],
            "Risultato finale non coerente")
    require(get_player(match.get("event_winner")) == state["sets"].index(max(state["sets"])),
            "Vincitore del match non coerente")
    return points
