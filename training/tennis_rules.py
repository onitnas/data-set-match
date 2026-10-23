# Regole e ricostruzione dei punteggi: nessuna lettura o scrittura di file.
GAME_POINTS = {"0": 0, "15": 1, "30": 2, "40": 3, "A": 4}
BEST_OF_5_TOURNAMENTS = ["1236", "2155", "2053", "1217"]
VALID_YEARS = [2023, 2024, 2025, 2026]


def require(condition, message):
    if not condition:
        raise ValueError(message)

def split_score(score, is_tiebreak=False):
    require(isinstance(score, str), "Punteggio non valido")
    scores = score.split("-")
    require(len(scores) == 2, "Punteggio non valido")
    player1_score = scores[0].strip()
    player2_score = scores[1].strip()

    if is_tiebreak:
        require(player1_score.isdecimal() and player2_score.isdecimal(), "Punteggio numerico non valido")
        return [int(player1_score), int(player2_score)]

    # Per il modello: 0/15/30/40/A diventano 0/1/2/3/4.
    if player1_score not in GAME_POINTS or player2_score not in GAME_POINTS:
        raise ValueError("Punteggio del game non valido")
    score = [GAME_POINTS[player1_score], GAME_POINTS[player2_score]]
    if 4 in score and score != [4, 3] and score != [3, 4]:
        raise ValueError("Vantaggio non valido")
    return score

def get_player(name):
    if name == "First Player":
        return 0
    elif name == "Second Player":
        return 1
    else:
        raise ValueError("Giocatore non valido")

def next_score(score, winner):
    own = score[winner]
    other = score[1 - winner]
    if own == 4 or (own == 3 and other < 3):
        return None  # Il game termina con questo punto.
    if own == 3 and other == 4:
        return [3, 3]
    result = score.copy()
    result[winner] += 1
    return result

def ordered_records(records, field):
    if not isinstance(records, list) or len(records) == 0:
        raise ValueError("Sequenza vuota")
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Record non valido")
        number = str(record.get(field, ""))
        if not number.isdecimal() or int(number) < 1:
            raise ValueError("Numero di punto o game non valido")
    ordered = sorted(records, key=lambda record: int(record[field]))
    for index, record in enumerate(ordered):
        if int(record[field]) != index + 1:
            raise ValueError("Numerazione mancante o duplicata")
    return ordered

def decode_game(game):
    server = get_player(game.get("player_served"))
    winner = get_player(game.get("serve_winner"))
    single_points = ordered_records(game.get("points"), "number_point")
    score = [0, 0]
    points = []

    for single_point in single_points:
        new_score = split_score(single_point.get("score"))
        point_winner = None
        for player in [0, 1]:
            if next_score(score, player) == new_score:
                point_winner = player
        if point_winner is None:
            raise ValueError("Passaggio di punteggio non valido")
        points.append({"score": score, "server": server,
                       "winner": point_winner, "is_inferred": False})
        score = new_score

    # Nel feed il punto che chiude il game normalmente manca.
    if next_score(score, winner) is not None:
        raise ValueError("Impossibile ricostruire il punto conclusivo")
    points.append({"score": score, "server": server,
                   "winner": winner, "is_inferred": True})
    return points

def decode_tiebreak(records, target):
    records = ordered_records(records, "number_game")
    first_server = get_player(records[0].get("player_served"))
    score = [0, 0]
    points = []
    for index, record in enumerate(records):
        if max(score) >= target and abs(score[0] - score[1]) >= 2:
            raise ValueError("Punti successivi alla fine del tie-break")
        server = get_player(record.get("player_served"))
        expected_server = first_server
        if ((index + 1) // 2) % 2 == 1:
            expected_server = 1 - first_server
        if server != expected_server:
            raise ValueError("Servizio del tie-break non valido")
        winner = get_player(record.get("serve_winner"))
        new_score = split_score(record.get("score"), True)
        expected_score = score.copy()
        expected_score[winner] += 1
        if new_score != expected_score:
            raise ValueError("Passaggio di punteggio del tie-break non valido")
        points.append({"score": score, "server": server,
                       "winner": winner, "is_inferred": False})
        score = new_score
    if max(score) < target or abs(score[0] - score[1]) < 2:
        raise ValueError("Tie-break incompleto")
    return points, score

def get_best_of(match):
    if str(match["tournament_key"]) in BEST_OF_5_TOURNAMENTS:
        return 5
    return 3

def group_sets(match, best_of):
    records = match.get("pointbypoint")
    if not isinstance(records, list) or len(records) == 0:
        raise ValueError("Punti assenti")

    grouped_sets = {}
    for record in records:
        if not isinstance(record, dict):
            raise ValueError("Record del match non valido")
        parts = str(record.get("set_number", "")).split()
        if len(parts) not in [2, 3] or parts[0] != "Set" or not parts[1].isdecimal():
            raise ValueError("Numero set non valido")
        set_number = int(parts[1])
        if set_number < 1 or set_number > best_of:
            raise ValueError("Set fuori dal formato previsto")
        if set_number not in grouped_sets:
            grouped_sets[set_number] = {"number": set_number, "games": [], "tiebreak": []}

        if len(parts) == 2:
            grouped_sets[set_number]["games"].append(record)
        elif parts[2] == "TieBreak":
            grouped_sets[set_number]["tiebreak"].append(record)
        else:
            raise ValueError("Tipo set non valido")

    match_sets = []
    for set_number in sorted(grouped_sets):
        if set_number != len(match_sets) + 1:
            raise ValueError("Sequenza set non valida")
        match_sets.append(grouped_sets[set_number])
    return match_sets

def decode_set(set_data, sets, next_server, best_of):
    set_number = set_data["number"]
    game_records = ordered_records(set_data["games"], "number_game")
    games = [0, 0]
    points = []
    set_winner = None
    tiebreak_score = None

    for game in game_records:
        if set_winner is not None:
            raise ValueError("Game successivo alla fine del set")
        target = 0
        if games == [6, 6]:
            placeholder = game.get("points")
            if not isinstance(placeholder, list) or len(placeholder) != 1:
                raise ValueError("Riepilogo tie-break non valido")
            point = placeholder[0]
            if not isinstance(point, dict) or point.get("score") != " - ":
                raise ValueError("Riepilogo tie-break non valido")
            if str(point.get("number_point")) != "1":
                raise ValueError("Numero punto del riepilogo tie-break non valido")
            target = 7
            if best_of == 5 and set_number == 5:
                target = 10
            game_points, tiebreak_score = decode_tiebreak(set_data["tiebreak"], target)
        else:
            game_points = decode_game(game)

        first_server = game_points[0]["server"]
        if next_server is not None and first_server != next_server:
            raise ValueError("Alternanza del servizio non valida")
        winner = game_points[-1]["winner"]
        if get_player(game.get("serve_winner")) != winner:
            raise ValueError("Vincitore del game non coerente")

        # Copiamo lo stato precedente: i conteggi cambiano dopo il game.
        for point in game_points:
            point["games"] = games.copy()
            point["sets"] = sets.copy()
            point["best_of"] = best_of
            point["tiebreak_target"] = target
            points.append(point)
        games[winner] += 1
        if split_score(game.get("score"), True) != games:
            raise ValueError("Punteggio dei game non coerente")
        next_server = 1 - first_server
        if target > 0 or (max(games) >= 6 and abs(games[0] - games[1]) >= 2):
            set_winner = winner

    if len(set_data["tiebreak"]) > 0 and tiebreak_score is None:
        raise ValueError("Tie-break senza un game a 6-6")
    if set_winner is None:
        raise ValueError("Set incompleto")
    final_score = [str(games[0]), str(games[1])]
    if tiebreak_score is not None:
        for player in [0, 1]:
            final_score[player] += "." + str(tiebreak_score[player])
    return set_winner, next_server, final_score, points


def decode_match(match):
    best_of = get_best_of(match)
    sets_to_win = best_of // 2 + 1
    match_sets = group_sets(match, best_of)
    sets = [0, 0]
    points = []
    final_scores = {}
    next_server = None

    for set_data in match_sets:
        if max(sets) >= sets_to_win:
            raise ValueError("Set successivo alla fine del match")
        winner, next_server, final_score, set_points = decode_set(set_data, sets, next_server, best_of)
        points.extend(set_points)
        sets[winner] += 1
        final_scores[set_data["number"]] = final_score

    if max(sets) != sets_to_win:
        raise ValueError("Match incompleto")
    check_final_scores(match, final_scores)
    if split_score(match.get("event_final_result"), True) != sets:
        raise ValueError("Risultato finale non coerente")
    winner = sets.index(max(sets))
    if get_player(match.get("event_winner")) != winner:
        raise ValueError("Vincitore del match non coerente")
    for index, point in enumerate(points, 1):
        point["point_index"] = index
    return points


def check_final_scores(match, final_scores):
    scores = match.get("scores")
    if not isinstance(scores, list):
        raise ValueError("Punteggi finali assenti")
    seen = set()
    for score in scores:
        number = int(score["score_set"])
        if number < 1 or number in seen:
            raise ValueError("Punteggi set duplicati o non validi")
        seen.add(number)
        result = [str(score["score_first"]), str(score["score_second"])]
        if number in final_scores:
            if final_scores[number] != result:
                raise ValueError("Punteggio finale del set non coerente")
        elif number <= len(final_scores) or result != ["0", "0"]:
            raise ValueError("Set aggiuntivo non valido")
    for number in final_scores:
        if number not in seen:
            raise ValueError("Punteggio finale del set mancante")
