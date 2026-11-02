"""Ricostruzione di uno snapshot live, senza accumulare dati fra messaggi."""

import re

from training import tennis_rules as rules


class MatchState:
    def __init__(self, best_of):
        self.best_of = best_of
        self.sets = [0, 0]
        self.games = [0, 0]
        self.score = [0, 0]
        self.server = None
        self.momentum = [0, 0]
        self.points_seen = 0
        self.inferred_points = 0
        self.tiebreak_target = 0

    def add_points(self, points):
        for point in points:
            winner = point["winner"]
            self.momentum[winner] += 1
            self.momentum[1 - winner] = 0
            self.points_seen += 1
            self.inferred_points += int(point["is_inferred"])

    def as_dict(self, finished=False):
        is_tiebreak = self.tiebreak_target > 0
        result = {
            "state_ok": True, "state_reason": None, "is_finished": finished,
            "best_of": self.best_of, "server_index": self.server,
            "points_seen": self.points_seen, "inferred_points": self.inferred_points,
            "is_tiebreak": int(is_tiebreak), "tiebreak_target": self.tiebreak_target
        }
        for index in [0, 1]:
            role = index + 1
            result[f"sets_{role}"] = self.sets[index]
            result[f"games_{role}"] = self.games[index]
            result[f"regular_score_{role}"] = 0 if is_tiebreak else self.score[index]
            result[f"tiebreak_score_{role}"] = self.score[index] if is_tiebreak else 0
            result[f"momentum_{role}"] = self.momentum[index]
        return result


def decode_open_game(game):
    """Legge solo punti osservati; non inventa la chiusura di un game aperto."""
    records = game.get("points")
    rules.require(isinstance(records, list), "Punti del game assenti")
    records = rules.ordered_records(records, "number_point") if records else []
    server = rules.get_player(game.get("player_served"))
    score = [0, 0]
    points = []
    for record in records:
        new_score = rules.split_score(record.get("score"))
        winners = [player for player in [0, 1] if rules.next_score(score, player) == new_score]
        rules.require(len(winners) == 1, "Passaggio di punteggio del game non valido")
        points.append({"winner": winners[0], "is_inferred": False})
        score = new_score
    return points, score, server


def decode_open_tiebreak(records, first_server, target):
    records = rules.ordered_records(records, "number_game") if records else []
    score = [0, 0]
    points = []
    for index, record in enumerate(records):
        rules.require(not (max(score) >= target and abs(score[0] - score[1]) >= 2),
                      "Punti successivi alla fine del tie-break")
        server = first_server if ((index + 1) // 2) % 2 == 0 else 1 - first_server
        rules.require(rules.get_player(record.get("player_served")) == server,
                      "Servizio del tie-break non valido")
        winner = rules.get_player(record.get("serve_winner"))
        expected_score = score.copy()
        expected_score[winner] += 1
        rules.require(rules.split_score(record.get("score"), True) == expected_score,
                      "Passaggio di punteggio del tie-break non valido")
        points.append({"winner": winner, "is_inferred": False})
        score = expected_score
    rules.require(not (max(score) >= target and abs(score[0] - score[1]) >= 2),
                  "Tie-break terminato ma stato del set non ancora aggiornato")
    next_server = first_server if ((len(records) + 1) // 2) % 2 == 0 else 1 - first_server
    return points, score, next_server


def decode_current_set(set_data, state):
    games = set_data["games"]
    games = rules.ordered_records(games, "number_game") if games else []
    placeholder = None
    for index, game in enumerate(games):
        if state.games == [6, 6]:
            rules.require(index == len(games) - 1, "Game successivo al tie-break")
            placeholder = game
            break

        server = rules.get_player(game.get("player_served"))
        rules.require(state.server is None or state.server == server,
                      "Alternanza del servizio non valida")
        state.server = server
        winner_name = game.get("serve_winner")
        if winner_name in [None, ""]:
            rules.require(index == len(games) - 1 and not set_data["tiebreak"],
                          "Game incompleto seguito da altri punti")
            points, state.score, state.server = decode_open_game(game)
            state.add_points(points)
            return

        try:
            points = rules.decode_game(game)
        except (ValueError, KeyError, TypeError, IndexError) as error:
            raise ValueError(f"Game {game['number_game']}: {error}") from error
        state.add_points(points)
        winner = rules.get_player(winner_name)
        state.games[winner] += 1
        rules.require(rules.split_score(game.get("score"), True) == state.games,
                      "Punteggio dei game non coerente")
        state.server = 1 - server
        rules.require(not (max(state.games) >= 6 and abs(state.games[0] - state.games[1]) >= 2),
                      "Set terminato ma stato dell'incontro non ancora aggiornato")

    if state.games != [6, 6]:
        rules.require(not set_data["tiebreak"], "Tie-break senza un punteggio di 6-6")
        return

    if placeholder is not None:
        rules.require(rules.get_player(placeholder.get("player_served")) == state.server,
                      "Primo servizio del tie-break non coerente")
        placeholder_points = placeholder.get("points")
        rules.require(placeholder_points == [] or (
            isinstance(placeholder_points, list) and len(placeholder_points) == 1
            and isinstance(placeholder_points[0], dict)
            and placeholder_points[0].get("score") == " - "
            and str(placeholder_points[0].get("number_point")) == "1"
        ), "Riepilogo tie-break non valido")
        rules.require(placeholder.get("serve_winner") in [None, ""],
                      "Tie-break terminato ma stato del set non ancora aggiornato")

    state.tiebreak_target = 10 if state.best_of == 5 and set_data["number"] == 5 else 7
    points, state.score, state.server = decode_open_tiebreak(
        set_data["tiebreak"], state.server, state.tiebreak_target
    )
    state.add_points(points)


def check_live_score(match, state, current_set, final_scores):
    """Il momentum è utilizzabile solo se i punti raggiungono lo stato live."""
    rules.require(rules.split_score(match.get("event_final_result"), True) == state.sets,
                  "Set live non allineati a pointbypoint")
    scores = match.get("scores")
    rules.require(isinstance(scores, list), "Punteggi dei set assenti")
    seen = set()
    for score in scores:
        number = int(score["score_set"])
        rules.require(1 <= number <= state.best_of and number not in seen,
                      "Punteggi set duplicati o non validi")
        seen.add(number)
        result = [str(score["score_first"]), str(score["score_second"])]
        if number < current_set:
            rules.require(result == final_scores.get(number), "Punteggio del set concluso non coerente")
        elif number == current_set:
            rules.require(result == list(map(str, state.games)), "Game live non allineati a pointbypoint")
        else:
            rules.require(result == ["0", "0"], "Punteggio di un set futuro non valido")
    rules.require(all(number in seen for number in range(1, current_set)), "Set concluso assente da scores")
    # All'inizio di un set il provider può non avere ancora una voce in scores.
    rules.require(current_set in seen or state.games == [0, 0], "Punteggio del set corrente assente")
    live_score = rules.split_score(match.get("event_game_result"), state.tiebreak_target > 0)
    rules.require(live_score == state.score, "Punti live non allineati a pointbypoint")
    server = rules.get_player(match.get("event_serve"))
    rules.require(state.server is None or server == state.server, "Battitore live non coerente con pointbypoint")
    state.server = server


def reconstruct_match(match):
    rules.require(isinstance(match, dict), "Match assente o non valido")
    best_of = rules.get_best_of(match)
    rules.require(not (best_of == 5 and str(match.get("event_qualification")).lower() == "true"),
                  "Formato delle qualificazioni Slam non supportato")
    state = MatchState(best_of)
    status = match.get("event_status")

    if status == "Finished":
        points = rules.decode_match(match)
        state.add_points(points)
        state.sets = rules.split_score(match.get("event_final_result"), True)
        last_set = sum(state.sets)
        final_score = next(score for score in match["scores"] if int(score["score_set"]) == last_set)
        state.games = [int(str(final_score[field]).split(".")[0]) for field in ["score_first", "score_second"]]
        return state.as_dict(finished=True)

    status_match = re.fullmatch(r"Set\s+([1-5])", str(status))
    rules.require(status_match is not None, "Stato non supportato: atteso Set N o Finished")
    current_set = int(status_match[1])
    rules.require(current_set <= best_of, "Set fuori dal formato previsto")
    records = match.get("pointbypoint")
    rules.require(isinstance(records, list), "Pointbypoint assente")
    match_sets = rules.group_sets(match, best_of) if records else []
    rules.require(len(match_sets) <= current_set, "Pointbypoint contiene set successivi allo stato live")
    final_scores = {}
    for set_data in match_sets:
        if set_data["number"] == current_set:
            decode_current_set(set_data, state)
            break
        winner, state.server, final_score, points = rules.decode_set(
            set_data, state.sets, state.server, best_of
        )
        state.add_points(points)
        state.sets[winner] += 1
        final_scores[set_data["number"]] = final_score
        rules.require(max(state.sets) < best_of // 2 + 1, "Match già terminato")

    rules.require(sum(state.sets) == current_set - 1, "Set precedenti mancanti in pointbypoint")
    check_live_score(match, state, current_set, final_scores)
    return state.as_dict()


def decode_live_state(match):
    """Adattatore per la UDF: un record non valido rimane visibile in console."""
    try:
        if hasattr(match, "asDict"):
            match = match.asDict(recursive=True)
        return reconstruct_match(match)
    except (ValueError, KeyError, TypeError, IndexError) as error:
        # Tutti i conteggi restano null: uno storico incompleto non equivale a zero.
        return {"state_ok": False, "state_reason": str(error), "is_finished": None}
