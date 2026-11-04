"""Valida il punteggio dello snapshot API senza ricostruire sequenze di punti."""

import re

from streaming.player_history import canonical_id
from training import tennis_rules as rules


def decode_live_state(match):
    try:
        rules.require(isinstance(match, dict), "Match non valido")
        best_of = rules.get_best_of({"tournament_key": canonical_id(match.get("tournament_key"))})
        target_sets = best_of // 2 + 1
        finished = match.get("event_status") == "Finished"
        status = re.fullmatch(r"Set\s+([1-5])", str(match.get("event_status")))
        rules.require(finished or status is not None, "Stato non supportato: atteso Set N o Finished")
        sets = rules.split_score(match.get("event_final_result"), True)
        if finished:
            rules.require(max(sets) == target_sets and min(sets) < target_sets, "Risultato finale non valido")
            current_set = sum(sets)
        else:
            current_set = int(status[1])
            rules.require(sum(sets) == current_set - 1 and max(sets) < target_sets,
                          "Conteggio dei set non valido")
        rules.require(1 <= current_set <= best_of, "Set fuori dal formato previsto")

        scores = match.get("scores")
        rules.require(isinstance(scores, list), "Punteggi dei set assenti")
        games_by_set = {}
        for entry in scores:
            rules.require(isinstance(entry, dict), "Punteggio del set non valido")
            number = int(entry["score_set"])
            rules.require(1 <= number <= best_of and number not in games_by_set, "Numero set non valido o duplicato")
            values = [str(entry[field]) for field in ["score_first", "score_second"]]
            rules.require(all(re.fullmatch(r"\d+(?:\.\d+)?", value) for value in values), "Game non numerici")
            games_by_set[number] = [int(value.split(".")[0]) for value in values]
            if number > current_set:
                rules.require(games_by_set[number] == [0, 0], "Set futuro già iniziato")

        completed = current_set if finished else current_set - 1
        wins = [0, 0]
        for number in range(1, completed + 1):
            rules.require(number in games_by_set, "Punteggio di un set concluso assente")
            games = games_by_set[number]
            rules.require((max(games) == 6 and min(games) <= 4) or
                          (max(games) == 7 and min(games) in [5, 6]), "Punteggio del set concluso non valido")
            wins[games.index(max(games))] += 1
            rules.require(max(wins) < target_sets or (finished and number == current_set),
                          "Set successivo alla conclusione del match")
        rules.require(wins == sets, "Risultati dei set non coerenti con il riepilogo")

        games = games_by_set.get(current_set, [0, 0])
        score, server, tiebreak_target = [0, 0], None, 0
        if finished:
            if match.get("event_winner") not in [None, ""]:
                rules.require(rules.get_player(match["event_winner"]) == sets.index(max(sets)),
                              "Vincitore non coerente con i set")
        else:
            rules.require(max(games) <= 6 and not (max(games) >= 6 and abs(games[0] - games[1]) >= 2),
                          "Set terminato o game non validi")
            tiebreak_target = rules.get_tiebreak_target(games, sets, best_of)
            score = rules.split_score(match.get("event_game_result"), tiebreak_target > 0)
            rules.require(current_set in games_by_set or score == [0, 0], "Punteggio del set corrente assente")
            if tiebreak_target:
                rules.require(not (max(score) >= tiebreak_target and abs(score[0] - score[1]) >= 2),
                              "Tie-break terminato ma riepilogo non aggiornato")
            server_name = match.get("event_serve")
            if server_name in [None, ""] and not tiebreak_target:
                current_games = [game for game in (match.get("pointbypoint") or [])
                                 if isinstance(game, dict) and game.get("set_number") == f"Set {current_set}"
                                 and str(game.get("number_game")) == str(sum(games) + 1)
                                 and game.get("serve_winner") in [None, ""]]
                if len(current_games) == 1:
                    server_name = current_games[0].get("player_served")
            rules.require(server_name in ["First Player", "Second Player"],
                          "Battitore assente o non valido nello snapshot")
            server = rules.get_player(server_name)

        state = {"state_ok": True, "state_reason": None, "state_source": "api_score",
                 "is_finished": finished, "best_of": best_of, "server_index": server,
                 "is_tiebreak": int(tiebreak_target > 0), "tiebreak_target": tiebreak_target}
        for index in [0, 1]:
            state.update({f"sets_{index + 1}": sets[index], f"games_{index + 1}": games[index],
                          f"regular_score_{index + 1}": score[index] if not tiebreak_target else 0,
                          f"tiebreak_score_{index + 1}": score[index] if tiebreak_target else 0})
        return state
    except (ValueError, KeyError, TypeError, IndexError) as error:
        return {"state_ok": False, "state_reason": str(error), "state_source": "api_score", "is_finished": None}
