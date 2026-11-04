"""Le stesse 21 feature del training, senza label o sample_weight."""

from datetime import date

from streaming.player_history import canonical_id, historical_rates, qualification_status
from training import tennis_rules as rules


FEATURE_COLUMNS = [
    "best_of", "surface", "player_a_serve_win_rate", "player_a_return_win_rate",
    "player_b_serve_win_rate", "player_b_return_win_rate", "player_a_rank", "player_b_rank",
    "player_a_rank_missing", "player_b_rank_missing", "sets_a", "sets_b", "games_a", "games_b",
    "regular_score_a", "regular_score_b", "is_tiebreak", "tiebreak_target",
    "tiebreak_score_a", "tiebreak_score_b", "player_a_is_serving"
]


def api_score(match, player_a):
    """Punteggio ricevuto, visibile anche quando il dettaglio non è utilizzabile."""
    sets = str(match.get("event_final_result", "")).split("-")
    points = str(match.get("event_game_result", "")).split("-")
    current_set = str(match.get("event_status", "")).removeprefix("Set ")
    scores = match.get("scores") or []
    current = next((s for s in scores if isinstance(s, dict) and str(s.get("score_set")) == current_set), None)
    games = [str(current.get("score_first")), str(current.get("score_second"))] if current else []
    parts = []
    for name, values in [("set", sets), ("game", games), ("punti", points)]:
        if len(values) == 2:
            parts.append(f"{name} {values[player_a].strip()}-{values[1 - player_a].strip()}")
    return "; ".join(parts)


class FeatureBuilder:
    def __init__(self, tournaments, rankings, timelines, training_prior):
        self.tournaments = tournaments
        self.rankings = rankings
        self.timelines = timelines
        self.training_prior = training_prior

    def build(self, match_id, match, state, parse_ok=True):
        result = {"features_ok": False, "skip_reason": None, "prediction_status": "invalid"}
        try:
            rules.require(parse_ok and isinstance(match, dict), "JSON non valido o ID del match assente")
            match_id = canonical_id(match_id)
            rules.require(match_id == canonical_id(match.get("event_key")), "Chiave Kafka diversa dall'ID del match")
            result["canonical_match_id"] = match_id
            player_ids = [canonical_id(match.get(key)) for key in ["first_player_key", "second_player_key"]]
            rules.require(player_ids[0] != player_ids[1], "Identificativi giocatori uguali")
            player_a, player_b = 0, 1
            for role, index in [("a", player_a), ("b", player_b)]:
                result[f"player_{role}_id"] = player_ids[index]
                result[f"player_{role}"] = match.get("event_first_player" if index == 0 else "event_second_player")
            result["api_score"] = api_score(match, player_a)
            result["qualification_status"] = qualification_status(match)
            result["prediction_status"] = "excluded"
            rules.require(str(match.get("event_type_type")).casefold() == "atp singles", "Tipo incontro diverso da ATP Singles")
            rules.require(canonical_id(match.get("tournament_key")) in self.tournaments, "Torneo non configurato")
            rules.require(result["qualification_status"] != "qualification", "Qualificazione esplicitamente indicata dal provider")
            result["prediction_status"] = "finished" if match.get("event_status") == "Finished" else "invalid_state"
            rules.require(match.get("event_status") != "Finished", "Match concluso: nessuna nuova previsione")
            rules.require(isinstance(state, dict) and state.get("state_ok"),
                          (state or {}).get("state_reason") or "Stato del match non valido")
            rules.require(not state.get("is_finished"), "Match concluso: nessuna nuova previsione")
            match_date = date.fromisoformat(match["event_date"])
            rules.require(match_date.year in rules.VALID_YEARS, "Annata del match non supportata")
            tournament = self.tournaments[canonical_id(match["tournament_key"])]
            result.update({
                "match_date": match_date.isoformat(),
                "best_of": state["best_of"], "surface": tournament["tournament_surface"],
                "is_tiebreak": state["is_tiebreak"], "tiebreak_target": state["tiebreak_target"],
                "player_a_is_serving": int(state["server_index"] == player_a)
            })
            for role, index in [("a", player_a), ("b", player_b)]:
                serve_rate, return_rate = historical_rates(
                    self.timelines, self.training_prior, player_ids[index], match_date.isoformat()
                )
                rank = self.rankings.get((player_ids[index], match_date.year - 1), 0)
                result.update({
                    f"player_{role}_serve_win_rate": serve_rate,
                    f"player_{role}_return_win_rate": return_rate,
                    f"player_{role}_rank": rank, f"player_{role}_rank_missing": int(rank == 0)
                })
                for name in ["sets", "games", "regular_score", "tiebreak_score"]:
                    result[f"{name}_{role}"] = state[f"{name}_{index + 1}"]
            result["features_ok"] = True
            result["prediction_status"] = "predicted"
        except (ValueError, KeyError, TypeError, IndexError) as error:
            result["skip_reason"] = str(error)
        return result
