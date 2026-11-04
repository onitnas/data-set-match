"""Statistiche dai match accettati, consultabili solo per giorni precedenti."""

import json
from bisect import bisect_left
from collections import defaultdict
from datetime import date
from decimal import Decimal, InvalidOperation
from pathlib import Path

from training import tennis_rules as rules


COUNT_FIELDS = ["serve_points", "serve_wins", "return_points", "return_wins"]


def canonical_id(value):
    try:
        number = Decimal(str(value))
        if not number.is_finite() or number <= 0 or number != number.to_integral_value():
            raise ValueError("Identificativo non intero o non positivo")
        return str(int(number))
    except InvalidOperation as error:
        raise ValueError("Identificativo non valido") from error


def qualification_status(match):
    value = str(match.get("event_qualification")).lower()
    return {"true": "qualification", "false": "main_draw"}.get(value, "unknown")


def is_supported_live_match(match, tournaments):
    return (
        qualification_status(match) != "qualification"
        and str(match.get("event_type_type")).casefold() == "atp singles"
        and canonical_id(match.get("tournament_key")) in tournaments
    )


def summarize_match(match):
    day = date.fromisoformat(match["event_date"]).isoformat()
    player_ids = [canonical_id(match[key]) for key in ["first_player_key", "second_player_key"]]
    rules.require(player_ids[0] != player_ids[1], "Identificativi giocatori uguali")
    points = rules.decode_match(match)
    counts = {player_id: [0, 0, 0, 0] for player_id in player_ids}
    for point in points:
        server = point["server"]
        server_won = int(point["winner"] == server)
        counts[player_ids[server]][0] += 1
        counts[player_ids[server]][1] += server_won
        counts[player_ids[1 - server]][2] += 1
        counts[player_ids[1 - server]][3] += 1 - server_won
    return {"match_id": canonical_id(match["event_key"]), "date": day, "players": counts,
            "qualification_status": qualification_status(match)}


def cumulative_days(days):
    dates = sorted(days)
    totals = []
    running = [0] * len(next(iter(days.values()))) if days else []
    for day in dates:
        running = [a + b for a, b in zip(running, days[day])]
        totals.append(running)
    return dates, totals


def counts_before(timeline, day):
    dates, totals = timeline
    index = bisect_left(dates, day) - 1
    return totals[index] if index >= 0 else None


def merge_timeline(base, additional_days):
    """Somma i nuovi match ai totali offline, senza rileggere i punti passati."""
    if not additional_days:
        return base
    days = {day: counts.copy() for day, counts in additional_days.items()}
    dates, totals = base
    previous = [0] * len(totals[0]) if totals else []
    for day, total in zip(dates, totals):
        delta = [value - old for value, old in zip(total, previous)]
        days[day] = [a + b for a, b in zip(days.get(day, [0] * len(delta)), delta)]
        previous = total
    return cumulative_days(days)


def historical_rates(timelines, training_prior, player_id, day):
    # La timeline globale contiene solo il 2024: il prior si congela dopo il 2024.
    training_counts = counts_before(training_prior, day)
    serve_prior = training_counts[1] / training_counts[0] if training_counts else 0.5
    counts = counts_before(timelines.get(player_id, ([], [])), day)
    serve_rate = counts[1] / counts[0] if counts and counts[0] else serve_prior
    return_rate = counts[3] / counts[2] if counts and counts[2] else 1 - serve_prior
    return serve_rate, return_rate


class PlayerHistory:
    def __init__(self, snapshot_path, live_dir, tournaments):
        snapshot_path = Path(snapshot_path)
        if not snapshot_path.is_file():
            raise FileNotFoundError(
                f"Storico assente: {snapshot_path}. Generarlo offline con "
                "python -m training.prepare_dataset --history-only"
            )
        with snapshot_path.open(encoding="utf-8") as file:
            snapshot = json.load(file)
        if snapshot.get("schema_version") != 2:
            raise ValueError("Rigenerare lo storico con python -m training.prepare_dataset --history-only")
        self.training_year = snapshot["training_year"]
        self.base_match_ids = {canonical_id(match_id) for match_id in snapshot["match_ids"]}
        self.base_timelines = {
            canonical_id(player_id): (
                [row["date"] for row in rows],
                [[row[field] for field in COUNT_FIELDS] for row in rows]
            ) for player_id, rows in snapshot["players"].items()
        }
        self.base_training_prior = (
            [row["date"] for row in snapshot["training_prior"]],
            [[row["points"], row["serve_wins"]] for row in snapshot["training_prior"]]
        )
        self.matches = {}
        self.versions = {}
        self.live_dir = Path(live_dir)
        self.tournaments = tournaments
        for path in sorted(self.live_dir.glob("*.json")):
            with path.open(encoding="utf-8") as file:
                saved = json.load(file)
            record = saved["record"]
            if record["match_id"] in self.base_match_ids:
                continue
            self.matches[record["match_id"]] = record
            self.versions[record["match_id"]] = tuple(saved["source_version"])
        self.rebuild_timelines()
        print(f"Storico pronto da {snapshot_path}: {len(self.base_match_ids)} match offline, "
              f"{len(self.matches)} match live, {len(self.timelines)} giocatori.", flush=True)

    def rebuild_timelines(self):
        per_player = defaultdict(lambda: defaultdict(lambda: [0, 0, 0, 0]))
        training_days = defaultdict(lambda: [0, 0])
        for record in self.matches.values():
            day = record["date"]
            for player_id, counts in record["players"].items():
                per_player[player_id][day] = [a + b for a, b in zip(per_player[player_id][day], counts)]
                if int(day[:4]) == self.training_year:
                    training_days[day][0] += counts[0]
                    training_days[day][1] += counts[1]
        self.timelines = dict(self.base_timelines)
        for player_id, days in per_player.items():
            self.timelines[player_id] = merge_timeline(self.base_timelines.get(player_id, ([], [])), days)
        self.training_prior = merge_timeline(self.base_training_prior, training_days)

    def update_finished_match(self, match, match_id, partition, offset, timestamp):
        if match.get("event_status") != "Finished" or not is_supported_live_match(match, self.tournaments):
            return False
        match_id = canonical_id(match_id)
        # La base offline è una fotografia fissa: i suoi match sono già conteggiati.
        if match_id in self.base_match_ids:
            return False
        # Kafka timestamp e offset distinguono i replay dagli aggiornamenti successivi.
        version = (timestamp.isoformat(), int(partition), int(offset))
        previous_version = self.versions.get(match_id)
        if previous_version is not None:
            if partition == previous_version[1] and offset <= previous_version[2]:
                return False
            if partition != previous_version[1] and version <= previous_version:
                return False
        record = summarize_match(match)
        rules.require(record["match_id"] == match_id, "Chiave Kafka diversa dall'ID del match")
        changed = self.matches.get(match_id) != record
        self.live_dir.mkdir(parents=True, exist_ok=True)
        path = self.live_dir / f"{match_id}.json"
        temporary = path.with_suffix(".tmp")
        with temporary.open("w", encoding="utf-8") as file:
            json.dump({"record": record, "source_version": version}, file)
        temporary.replace(path)
        self.matches[match_id] = record
        self.versions[match_id] = version
        return changed
