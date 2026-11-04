"""Cache delle previsioni e recupero dell'ultimo micro-batch, senza memoria dei punti."""

import json
from pathlib import Path

from training.tennis_rules import require


class MatchStateStore:
    def __init__(self, path):
        self.path = Path(path)
        self.last_batch_id = None
        self.last_offsets = []
        self.predictions = {}
        self.last_output = None
        self.model_id = None
        if self.path.is_file():
            saved = json.loads(self.path.read_text(encoding="utf-8"))
            require(saved.get("schema_version", 1) in [1, 2], "Versione della memoria live non supportata")
            self.last_batch_id = saved["batch_id"]
            # Il vecchio formato conteneva anche stati e serie di punti: non servono più.
            self.last_offsets = sorted(saved.get("offsets", saved.get("results", {})))
            self.predictions = saved.get("predictions", {})
            self.last_output = saved.get("output")
            self.model_id = saved.get("model_id")

    def replay(self, records, batch_id, model_id):
        offsets = sorted(f"{row['partition']}:{row['offset']}" for row in records)
        if batch_id == self.last_batch_id:
            require(offsets == self.last_offsets, "Il replay del batch contiene offset diversi")
            if model_id == self.model_id:
                return self.last_output
        else:
            require(self.last_batch_id is None or batch_id > self.last_batch_id, "Batch precedente allo stato salvato")
        return None

    def commit(self, records, batch_id, predictions, output, model_id):
        offsets = sorted(f"{row['partition']}:{row['offset']}" for row in records)
        saved = {"schema_version": 2, "batch_id": batch_id, "offsets": offsets,
                 "predictions": predictions, "output": output, "model_id": model_id}
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(saved, allow_nan=False), encoding="utf-8")
        temporary.replace(self.path)
        self.last_batch_id, self.last_offsets = batch_id, offsets
        self.predictions, self.last_output, self.model_id = predictions, output, model_id
