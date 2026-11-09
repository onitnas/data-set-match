# Probabilità live di vittoria del match

Il modello operativo è un `GBTClassifier` Spark ML con 17 feature, senza momentum.
`label = 1` indica la vittoria finale di A, `0` quella di B.
A è sempre il primo giocatore dell'API (indice 0), B il secondo (indice 1),
anche quando cambia il battitore. Tutti gli stati dello stesso match hanno
la stessa label.

## Raccolta e preparazione

Dalla radice del repository:

```bash
.venv/bin/python -m pip install -r training/requirements.txt
```

`fetch_matches.py` legge i tornei configurati, chiede l'anno e salva i JSON
tramite `get_fixtures`. Riscrive i file restituiti per quell'annata:

```bash
.venv/bin/python -m training.fetch_matches
```

`fetch_players.py` chiama `get_players` e salva un JSON per giocatore, saltando
i profili già presenti:

```bash
.venv/bin/python -m training.fetch_players
```

`prepare_dataset.py` legge gli incontri conclusi e usa direttamente i punteggi
forniti dall'API, senza ricostruire punti mancanti:

```bash
.venv/bin/python -m training.prepare_dataset
```

Gli input sono `training/data/raw` e `training/data/players`; l'output è
`training/data/processed`. La directory di output deve essere assente.
Il 2024 va in `train.parquet`, il 2025 in `validation.parquet` e il 2026
in `test.parquet`. `schema.json` descrive le colonne (versione 6).

Ogni match include uno stato iniziale con `point_index = 0`, set, game e
punti a zero e il primo battitore indicato dall'API. Le righe successive
contengono il punteggio osservato dopo il punto. La valutazione a inizio
incontro usa esclusivamente `point_index = 0`.

Un punto con score `None` viene saltato. Se manca il risultato di un game
necessario, viene scartato il match; un eventuale game finale vuoto con
score `None` viene ignorato. Gli stati con tie-break già concluso vengono
esclusi: il risultato del set arriva dal riepilogo del game 13. Il conteggio
dei set vinti deve coincidere con il risultato finale del match.

Le 17 feature sono:

- `best_of`, `surface`.
- `player_a_rank`, `player_b_rank`, `player_a_rank_missing`, `player_b_rank_missing`.
- `sets_a`, `sets_b`, `games_a`, `games_b`.
- `regular_score_a`, `regular_score_b`.
- `is_tiebreak`, `tiebreak_target`, `tiebreak_score_a`, `tiebreak_score_b`.
- `player_a_is_serving`.

A è sempre il primo giocatore dell'API, B il secondo. ID, data e indice
sono metadati. I ranking singles provengono dalla stagione precedente;
un ranking assente vale zero con indicatore `rank_missing = 1`.
La superficie proviene da `config/tournaments.json`. Il punteggio ordinario
usa 0/1/2/3/4 per 0/15/30/40/vantaggio; il tie-break usa conteggi dedicati.
Il battitore è quello dello stato corrente, quindi dopo un punto dispari
del tie-break cambia rispetto a chi ha appena servito.

Ogni riga pesa `1 / numero_di_stati_salvati_del_match`: ogni incontro
contribuisce con peso totale uno, anche quando alcuni punti vengono saltati.
Dataset e modello delle versioni precedenti devono essere rigenerati.

## Training con Docker

```bash
docker compose up --build spark-ml
```

Compose monta `training/data/processed` in sola lettura nel container e rende
persistente `training/artifacts`. L'immagine contiene Python 3.13, Java e
PySpark ML 4.2.0. Spark gira in locale con due thread.

Il training addestra due candidati sul 2024: uno con gli iperparametri
attuali e uno con i default di Spark. Mostra le quattro metriche sul 2025
e sceglie la configurazione con log loss pesata minore. A parità mantiene
quella attuale. Riaddestra poi da zero su 2024 + 2025 e salva il modello.
Lo script segue la struttura lineare dell'esempio
[`sentiment_analysis_model.py` di TAP2026](https://github.com/tapunict/tap2026/blob/main/spark/code/sentiment_analysis_model.py).
Usa i Parquet prodotti da `prepare_dataset.py`; i controlli sui dati restano
nella preparazione e le metriche in `evaluate_model.py`.
La pipeline è `StringIndexer → OneHotEncoder → VectorAssembler → GBTClassifier`;
i primi due passaggi codificano `surface`.

I percorsi `DATA_DIR` e `MODEL_DIR` e il seed `SEED = 42` si modificano
all'inizio di `train_model.py`. Il candidato attuale usa
`maxDepth=3`, `maxIter=60`, `stepSize=0.1`; l'altro lascia gli iperparametri
ai default di `GBTClassifier`. Entrambi usano gli stessi pesi per match e seed.
Non ci sono argomenti da terminale per il training.

La configurazione attuale deriva dagli esperimenti precedenti sul 2025.
Il 2026 non entra nell'addestramento; `evaluate_model.py` ne calcola le metriche.
Se `MODEL_DIR` esiste già, lo script termina con un errore prima di avviare
Spark. Per un nuovo esperimento scegliere, per esempio,
`MODEL_DIR = Path("training/artifacts/run2/model")`, quindi ricostruire
l'immagine per includere la configurazione modificata:

```bash
docker compose run --build --rm spark-ml
```

Il salvataggio è diretto: un'interruzione può lasciare una directory incompleta.
Per un esperimento successivo scegliere un altro percorso.

Per cambiare dataset usare `TENNIS_DATASET_DIR`; il percorso interno rimane
`/app/training/data/processed`.

## Valutazione finale

```bash
docker compose run --rm spark-ml python -m training.evaluate_model
```

Per valutare un esperimento salvato altrove, aggiungere per esempio
`--model-dir training/artifacts/run2/model` al comando di valutazione.

Carica il modello salvato, controlla lo schema e valuta il 2026 senza training.
Mostra le metriche in console senza creare report su file. La baseline costante
viene ricalcolata dal Parquet di training del 2024; quella di ranking usa il test.

Questi risultati si riferiscono al precedente dataset con schema 4 e 21
feature; non sono una valutazione del nuovo dataset con schema 6.

| Metrica live | Validation 2025 | Test 2026 |
| --- | --- | --- |
| Accuracy | 76,35% | 77,99% |
| Log Loss | 0,483199 | 0,461415 |
| Brier Score | 0,159752 | 0,150688 |
| ROC-AUC | 0,846657 | 0,865781 |

Le metriche di validation appartengono al candidato addestrato sul 2024;
quelle del test al modello finale riaddestrato su 2024 + 2025.
Il test contiene 928 incontri del 2026.
Il 2026 era già stato esaminato con modelli precedenti: questo è un aggiornamento
della valutazione del modello fissato, non un nuovo test mai osservato.

Dataset e modello sono stati rigenerati con A/B nell'ordine dell'API e con
i punteggi intermedi considerati autorevoli. Le metriche sopra appartengono
a questa versione, con profondità 3, 60 iterazioni e passo 0,1. Gli schemi
sono alla versione 4; lo storico usa il formato 2. Il riaddestramento non usa il 2026.

## File Python

- `fetch_matches.py`: download degli incontri per torneo e anno.
- `fetch_players.py`: download dei profili giocatori, riutilizzando quelli già salvati.
- `player_rankings.py`: ranking singles della stagione precedente.
- `prepare_dataset.py`: snapshot live, split annuali e schema.
- `train_model.py`: confronto sul 2025 dei due GBT e riaddestramento su 2024 + 2025.
- `evaluate_model.py`: metriche finali del modello fissato sul 2026.
