# Probabilità live di vittoria del match

Il modello operativo è un `GBTClassifier` Spark ML con 21 feature, senza momentum.
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

`prepare_dataset.py` legge i raw, controlla duplicati e date, seleziona ATP
Singles conclusi nei tabelloni principali dei tornei configurati e ricostruisce
i punti con `tennis_rules.py`. Un incontro incoerente viene scartato interamente.

`decode_match()` percorre set e game e controlla servizio e risultati del feed.
`decode_game()` e `decode_tiebreak()` usano direttamente i punteggi intermedi
forniti dall'API. `add_point()` salva le copie dello stato prima del punto
e assegna `point_index`; poi il punteggio viene aggiornato con quello letto.
`next_state()` gestisce il punto conclusivo del game o del tie-break e
l'eventuale passaggio al set successivo. Restano i controlli sul formato,
sulla numerazione senza buchi o duplicati, sul servizio e sui risultati finali.

Il percorso di output predefinito è `training/data/processed`.
`--input-dir` e `--players-dir` permettono di cambiare gli input.

La preparazione salva anche `player_history.json`: conteggi cumulativi per
giocatore e giornata, prior del 2024 e ID degli incontri già inclusi.
Lo streaming carica questo file per costruire le feature storiche.
Per aggiungerlo ai dataset già presenti, senza riscrivere Parquet, schema
o modello, eseguire dalla radice:

```bash
.venv/bin/python -m training.prepare_dataset --history-only
```

Il comando crea o sostituisce soltanto `player_history.json` nella directory
di output. La preparazione completa continua a richiedere una nuova directory.

| Split | Anno | Incontri | Stati |
| --- | --- | --- | --- |
| Training | 2024 | 1.015 | 186.090 |
| Validation | 2025 | 995 | 180.494 |
| Test | 2026 | 928 | 164.313 |

Ogni riga è lo stato prima di un punto. Non viene incluso lo stato successivo
alla conclusione del match. Tutti gli stati di un incontro appartengono allo
stesso split.

Il punto conclusivo di un game viene inferito soltanto quando è ricostruibile
legalmente con un unico punto; `is_inferred` lo segnala. I punteggi intermedi
sono considerati autorevoli e non vengono ricalcolati per confrontarli con
l'API. Se il punteggio salta pur mantenendo una numerazione continua, non
vengono aggiunti i punti intermedi mancanti: i conteggi storici si basano sui
record disponibili e sui punti conclusivi inferiti. Il decoder supporta i
formati configurati 2023-2026: Masters al meglio dei tre set, Slam al meglio
dei cinque, tie-break a 7 punti o a 10 nel quinto set degli Slam.

La preparazione salva i Parquet, lo schema delle feature e lo storico giocatori.
I conteggi e i motivi degli scarti vengono mostrati in console. La pubblicazione
dell'output avviene dopo il completamento della preparazione.

## Feature e metadati

Le 21 feature sono:

- `best_of`, `surface`.
- `player_a_serve_win_rate`, `player_a_return_win_rate`,
  `player_b_serve_win_rate`, `player_b_return_win_rate`.
- `player_a_rank`, `player_b_rank`,
  `player_a_rank_missing`, `player_b_rank_missing`.
- `sets_a`, `sets_b`, `games_a`, `games_b`.
- `regular_score_a`, `regular_score_b`.
- `is_tiebreak`, `tiebreak_target`, `tiebreak_score_a`, `tiebreak_score_b`.
- `player_a_is_serving`.

L'orientamento segue l'ordine dell'API: `player_a = 0`, `player_b = 1`.
Gli ID e la data sono metadati; non entrano nelle feature.

Dataset e modelli preparati con il precedente orientamento tramite hash
devono essere rigenerati in nuove directory di output. Cambiare soltanto
`schema.json` non converte i dati o il modello. Il file `player_history.json`
contiene conteggi per ID giocatore e non dipende dall'orientamento A/B.

Verifica delle transizioni, dello stato e delle feature live:

```bash
.venv/bin/python -m unittest streaming.test_streaming.MatchStateTests -v
```

Lo storico di servizio e risposta usa soltanto gli incontri accettati dei
giorni precedenti, inclusi i punti inferiti. Tutti i match di una giornata
ricevono lo storico prima dell'aggiornamento con gli esiti del giorno.
Lo storico resta costante durante un incontro.

Senza storico personale si usa una media globale per ruolo, inizialmente
0,5. Nel 2024 comprende solo i giorni già conclusi; dal 2025 resta fissata
sul 2024. Gli storici personali continuano ad aggiornarsi a fine giornata
anche nel 2025 e nel 2026, senza riaddestrare il modello.

I ranking sono le statistiche `singles` della stagione precedente:
2023 per incontri 2024, 2024 per 2025, 2025 per 2026. Sono un'approssimazione
annuale, non ranking settimanali. Un ranking assente vale zero con indicatore
`rank_missing = 1`.

La superficie proviene da `config/tournaments.json` ed è codificata con
`StringIndexer` e `OneHotEncoder`. Il punteggio ordinario usa 0/1/2/3/4
per 0/15/30/40/vantaggio; nel tie-break si usano i conteggi dedicati.
`player_a_is_serving` vale 1 quando serve A.

Le feature non richiedono l'ordine degli ultimi punti. Nel live il riepilogo
API fornisce lo stato corrente; la ricostruzione dei punti rimane nella
preparazione offline e nell'aggiornamento delle statistiche dei match conclusi.

Ogni riga pesa `1 / numero_di_stati_del_match`: ciascun incontro contribuisce
con peso totale uno nel training e nelle metriche.

## Training con Docker

```bash
docker compose up --build spark-ml
```

Compose monta `training/data/processed` in sola lettura nel container e rende
persistente `training/artifacts`. L'immagine contiene Python 3.13, Java e
PySpark ML 4.2.0. Spark gira in locale con due thread.

Il training legge lo schema, codifica la superficie e costruisce
`VectorAssembler → GBTClassifier`. Senza parametri espliciti prova otto
combinazioni sul 2024: `maxDepth=[3,5]`, `maxIter=[20,60]`,
`stepSize=[0.05,0.1]`, seed 42; sceglie la Log Loss minore sul 2025.
Con `--max-depth`, `--max-iter` e `--step-size` verifica invece una sola
configurazione fissata. In entrambi i casi riaddestra da zero su 2024 + 2025.

Il modello a 21 feature è stato adottato dopo un confronto sul 2025 con la
versione a 23 feature. Usa parametri fissati: profondità 3, 60 iterazioni e
passo 0,1. Le metriche e i parametri scelti vengono mostrati in console.
Se il modello completo con lo stesso schema esiste già, il comando salta
il training prima di avviare Spark. Per un nuovo addestramento:

```bash
docker compose run --build --rm spark-ml python -m training.train_model \
  --output-dir training/artifacts/run2 --max-depth 3 --max-iter 60 --step-size 0.1
```

Per cambiare dataset usare `TENNIS_DATASET_DIR`; il percorso interno rimane
`/app/training/data/processed`.

## Valutazione finale

```bash
docker compose run --rm spark-ml python -m training.evaluate_model
```

Carica il modello salvato, controlla lo schema e valuta il 2026 senza training.
Mostra le metriche in console senza creare report su file. La baseline costante
viene ricalcolata dal Parquet di training del 2024; quella di ranking usa il test.

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
- `tennis_rules.py`: ricostruzione e controllo dei punteggi.
- `historical_features.py`: storico dei giocatori aggiornato a fine giornata.
- `player_rankings.py`: ranking singles della stagione precedente.
- `prepare_dataset.py`: dataset live, split annuali, schema e storico giocatori.
- `train_model.py`: selezione sul 2025 e riaddestramento su 2024 + 2025.
- `evaluate_model.py`: metriche finali del modello fissato sul 2026.
