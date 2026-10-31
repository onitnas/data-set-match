# Probabilità live di vittoria del match

Il modello operativo è un `GBTClassifier` Spark ML con 23 feature.
`label = 1` indica la vittoria finale di A, `0` quella di B.
I giocatori A/B rimangono fissi durante l'incontro, anche quando cambia
il battitore. Tutti gli stati dello stesso match hanno la stessa label.

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

Il percorso di output predefinito è `training/data/processed`.
`--input-dir` e `--players-dir` permettono di cambiare gli input.

| Split | Anno | Incontri | Stati |
| --- | --- | --- | --- |
| Training | 2024 | 981 | 178.643 |
| Validation | 2025 | 980 | 177.251 |
| Test | 2026 | 911 | 160.650 |

Ogni riga è lo stato prima di un punto. Non viene incluso lo stato successivo
alla conclusione del match. Tutti gli stati di un incontro appartengono allo
stesso split.

Il punto conclusivo di un game viene inferito soltanto quando è ricostruibile
legalmente con un unico punto; `is_inferred` lo segnala. Il decoder controlla
punteggi, servizio, tie-break, numerazione e risultati finali. Supporta i
formati configurati 2023-2026: Masters al meglio dei tre set, Slam al meglio
dei cinque, tie-break a 7 punti o a 10 nel quinto set degli Slam.

`report.json` riporta conteggi, motivi degli scarti, tornei, copertura, ranking
mancanti e politica dello storico. La pubblicazione dell'output avviene dopo
il completamento della preparazione.

## Feature e metadati

Le 23 feature sono:

- `best_of`, `surface`.
- `player_a_serve_win_rate`, `player_a_return_win_rate`,
  `player_b_serve_win_rate`, `player_b_return_win_rate`.
- `player_a_rank`, `player_b_rank`,
  `player_a_rank_missing`, `player_b_rank_missing`.
- `sets_a`, `sets_b`, `games_a`, `games_b`.
- `regular_score_a`, `regular_score_b`.
- `is_tiebreak`, `tiebreak_target`, `tiebreak_score_a`, `tiebreak_score_b`.
- `player_a_is_serving`, `player_a_momentum`, `player_b_momentum`.

A viene scelto con il primo byte di SHA-256 del `match_id` modulo due:
l'orientamento è stabile e indipendente da ranking, servizio e risultato.
Gli ID e la data sono metadati; non entrano nelle feature.

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

Il momentum conta i punti consecutivi già vinti dal giocatore. Il vincitore
incrementa la propria serie e azzera quella dell'avversario dopo la costruzione
della riga corrente. La serie continua tra game, set e cambi di servizio;
riparte da zero a inizio incontro.

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
`VectorAssembler → GBTClassifier`. Prova otto combinazioni sul 2024:
`maxDepth=[3,5]`, `maxIter=[20,60]`, `stepSize=[0.05,0.1]`, seed 42.
Sceglie la Log Loss minore sul 2025 e riaddestra da zero su 2024 + 2025.

Il modello selezionato usa profondità 3, 60 iterazioni e passo 0,1.
Se il modello completo con lo stesso schema esiste già, il comando salta
il training prima di avviare Spark. Per un nuovo addestramento:

```bash
docker compose run --rm spark-ml python -m training.train_model --output-dir training/artifacts/run2
```

Per cambiare dataset usare `TENNIS_DATASET_DIR`; il percorso interno rimane
`/app/training/data/processed`.

## Valutazione finale

```bash
docker compose run --rm spark-ml python -m training.evaluate_model
```

Carica il modello salvato, controlla lo schema e valuta il 2026 senza training.
Se il report esiste e corrisponde agli stessi artefatti, mostra il risultato
registrato. Il modello e il test sono identificati nel report tramite SHA-256.

| Metrica live | Validation 2025 | Test 2026 |
| --- | --- | --- |
| Accuracy | 76,91% | 78,18% |
| Log Loss | 0,472565 | 0,456618 |
| Brier Score | 0,156105 | 0,149090 |
| ROC-AUC | 0,854676 | 0,866584 |

Le metriche di validation appartengono al candidato addestrato sul 2024;
quelle del test al modello finale riaddestrato su 2024 + 2025.
Il test contiene 911 incontri dal 18 gennaio al 17 ottobre 2026.

## File Python

- `fetch_matches.py`: download degli incontri per torneo e anno.
- `fetch_players.py`: download dei profili giocatori, riutilizzando quelli già salvati.
- `tennis_rules.py`: ricostruzione e controllo dei punteggi.
- `historical_features.py`: storico dei giocatori aggiornato a fine giornata.
- `player_rankings.py`: ranking singles della stagione precedente.
- `prepare_dataset.py`: dataset live, split annuali, schema e report.
- `train_model.py`: selezione sul 2025 e riaddestramento su 2024 + 2025.
- `evaluate_model.py`: metriche finali del modello fissato sul 2026.
