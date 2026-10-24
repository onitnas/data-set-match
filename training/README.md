# Dataset e addestramento del modello

Questa cartella contiene gli script per scaricare e preparare gli incontri ATP
dei quattro Slam e dei nove Masters 1000.

## Installazione

Eseguire i comandi dalla radice del progetto:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r training/requirements.txt
```

## Download

```bash
read -rs -p 'API Tennis key: ' API_TENNIS_KEY
export API_TENNIS_KEY
.venv/bin/python -m training.fetch_matches
```

Inserire l'anno quando richiesto. La chiave viene letta dall'ambiente; i file
`.env` non sono caricati automaticamente. I tornei e le loro chiavi sono in
`config/tournaments.json`: verificarle per la stagione scelta.

Ogni torneo viene salvato in `training/data/raw/<anno>/<torneo>_<anno>.json`,
con la lista degli incontri nel campo `tournament`. Vengono esclusi qualificazioni
e incontri senza `pointbypoint`. Un torneo senza incontri viene saltato.
Una nuova esecuzione sovrascrive i JSON dello stesso anno e torneo.

## Preparazione

```bash
.venv/bin/python -m training.prepare_dataset
```

Il comando legge i JSON nelle cartelle `training/data/raw/2024/`,
`training/data/raw/2025/` e `training/data/raw/2026/`.
Tutte e tre le cartelle devono essere presenti.
L'output viene creato in una nuova directory:

```text
training/data/processed/
├── train.parquet
├── validation.parquet
├── test.parquet
├── schema.json
└── report.json
```

## Cosa fa lo script

1. Legge i JSON delle tre annate e controlla i duplicati tramite `event_key`.
2. Seleziona i match `Finished` del tabellone principale dei tornei configurati.
3. Ricostruisce i punti e scarta l'intero incontro se la sequenza è incoerente.
4. Assegna il 2024 al training, il 2025 alla validation e il 2026 al test,
   poi salva i tre Parquet, lo schema e il riepilogo.

Le regole sono in `tennis_rules.py`. Il percorso è
`decode_match` → `decode_set` → `decode_game` oppure `decode_tiebreak`.
`group_sets` raggruppa i record per set. Ogni punto restituito contiene il
punteggio precedente, game e set già vinti, battitore e vincitore del punto.

`prepare_dataset.py` mantiene la classe `PrepareDataset`: legge i JSON, chiama
`tennis_rules.decode_match(match)` e costruisce le 18 colonne con `create_row`.
Infine assegna gli split per stagione e scrive i Parquet con PyArrow. La lista
dei punti rimane in memoria: non è un nuovo file intermedio.

Ogni stagione resta interamente nello stesso split, senza percentuali
prestabilite. Serve almeno un incontro valido per ciascuna delle tre annate.
I duplicati identici nella stessa annata sono ignorati; versioni diverse dello
stesso incontro, identificativi ripetuti tra annate o date non coerenti con la
cartella annuale interrompono il programma.

Le righe sono raccolte in liste in memoria e scritte con `parquet.write_table`.

## Protocollo di addestramento e valutazione

`train_model.py` implementa le prime due fasi del protocollo. La valutazione
definitiva verrà eseguita con uno script separato:

1. Addestramento su `train.parquet` (2024) e scelta degli iperparametri usando
   `validation.parquet` (2025).
2. Scelti gli iperparametri e fissata la procedura, riaddestramento su
   `train.parquet` + `validation.parquet` (2024 e 2025).
3. Valutazione definitiva su `test.parquet` (2026), mai usato per addestrare,
   scegliere iperparametri o ottimizzare il modello, inclusa l'eventuale
   calibrazione delle probabilità o scelta della soglia di classificazione.

Le metriche previste sono:

| Metrica | Cosa misura | Direzione migliore |
| --- | --- | --- |
| Accuracy | Percentuale di punti classificati correttamente alla soglia scelta | Più alta |
| Log Loss | Qualità delle probabilità, penalizzando gli errori molto sicuri | Più bassa |
| Brier Score | Errore quadratico medio tra probabilità del battitore ed esito 0/1 | Più basso |
| ROC-AUC | Capacità di ordinare i punti vinti dal battitore sopra quelli persi | Più alta |

## Training con Docker

Con i Parquet e `schema.json` già presenti in `training/data/processed/`,
eseguire dalla radice del progetto:

```bash
docker compose up --build spark-ml
```

L'immagine contiene Python 3.13, il runtime Java predefinito di Debian e PySpark ML 4.2.0. Spark gira in
modalità locale nel container con due thread; i dataset sono montati in sola
lettura. Non serve installare PySpark nell'ambiente Python dell'host.

Lo script legge l'ordine delle feature da `schema.json`, usa `VectorAssembler`
e `GBTClassifier` e prova otto configurazioni definite in `PARAMETER_GRID`:
profondità 3/5, iterazioni 30/60 e passo 0.05/0.1. Ogni configurazione viene
addestrata sul 2024 e valutata sul 2025 con le quattro metriche. Viene calcolato
anche un riferimento a probabilità costante, stimata sul 2024 e valutata sul 2025.
La configurazione con Log Loss minore viene riaddestrata da zero su 2024 + 2025.
La classificazione usa soglia 0.5 e il seme casuale è 42. Il 2026 non viene letto.

Il codice segue un flusso principale sequenziale, con commenti numerati e messaggi
di avanzamento, sul modello dell'esempio del corso
[`sentiment_analysis_model.py`](https://github.com/tapunict/tap2026/blob/main/spark/code/sentiment_analysis_model.py).
L'assemblatore, il classificatore e la pipeline sono costruiti esplicitamente;
`pipeline.fit()` addestra ogni candidato e `model.transform()` produce le
previsioni sulla validation. La funzione `evaluate_predictions` raccoglie le
quattro metriche ed evita di ripeterne il calcolo nel codice.

L'output, persistente e ignorato da Git, è:

```text
training/artifacts/model/
├── pipeline/             # VectorAssembler + GBTClassificationModel
├── schema.json
└── training_report.json  # Baseline, candidati, metriche 2025 e parametri scelti
```

Le metriche nel report sono quelle dei candidati addestrati sul 2024 e valutati
sul 2025, prima del riaddestramento finale. Non sono metriche del modello finale
sul 2026. La probabilità `probability[1]` indica la vittoria del battitore.

La pipeline e i report vengono salvati in una directory temporanea e pubblicati
nel percorso finale solo a salvataggio concluso. Se quel modello esiste già,
lo script termina senza addestrare e senza avviare Spark. Un percorso finale
incompleto produce un errore. Non viene usato un file `training.complete`.

Per un nuovo addestramento usare una directory diversa, senza cancellare il
modello precedente:

```bash
docker compose run --rm spark-ml python -m training.train_model --output-dir training/artifacts/run_2
```

Per controllare la configurazione Compose senza avviare il training:

```bash
docker compose config --quiet
```

## Riepilogo della preparazione

Ogni preparazione completata salva `report.json` accanto ai Parquet e stampa
un riepilogo dei conteggi in console. Il file contiene:

- `input_files`: numero di file JSON letti.
- `matches_read`, `unique_matches`, `identical_duplicates`: incontri letti,
  incontri unici e occorrenze duplicate identiche ignorate.
- `filtered_matches` e `filtered_by_reason`: incontri esclusi dai filtri,
  raggruppati per motivo (qualificazioni, stato, tipo o torneo).
- `rejected_matches` e `rejected_by_reason`: incontri scartati durante la
  ricostruzione, raggruppati per messaggio di errore.
- `rejected_details`: identificativo dell'incontro, chiave del torneo e motivo
  di ogni scarto durante la ricostruzione.
- `accepted_matches`: numero di incontri inclusi nel dataset.
- `tournaments`: nome e conteggi `read`, `filtered`, `rejected`, `accepted`
  per ogni chiave torneo. `read` conta gli incontri unici; i tornei configurati
  senza incontri hanno tutti i conteggi a zero.
- `splits`: anno, incontri, punti, punti inferiti, prima/ultima data effettiva e
  conteggi degli incontri accettati per chiave torneo di training, validation e
  test. I tornei configurati senza incontri accettati nello split valgono zero.

## Le 18 colonne

Ogni riga descrive lo stato **prima** di un punto.

| Gruppo | Colonne |
| --- | --- |
| 6 metadati | `match_id`, `match_date`, `server_id`, `receiver_id`, `point_index`, `is_inferred` |
| 11 feature | `best_of`, `sets_server`, `sets_receiver`, `games_server`, `games_receiver`, `regular_score_server`, `regular_score_receiver`, `is_tiebreak`, `tiebreak_target`, `tiebreak_score_server`, `tiebreak_score_receiver` |
| 1 target | `label`: 1 se vince il battitore, 0 se vince il ribattitore |

Le feature sono orientate rispetto al battitore (`server`) e al ribattitore
(`receiver`). Il punteggio ordinario usa la codifica
`0/1/2/3/4 = 0/15/30/40/A`. Nel tie-break si usano conteggi numerici nelle
colonne dedicate e le colonne del punteggio ordinario valgono zero.

`schema.json` elenca feature, metadati, target e codifica del punteggio.

## Perché serve ricostruire il punteggio

Nel feed `score` è il punteggio dopo il punto. Confrontandolo con quello
precedente si ricava il vincitore, mentre la riga contiene lo stato precedente.
Il punto conclusivo di un game viene aggiunto soltanto quando il vincitore
registrato può chiuderlo legalmente con un unico punto; `is_inferred` lo segnala.

Lo script controlla vantaggi, servizio, tie-break, continuità della numerazione
e corrispondenza con i risultati finali. I formati supportati sono quelli dei
main draw 2023-2026: Slam al meglio dei cinque set e Masters al meglio dei tre,
tie-break sul 6-6 a 7 punti, oppure a 10 nel quinto set degli Slam.

## File

- `fetch_matches.py`: download degli incontri per torneo.
- `config/tournaments.json`: elenco dei tornei.
- `tennis_rules.py`: funzioni per punteggi, servizio e ricostruzione dei punti.
- `prepare_dataset.py`: lettura, creazione delle 18 colonne, split e salvataggio.
- `train_model.py`: selezione degli iperparametri e riaddestramento finale.
- `Dockerfile`: ambiente del training Spark.
- `requirements.txt`: dipendenze Python per download e preparazione dei dati.
