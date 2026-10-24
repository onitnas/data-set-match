# Download e preparazione del dataset

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

Il training del modello verrà implementato in un branch successivo con queste fasi:

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
- `requirements.txt`: dipendenze Python.