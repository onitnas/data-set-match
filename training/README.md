# Download e preparazione del dataset

Questa cartella contiene gli script per scaricare e preparare gli incontri ATP
dei quattro Slam e dei nove Masters 1000. Il training Spark ML sarà aggiunto
in un branch successivo.

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

Inserendo `2024`, vengono letti i JSON nella cartella `training/data/raw/2024/`.
L'output viene creato direttamente in:

```text
training/data/processed/2024/
├── train.parquet
├── validation.parquet
├── test.parquet
└── schema.json
```

Non viene salvato un JSON intermedio. La directory di output deve essere nuova;
lo script si ferma se esiste già. Per scegliere un altro percorso:

```python
from training.prepare_dataset import PrepareDataset

dataset = PrepareDataset(
    "training/data/raw/2024",
    "training/data/processed/2024_v2"
)
dataset.prepare_dataset()
```

## Cosa fa lo script

1. Legge i JSON della cartella annuale e controlla i duplicati tramite `event_key`.
2. Seleziona i match `Finished` del tabellone principale dei tornei configurati.
3. Ricostruisce i punti e scarta l'intero incontro se la sequenza è incoerente.
4. Divide cronologicamente gli incontri e salva i tre Parquet.

Le regole sono in `tennis_rules.py`, un modulo con sole funzioni e costanti,
senza classi, lettura di file o dipendenze esterne. Il percorso è
`decode_match` → `decode_set` → `decode_game` oppure `decode_tiebreak`.
`group_sets` raggruppa i record per set. Ogni punto restituito contiene il
punteggio precedente, game e set già vinti, battitore e vincitore del punto.

`prepare_dataset.py` mantiene la classe `PrepareDataset`: legge i JSON, chiama
`tennis_rules.decode_match(match)` e costruisce le 18 colonne con `create_row`.
Infine sceglie gli split temporali e scrive i Parquet con PyArrow. La lista
dei punti rimane in memoria: non è un nuovo file intermedio.

Ogni giornata resta nello stesso split. Le quote sono circa 70% training,
15% validation e 15% test, misurate sul numero di incontri. Servono almeno
tre giornate valide. I duplicati identici sono ignorati; versioni diverse dello
stesso incontro interrompono il programma.

Le righe sono raccolte in liste in memoria e scritte con `parquet.write_table`.

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
Il futuro `VectorAssembler` dovrà usare soltanto `feature_columns`.
Live, storico, superficie e identificativi dei giocatori non sono feature
di questa prima versione. Non è presente una colonna `split`: il gruppo
è identificato dal file Parquet.

## Perché serve ricostruire il punteggio

Nel feed `score` è il punteggio dopo il punto. Confrontandolo con quello
precedente si ricava il vincitore, mentre la riga contiene lo stato precedente.
Il punto conclusivo di un game viene aggiunto soltanto quando il vincitore
registrato può chiuderlo legalmente con un unico punto; `is_inferred` lo segnala.

Lo script controlla vantaggi, servizio, tie-break, continuità della numerazione
e corrispondenza con i risultati finali. I formati supportati sono quelli dei
main draw 2023-2026: Slam al meglio dei cinque set e Masters al meglio dei tre,
tie-break sul 6-6 a 7 punti, oppure a 10 nel quinto set degli Slam.

Nei JSON 2023 molti game iniziano dal numero 2: questi incontri vengono
esclusi, senza correggere automaticamente i dati del provider. File mancanti
e incontri esclusi limitano la copertura effettiva del dataset.

## File

- `fetch_matches.py`: download degli incontri per torneo.
- `config/tournaments.json`: elenco dei tornei.
- `tennis_rules.py`: funzioni per punteggi, servizio e ricostruzione dei punti.
- `prepare_dataset.py`: lettura, creazione delle 18 colonne, split e salvataggio.
- `requirements.txt`: dipendenze Python.

I dati in `training/data/` sono ignorati da Git.
