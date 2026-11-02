# Decodifica e ricostruzione degli aggiornamenti live

`stream_matches.py` legge il topic configurato in `KAFKA_TOPIC`, interpreta
il JSON e mostra le informazioni degli incontri in colonne separate.
`match_state.py` ricostruisce lo stato dai punti e calcola il momentum,
riutilizzando le funzioni di `training/tennis_rules.py`.
Il modello e le feature storiche non sono ancora collegati.

## Avvio

Dalla radice del progetto:

```bash
docker compose up --build -d spark-streaming
docker compose logs -f spark-streaming
```

## Cosa vedere nei log

Spark esegue la query in micro-batch, con intervallo richiesto di cinque
secondi. L'intervallo non garantisce che un batch termini entro cinque secondi.
Quando ci sono dati, il terminale mostra una tabella con:

- `match_id`: la chiave Kafka convertita in stringa.
- `player_1` e `player_2`: i giocatori nell'ordine del provider.
- `tournament_id` e `tournament`: identificativo e nome del torneo.
- `status`: stato dell'incontro, per esempio `Set 1` o `Finished`.
- `sets`: risultato in set fornito da `event_final_result`.
- `games_set`: numero del set a cui si riferiscono i game mostrati.
- `games_1` e `games_2`: game dei due giocatori, letti da `scores`.
- `point_score`: punteggio del game corrente, da `event_game_result`.
- `server`: nome del giocatore al servizio; se il valore del provider non è
  riconosciuto, viene mostrato il valore originale.
- `qualification`: indicatore di qualificazione del provider.
- `parse_ok`: il JSON è stato interpretato senza errori e contiene
  `match.event_key`. Non verifica ancora la coerenza del punteggio.
- `state_ok`: la sequenza dei punti è valida e raggiunge il punteggio live;
  per i match terminati viene verificato il risultato completo.
- `state_reason`: motivo per cui non è possibile ricostruire lo stato.
- `is_finished`: incontro concluso e validato, da escludere dalle previsioni live.
- `points_seen`: punti ricostruiti, comprese le chiusure dei game inferite.
- `inferred_points`: quanti punti conclusivi sono stati ricostruiti.
- `momentum_1` e `momentum_2`: punti consecutivi vinti dai giocatori.
- `partition` e `offset`: identificano la posizione del messaggio nel topic.
- `timestamp`: il timestamp Kafka del messaggio.

`from_json` usa uno schema esplicito annidato: l'oggetto `match` contiene i
campi dell'API, compresi `scores` e `pointbypoint`. I campi non inclusi nello
schema, come `statistics`, restano nel payload originale.
Gli identificativi vengono letti come testo, accettando le rappresentazioni
numeriche e testuali presenti nei messaggi.

`scores` viene letto come array di oggetti con `score_set`, `score_first` e
`score_second`. Quando `status` indica `Set N`, viene selezionato il punteggio
di quel set, indipendentemente dall'ordine dell'array. Per altri stati, come
`Finished`, viene mostrata l'ultima voce ricevuta insieme al suo `games_set`.
Se il set richiesto manca o l'array è vuoto, i game rimangono `null`:
non vengono sostituiti con il punteggio del set precedente o con zeri inventati.

## Ricostruzione dei punti e momentum

Spark applica `decode_live_state` a ogni messaggio tramite una
[UDF Python](https://spark.apache.org/docs/4.2.0/api/python/reference/pyspark.sql/api/pyspark.sql.functions.udf.html),
cioè una funzione Python richiamata sulle righe del DataFrame.
Le regole del training controllano i set già conclusi e i game completati.
Per il game corrente si leggono soltanto i punti disponibili; il tie-break
aperto viene controllato anche nell'alternanza dei servizi.
Il punto conclusivo di un game viene inferito solo quando il vincitore è
presente e il punteggio permette di ricostruirlo senza ambiguità, come nel training.

Il momentum parte da zero all'inizio del match. Dopo ogni punto aumenta quello
del vincitore e si azzera quello dell'avversario; continua attraverso i cambi
di game e di set. Lo stato restituito è successivo ai punti ricevuti e quindi
precedente al prossimo punto. Nei match conclusi comprende anche l'ultimo punto,
ma non deve essere usato per una nuova previsione.

Ogni messaggio deve contenere lo storico `pointbypoint` dall'inizio dell'incontro.
Il calcolo riparte da quello storico per ogni snapshot: non esiste un contatore
Python condiviso fra i match o fra i batch. Ripetere un messaggio restituisce
gli stessi conteggi; una correzione viene ricalcolata dalla sequenza ricevuta.
Questo richiede lavoro proporzionale alla lunghezza dello storico per ogni
aggiornamento. Non elimina i messaggi duplicati dalla console e non conserva
uno stato del match fra gli aggiornamenti.

Se mancano punti o ci sono salti non ricostruibili, `state_ok=false` e i conteggi
rimangono `null`. Lo stesso accade se `pointbypoint` è in ritardo rispetto a
set, game, punti o battitore del riepilogo live: non si associa un momentum
vecchio a un punteggio più recente. `state_reason` distingue questi casi.
Un aggiornamento successivo può diventare valido quando il provider completa
o corregge i dati. Gli stati diversi da `Set N` e `Finished` e le qualificazioni
Slam non sono ancora supportati; il record rimane visibile con il motivo.

Le colonne del punteggio mostrate in console continuano a provenire dal
riepilogo dell'API anche quando `state_ok=false`. La colonna `state`, interna
al DataFrame, contiene invece i conteggi numerici validati da utilizzare
nel prossimo passaggio di costruzione delle feature.

La console mostra fino a dieci righe per batch senza abbreviare i campi.
`player_1` e `player_2` non sono ancora l'orientamento A/B usato dal modello.

Alla prima esecuzione `startingOffsets=earliest` legge anche i messaggi ancora
conservati nel topic. Possono quindi apparire vecchi ID in notazione scientifica,
pubblicati prima della normalizzazione in Logstash.

Il checkpoint risiede nel volume Docker `streaming-checkpoints`. Al riavvio,
Spark usa gli offset salvati nel checkpoint: `earliest` non causa ogni volta
la rilettura del topic dall'inizio. Se non ci sono nuovi messaggi, la query
rimane in attesa; il solo avvio non dimostra l'arrivo dei dati.

## Applicare la modifica e verificare

Per questa modifica occorre ricostruire l'immagine: il Dockerfile aggiunge il
percorso Python e include le regole condivise. Il Compose monta le regole
anche in sola lettura, così gli aggiornamenti successivi rimangono disponibili.

```bash
docker compose up --build -d spark-streaming
docker compose logs --since 1m -f spark-streaming
```

Con dati validi, solo uno dei due momentum può essere maggiore di zero.
All'inizio entrambi possono essere zero. Un nuovo punto dello stesso giocatore
allunga la sequenza; un punto dell'altro giocatore la fa ripartire da uno.