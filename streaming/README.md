# Lettura e decodifica degli aggiornamenti Kafka

`stream_matches.py` legge il topic configurato in `KAFKA_TOPIC`, interpreta
il JSON e mostra le informazioni degli incontri in colonne separate.
Non costruisce ancora lo stato dei match o le feature del modello.

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
- `partition` e `offset`: identificano la posizione del messaggio nel topic.
- `timestamp`: il timestamp Kafka del messaggio.

`from_json` usa uno schema esplicito annidato: l'oggetto `match` contiene i
campi dell'API. I campi non inclusi nello schema, come `pointbypoint` e
`statistics`, restano nel payload originale e non sono ancora elaborati.
Gli identificativi vengono letti come testo, accettando le rappresentazioni
numeriche e testuali presenti nei messaggi.

`scores` viene letto come array di oggetti con `score_set`, `score_first` e
`score_second`. Quando `status` indica `Set N`, viene selezionato il punteggio
di quel set, indipendentemente dall'ordine dell'array. Per altri stati, come
`Finished`, viene mostrata l'ultima voce ricevuta insieme al suo `games_set`.
Se il set richiesto manca o l'array è vuoto, i game rimangono `null`:
non vengono sostituiti con il punteggio del set precedente o con zeri inventati.

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

Lo script è montato dal filesystem locale, quindi basta riavviare il servizio:

```bash
docker compose restart spark-streaming
docker compose logs --since 1m -f spark-streaming
```