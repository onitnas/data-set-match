# Primo lettore Kafka con Spark Structured Streaming

`stream_matches.py` legge il topic configurato in `KAFKA_TOPIC` e stampa i
messaggi a terminale. Questo primo passo verifica il collegamento Kafka → Spark.

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
- `payload`: il valore Kafka convertito in testo JSON, ancora da interpretare.
- `partition` e `offset`: identificano la posizione del messaggio nel topic.
- `timestamp`: il timestamp Kafka del messaggio.

Alla prima esecuzione `startingOffsets=earliest` legge anche i messaggi ancora
conservati nel topic. Possono quindi apparire vecchi ID in notazione scientifica,
pubblicati prima della normalizzazione in Logstash.

Il checkpoint risiede nel volume Docker `streaming-checkpoints`. Al riavvio,
Spark usa gli offset salvati nel checkpoint: `earliest` non causa ogni volta
la rilettura del topic dall'inizio. Se non ci sono nuovi messaggi, la query
rimane in attesa; il solo avvio non dimostra l'arrivo dei dati.