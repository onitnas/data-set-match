## Configurazione

`tennis-predictions-template.json` definisce mapping e impostazioni dei nuovi
indici: uno shard primario, zero repliche, probabilità `double`, nomi e ID
`keyword`, conteggi `integer` e `timestamp` di tipo `date`.
`elasticsearch-init` installa il template tramite la REST API e termina.

La sessione Spark configura `es.nodes`, `es.port` e `es.nodes.wan.only`.
Dentro `foreachBatch`, dopo la preparazione delle feature e l'inferenza,
`write_predictions()` applica al DataFrame:

```python
documents.write.format("es") \
    .option("es.mapping.id", "document_id") \
    .option("es.spark.dataframe.write.null", "true") \
    .mode("append") \
    .save(elasticsearch_index)
```

## Documenti e recupero

Il batch termina dopo la scrittura Elasticsearch. Se fallisce, Spark non
conferma gli offset di ingresso; al riavvio recupera l'output dalla cache e
ritenta la scrittura con gli stessi ID. Il checkpoint esistente è mantenuto.
Il modello non viene riaddestrato e non si rilegge automaticamente tutto Kafka.

## Avvio e verifica

Dalla radice:

```bash
docker compose config -q
docker compose up --build -d filebeat spark-streaming
docker compose logs -f --tail 30 spark-streaming
```

Spark deve mostrare `previsioni salvate in tennis-predictions`.
Elasticsearch è accessibile su `http://localhost:9200`:

```bash
curl -s 'http://localhost:9200/_cluster/health?pretty'
curl -s 'http://localhost:9200/tennis-predictions/_count?pretty'
curl -s 'http://localhost:9200/tennis-predictions/_search?size=3&sort=timestamp:desc&pretty'
```