# Elasticsearch e Kibana

Spark carica il modello già addestrato e calcola le probabilità di vittoria
del match. Elasticsearch conserva ogni aggiornamento; Kibana interroga quei
documenti per mostrarli. La dashboard non esegue addestramento o inferenza.

## Tabellone tennis in Canvas

Apri [Tabellone tennis](http://localhost:5601/app/canvas#/workpad/workpad-tabellone-tennis/page/1)
e seleziona una coppia di giocatori nella colonna **Incontri**. All’apertura viene
proposto il match con l’offset più recente. Il link di ciascun incontro conserva
la selezione anche dopo un aggiornamento della pagina.

Il tabellone legge foto, servizio, punti e risultati dei set da `tennis-scores`.
Il grafico mostra fino a 100 snapshot di `tennis-predictions` per l’incontro,
in ordine di offset. Le due curve rappresentano i due giocatori. Quando la
previsione si riferisce a uno snapshot precedente, il pannello mostra il
punteggio di quella previsione. A partita conclusa compare il vincitore.

### Avviare la diretta di prova

1. Con `TENNIS_WEBSOCKET_URL=ws://host.docker.internal:3000/tennis/live`
   nel file `.env`, avvia i servizi dalla radice del progetto:
   `docker compose up -d filebeat spark-streaming kibana`.
2. In Mockoon apri [ws.json](../ingestion/data/ws.json) e premi **Start server**.
   L’ambiente legge un frame dalla cartella `ws/` ogni 5 secondi. Alcuni frame
   consecutivi riportano lo stesso punteggio. Raggiunto il frame 724, il replay
   continua a mostrare il risultato finale.
3. Nel workpad apri **View → Auto refresh settings** e scegli **5 seconds**.
   Per verificare il flusso guarda anche **Aggiornato alle**, che indica l’ora
   del messaggio Kafka mostrato.

Per installarlo su un altro Kibana 9.5.5, apri **Stack Management → Saved Objects
→ Import** e scegli [tabellone-tennis.ndjson](tabellone-tennis.ndjson). Il file
include il workpad e la visualizzazione Vega incorporata. I dati restano nei
due indici Elasticsearch del progetto.

## Configurazione

Non viene installato un template per `tennis-predictions`: Elasticsearch
assegna automaticamente i tipi ai nuovi campi quando riceve i documenti.
Spark aspetta che Elasticsearch superi il controllo di salute prima di partire.
Gli indici e i template eventualmente già presenti conservano la propria
configurazione. Controllare il mapping effettivo prima di configurare le
viste e i grafici Kibana, in particolare il tipo di `timestamp`.

Kibana usa la stessa versione di Elasticsearch, `9.5.5`, e lo raggiunge
su `http://elasticsearch:9200` nella rete Compose. Le porte pubblicate sono
`127.0.0.1:9200` e `127.0.0.1:5601`; l'ambiente locale ha autenticazione
disattivata. Dashboard e viste dati sono conservate nel volume Elasticsearch.

Su un volume nuovo apri **Stack Management → Saved Objects → Import** e scegli
[general_dashboard.ndjson](general_dashboard.ndjson). L’export include **General Dashboard**
e la data view **Data Set Match Prediction**, associata a `tennis-predictions` con
campo temporale `iso_timestamp`. Il tabellone Canvas ha un export separato.
Per conservare gli oggetti usa `docker compose down` e riavvia lo stesso progetto.
L’opzione `-v` elimina anche il volume Elasticsearch.

La sessione Spark configura `es.nodes`, `es.port` e `es.nodes.wan.only`.
Dopo l'inferenza, Spark scrive le previsioni con il sink streaming Elasticsearch.

```python
output.writeStream.format("es") \
    .option("es.mapping.id", "document_id") \
    .option("es.spark.dataframe.write.null", "true") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint) \
    .trigger(processingTime="5 seconds") \
    .start(elasticsearch_index)
```

## Documenti e recupero

Spark registra nel checkpoint i micro-batch completati. Al riavvio, gli eventi
da rielaborare vengono scritti con gli stessi ID basati su topic, partizione e offset.

## Avvio e verifica

Dalla radice, con modello e storico giocatori già disponibili in
`training/artifacts/model` e `training/data/processed/player_history.json`:

```bash
docker compose config -q
docker compose up --build -d filebeat spark-streaming kibana
docker compose ps -a
docker compose logs -f --tail 30 spark-streaming
```

Il comando avvia anche Logstash, Kafka, Elasticsearch e Kibana tramite le
dipendenze Compose. `spark-ml` è il training offline e non serve in questo avvio.
`kafka-init` deve terminare con codice 0;
gli altri servizi restano attivi. Per la sola visualizzazione dei dati già
salvati basta `docker compose up -d kibana`.

Spark deve mostrare `previsioni salvate in tennis-predictions` quando riceve
eventi. Elasticsearch è accessibile su `http://localhost:9200`:

```bash
curl -s 'http://localhost:9200/_cluster/health?pretty'
curl -s 'http://localhost:9200/tennis-predictions/_count?pretty'
curl -s 'http://localhost:9200/tennis-predictions/_search?size=3&sort=timestamp:desc&pretty'
curl -s 'http://localhost:5601/api/status'
```

Apri [General Dashboard](http://localhost:5601/app/dashboards#/view/133faf11-21c3-4265-be0a-a0008b16cfa8)
e scegli un intervallo temporale che includa le previsioni salvate.

## Modificare e salvare la dashboard

Dopo aver modificato pannelli e layout in Kibana, premi **Save**. Per conservare
le modifiche nel repository, riesportare la dashboard con le sue dipendenze:

```bash
curl -fsS -X POST 'http://localhost:5601/api/saved_objects/_export' \
  -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
  --data '{"objects":[{"type":"dashboard","id":"133faf11-21c3-4265-be0a-a0008b16cfa8"}],"includeReferencesDeep":true}' \
  -o visualization/general_dashboard.ndjson
```

Per ripristinare la dashboard e la vista su un’installazione vuota:

```bash
curl -fsS -X POST 'http://localhost:5601/api/saved_objects/_import' \
  -H 'kbn-xsrf: true' --form file=@visualization/general_dashboard.ndjson
```
