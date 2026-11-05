# Elasticsearch e Kibana

Spark carica il modello già addestrato e calcola le probabilità di vittoria
del match. Elasticsearch conserva ogni aggiornamento; Kibana interroga quei
documenti per mostrarli. La dashboard non esegue addestramento o inferenza.

## Configurazione

`tennis-predictions-template.json` definisce mapping e impostazioni dei nuovi
indici: uno shard primario, zero repliche, probabilità `double`, nomi e ID
`keyword`, conteggi `integer` e `timestamp` di tipo `date`.
`elasticsearch-init` installa il template tramite la REST API e termina.

Kibana usa la stessa versione di Elasticsearch, `9.5.5`, e lo raggiunge
su `http://elasticsearch:9200` nella rete Compose. Le porte pubblicate sono
`127.0.0.1:9200` e `127.0.0.1:5601`; l'ambiente locale ha autenticazione
disattivata. Dashboard e viste dati sono conservate nel volume Elasticsearch.

`tennis-dashboard.ndjson` è un'esportazione nativa di Kibana 9.5.5 contenente
la dashboard `tennis-live` e la vista dati `tennis-predictions-view`.
`kibana-init` aspetta Kibana, importa questi oggetti e termina. A ogni sua
esecuzione sovrascrive gli stessi oggetti con la versione nel repository;
gli eventi Elasticsearch e gli altri oggetti Kibana rimangono invariati.

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

Dalla radice, con modello e storico giocatori già disponibili in
`training/artifacts/model` e `training/data/processed/player_history.json`:

```bash
docker compose config -q
docker compose up --build -d filebeat spark-streaming kibana-init
docker compose ps -a
docker compose logs kibana-init
docker compose logs -f --tail 30 spark-streaming
```

Il comando avvia anche Logstash, Kafka, Elasticsearch e Kibana tramite le
dipendenze Compose. `spark-ml` è il training offline e non serve in questo avvio.
`kafka-init`, `elasticsearch-init` e `kibana-init` devono terminare con codice 0;
gli altri servizi restano attivi. Per la sola visualizzazione dei dati già
salvati basta `docker compose up -d kibana-init`.

Spark deve mostrare `previsioni salvate in tennis-predictions` quando riceve
eventi. Elasticsearch è accessibile su `http://localhost:9200`:

```bash
curl -s 'http://localhost:9200/_cluster/health?pretty'
curl -s 'http://localhost:9200/tennis-predictions/_count?pretty'
curl -s 'http://localhost:9200/tennis-predictions/_search?size=3&sort=timestamp:desc&pretty'
curl -s 'http://localhost:5601/api/status'
```

Aprire la dashboard:

**http://localhost:5601/app/dashboards#/view/tennis-live**

## Come leggere la dashboard

L'intervallo iniziale è **Last 7 days**, con aggiornamento automatico ogni
30 secondi. Può essere cambiato dal selettore temporale in alto. Un intervallo
senza eventi produce pannelli vuoti: non significa che i servizi siano fermi.

- **Aggiornamenti ricevuti:** numero di documenti nell'intervallo selezionato,
  non numero di punti né di chiamate al modello.
- **Incontri osservati:** numero di `match_id` distinti nello stesso intervallo.
- **Match ID:** selezionare un incontro per filtrare tutte le viste.
  La tabella permette di associare l'ID ai nomi dei giocatori.
- **Ultimo aggiornamento per incontro:** una riga per match, con punteggio,
  probabilità, vincitore previsto e stato dell'elaborazione. Mostra fino a
  100 incontri; "ultimo" si riferisce ai dati disponibili nell'intervallo,
  non certifica che la partita sia ancora in corso.
- **Andamento delle probabilità:** due serie, vittoria di A e vittoria di B,
  per ciascun incontro. Selezionare un match per leggere il suo andamento.
  Il grafico usa gli stati con probabilità disponibili, senza mediare tra
  incontri, e visualizza fino a 5.000 aggiornamenti (due valori per evento).
  La legenda indica A/B, nome del giocatore e ID del match. Ridurre l'intervallo
  per esplorare periodi più densi.
- **Ultimi 200 aggiornamenti:** registro degli eventi più recenti, con stato,
  motivo di esclusione, partizione e offset Kafka.

A è il primo giocatore dell'API e B il secondo. Le percentuali riguardano
la vittoria finale del **match**, non del prossimo punto. `predicted` indica
un nuovo input al modello; `unchanged` una previsione riutilizzata. Un evento
senza previsione mantiene probabilità nulle anche nella tabella dell'ultimo
aggiornamento: non eredita quelle di un evento precedente.

## Modificare e salvare la dashboard

Usare **Edit** in Kibana per modificare pannelli e layout. Per conservare
le modifiche nel repository, riesportare la dashboard con le sue dipendenze:

```bash
curl -fsS -X POST 'http://localhost:5601/api/saved_objects/_export' \
  -H 'kbn-xsrf: true' -H 'Content-Type: application/json' \
  --data '{"objects":[{"type":"dashboard","id":"tennis-live"}],"includeReferencesDeep":true,"excludeExportDetails":true}' \
  -o visualization/tennis-dashboard.ndjson
```

Esportare prima di rieseguire `kibana-init`, altrimenti viene reimportata la
versione precedente del repository. Il file contiene configurazione e query,
non le partite o le previsioni. Per importarlo manualmente:

```bash
curl -fsS -X POST 'http://localhost:5601/api/saved_objects/_import?overwrite=true' \
  -H 'kbn-xsrf: true' --form file=@visualization/tennis-dashboard.ndjson
```
