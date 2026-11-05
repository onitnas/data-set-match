# Ingestion WebSocket → Filebeat → Logstash → Kafka

Filebeat riceve il WebSocket di API Tennis, interpreta il JSON e invia
un evento per match a Logstash tramite il protocollo Beats. Logstash
aggiunge l'identificativo dell'incontro e inoltra l'evento a Kafka.

Il blocco `filter` di Logstash conserva soltanto gli incontri `Atp Singles`, quattro Slam
e nove Masters 1000. 

## Verifica manuale sul feed reale

### 1. Controllare l'inizializzazione e aprire il consumer Kafka

Il topic viene creato automaticamente da `kafka-init`, avviato come dipendenza
di Logstash. Per controllare l'esito:

```bash
docker compose ps -a kafka-init
docker compose logs kafka-init
```

In un terminale dedicato, leggere il nome del topic dalla configurazione del
container Logstash e avviare il consumer prima di Filebeat:

```bash
tennis_topic="$(docker compose exec -T logstash printenv KAFKA_TOPIC)"
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic "$tennis_topic" --formatter-property print.key=true
```

Il consumer mostra i nuovi messaggi. Per leggere anche quelli già presenti
nel topic, aggiungere `--from-beginning`. La variabile `tennis_topic` evita
di usare un nome diverso da quello del producer.

### 2. Aprire il WebSocket reale

In un altro terminale:

```bash
docker compose up -d filebeat
docker compose logs -f filebeat logstash
```

Filebeat apre la connessione `wss://` configurata e invia i dati a Logstash.
Non servono un client WebSocket separato, un Dockerfile o un plugin WebSocket
aggiuntivo in Logstash.

Per leggere gli eventi prima dei filtri Logstash, fermare Filebeat e avviarne
temporaneamente una copia che stampi sul terminale invece di inviare a Logstash:

```bash
docker compose stop filebeat
docker compose run --rm --no-deps filebeat -e --strict.perms=false -E output.logstash.enabled=false -E output.console.enabled=true -E output.console.pretty=true -E logging.level=error
```

Questa prova apre il feed reale. Attendere un evento e controllare dove si
trovano `event_key`, `event_type_type` e `tournament_key` nel campo `match`.

### 3. Fermare l'ingestion

```bash
docker compose stop filebeat logstash kafka
```

## Contenuto degli eventi e responsabilità successive

Il programma `program` in `filebeat.yml` usa CEL: decodifica ogni messaggio
JSON, trasforma un oggetto singolo in una lista di un elemento e pubblica
un evento per ogni match nel campo `match`. 

Gli array mantengono il loro ordine. Logstash scarta heartbeat o eventi 
privi di `match.event_key`, incontri diversi dal singolare ATP e tornei 
assenti dall'elenco configurato.

Il filtro usa il controllo nativo `not in` sulla lista di chiavi presente nel
`.conf`. Una copia testuale di `match.tournament_key` viene inserita in
`@metadata` per confrontare chiavi JSON sia numeriche sia testuali, senza
modificare il payload. `@metadata` non viene pubblicato su Kafka.
Il filtro `drop_unlisted_tournament` scarta gli incontri fuori elenco.

La normalizzazione usa tre filtri nativi `mutate` consecutivi: `copy` copia
`match.event_key` in `match_id`, `convert` lo converte in `integer` e infine
un secondo `convert` lo converte in `string`. Il campo originale rimane invariato.
Gli ID numerici `12176099`, `12176099.0` e `1.2176099E7` diventano quindi
`match_id = "12176099"`. I blocchi sono separati per rispettare l'ordine delle
operazioni. Non viene usato codice Ruby nella pipeline.

La conversione si applica agli identificativi interi del provider: `integer`
tronca eventuali parti frazionarie. Il controllo finale scarta errori di
conversione e ID normalizzati che non siano stringhe di interi positivi.

Logstash aggiunge anche `event_type = match_update` e `source = api-tennis`.
`@timestamp` rappresenta l'istante di acquisizione, non l'orario del punto.
Usa `match_id` come chiave Kafka e una sola pipeline worker, con elaborazione
ordinata.

Dopo il riavvio di Logstash, la normalizzazione si applica ai nuovi eventi.
I messaggi già presenti nel topic mantengono il formato con cui furono inviati.
Con `--from-beginning` il consumer può quindi mostrare anche vecchi ID in
notazione scientifica.