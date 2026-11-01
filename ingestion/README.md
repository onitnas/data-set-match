# Ingestion WebSocket → Filebeat → Logstash → Kafka

Filebeat riceve il WebSocket di API Tennis, interpreta il JSON e invia
un evento per match a Logstash 9.5.5 tramite il protocollo Beats. Logstash
aggiunge l'identificativo dell'incontro e inoltra l'evento a Kafka.

Il blocco `filter` di Logstash conserva soltanto gli incontri `Atp Singles`
dei tornei elencati direttamente in `pipeline/logstash.conf`: i quattro Slam
e i nove Masters 1000. L'elenco iniziale coincide con quello della preparazione
del dataset, ma viene mantenuto nel `.conf`; Logstash non legge il file di training.

## Verifica manuale sul feed reale

### 1. Controllare la configurazione e avviare Logstash e Kafka

```bash
docker compose config --quiet
docker compose run --rm --no-deps filebeat test config --strict.perms=false
docker compose run --rm --no-deps logstash --config.test_and_exit
docker compose up -d logstash
docker compose run --rm --no-deps filebeat test output --strict.perms=false
```

Il primo comando deve terminare senza errori. Filebeat deve indicare
`Config OK`, Logstash `Configuration OK`; `test output` deve confermare
la connessione a `logstash:5044`. Se Logstash è ancora in avvio, ripetere
`test output`. Questi controlli non aprono il WebSocket.
`test config` non dimostra ancora che il programma CEL elabori correttamente
i messaggi: questo viene verificato con l'arrivo degli eventi reali.

### 2. Controllare l'inizializzazione e aprire il consumer Kafka

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

### 3. Aprire il WebSocket reale

In un altro terminale:

```bash
docker compose up -d filebeat
docker compose logs -f filebeat logstash
```

Filebeat apre la connessione `wss://` configurata e invia i dati a Logstash.
Non servono un client WebSocket separato, un Dockerfile o un plugin WebSocket
aggiuntivo in Logstash.

### 4. Controllare servizi e topic in caso di problemi

```bash
docker compose ps
docker compose logs --tail 100 filebeat logstash kafka
tennis_topic="$(docker compose exec -T logstash printenv KAFKA_TOPIC)"
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --describe --topic "$tennis_topic"
```

Controllare errori di autenticazione o handshake WebSocket in Filebeat,
errori della pipeline o del producer Kafka in Logstash e la presenza di un
leader per il topic. Verificare anche che `tournament_key` sia nell'elenco
configurato e che il match sia `Atp Singles`: gli altri eventi vengono scartati.
I retry WebSocket sono limitati a cinque tentativi per
gli errori considerati recuperabili. Dopo aver corretto chiave o URL nel `.env`,
ricreare Filebeat per aggiornare le variabili del container:

```bash
docker compose up -d --force-recreate filebeat
```

Se compare `UNKNOWN_TOPIC_OR_PARTITION`, controllare il nome del topic e
l'esito di `kafka-init`. Per eseguire nuovamente l'inizializzazione:

```bash
docker compose up -d kafka-init
docker compose logs kafka-init
```

Se si modifica `KAFKA_TOPIC` nel `.env`, rieseguire `docker compose up -d filebeat`
per applicare lo stesso nome al servizio di inizializzazione e a Logstash.

Per leggere gli eventi prima dei filtri Logstash, fermare Filebeat e avviarne
temporaneamente una copia che stampi sul terminale invece di inviare a Logstash:

```bash
docker compose stop filebeat
docker compose run --rm --no-deps filebeat -e --strict.perms=false -E output.logstash.enabled=false -E output.console.enabled=true -E output.console.pretty=true -E logging.level=error
```

Questa prova apre il feed reale. Attendere un evento e controllare dove si
trovano `event_key`, `event_type_type` e `tournament_key` nel campo `match`.
Non eseguire le due copie contemporaneamente: condividono la directory dati.
Terminare con `Ctrl+C`, poi ripristinare l'invio a Logstash:

```bash
docker compose up -d filebeat
```

I filtri `drop_missing_match_id` e `drop_non_atp_singles` hanno identificativi
espliciti per riconoscere il motivo degli scarti nelle statistiche Logstash.
Dopo aver aggiornato la pipeline, riavviare Logstash per caricare le modifiche.

### 5. Fermare l'ingestion

```bash
docker compose stop filebeat logstash kafka
```

## Contenuto degli eventi e responsabilità successive

Il programma `program` in `filebeat.yml` usa CEL: decodifica ogni messaggio
JSON, trasforma un oggetto singolo in una lista di un elemento e pubblica
un evento per ogni match nel campo `match`. Gli array mantengono il loro ordine.
I messaggi non JSON generano errori di elaborazione in Filebeat. Logstash
scarta heartbeat o eventi privi di `match.event_key`, incontri diversi dal
singolare ATP e tornei assenti dall'elenco configurato.

Il filtro usa il controllo nativo `not in` sulla lista di chiavi presente nel
`.conf`. Una copia testuale di `match.tournament_key` viene inserita in
`@metadata` per confrontare chiavi JSON sia numeriche sia testuali, senza
modificare il payload. `@metadata` non viene pubblicato su Kafka.
Il filtro `drop_unlisted_tournament` scarta gli incontri fuori elenco.
Dopo aver modificato la lista in `pipeline/logstash.conf`, riavviare Logstash:

```bash
docker compose restart logstash
```

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

Nonostante il nome `tennis-points`, ogni messaggio è un aggiornamento del match,
che può includere punti già ricevuti. Lo streaming dovrà individuare i nuovi
punti, gestire duplicati e correzioni e costruire le 23 feature del modello.
La riconnessione WebSocket non garantisce il recupero di aggiornamenti persi:
l'eventuale riallineamento REST va implementato nella fase successiva.

## File e riferimenti

- `filebeat.yml`: client WebSocket, decodifica JSON, output verso Logstash.
- `pipeline/logstash.conf`: input Beats sulla porta 5044 e output Kafka.
- `../docker-compose.yml`: servizi e volumi persistenti.

Riferimenti: [WebSocket API Tennis](https://api-tennis.com/documentation_websocket),
[Streaming input Filebeat](https://www.elastic.co/docs/reference/beats/filebeat/filebeat-input-streaming),
[output Logstash di Filebeat](https://www.elastic.co/docs/reference/beats/filebeat/logstash-output),
[output Kafka di Logstash](https://www.elastic.co/docs/reference/logstash/plugins/plugins-outputs-kafka).
