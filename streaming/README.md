# Previsione live del vincitore del match

La pipeline è **WebSocket → Filebeat → Logstash → Kafka → Spark Structured
Streaming → Spark ML → console**. Il servizio carica il Gradient Boosted Trees
salvato in `training/artifacts/model/pipeline`, con **21 feature senza momentum**,
e restituisce la probabilità di vittoria finale (`target=match_winner`).
Il servizio non addestra il modello e non chiama API. Elasticsearch e Kibana
sono il passaggio successivo.

## Flusso di un micro-batch

1. Spark legge Kafka e interpreta il JSON con uno schema esplicito.
2. `foreachBatch` ordina gli eventi per partizione e offset e li legge una volta
   nel driver con `toLocalIterator()`.
3. Ogni snapshot viene validato autonomamente: set da `event_final_result`,
   game da `scores`, punti da `event_game_result`, battitore da `event_serve`.
4. Il driver costruisce le 21 feature unendo punteggio, superficie, ranking e
   storico disponibile prima della giornata del match.
5. Se le feature sono identiche all'ultimo input del match, riutilizza la
   previsione. Gli input nuovi diventano un DataFrame passato a
   `PipelineModel.transform()`. Non servono join o UDF Python.
6. Cache e risultati del batch vengono salvati insieme. La console mostra
   fino a dieci righe; tutte le righe vengono comunque elaborate.

Il trigger richiede un batch ogni cinque secondi; l'elaborazione può richiedere
più tempo. Spark gestisce sorgente, micro-batch, offset e inferenza ML. La logica
applicativa rimane nel driver locale: non è uno stato distribuito e memoria e
lavoro crescono con il carico. Gli eventi di un match devono continuare a usare
la stessa chiave Kafka e partizione; non esiste un ordine globale degli offset
fra partizioni.

| Modulo | Responsabilità |
| --- | --- |
| `stream_matches.py` | Sessione Spark, schema JSON, Kafka e avvio della query. |
| `predict_matches.py` | Coordinamento, confronto delle feature, inferenza e output. |
| `match_state.py` | Validazione del punteggio corrente ricevuto dall'API. |
| `match_state_store.py` | Cache delle previsioni e recupero dell'ultimo batch. |
| `match_features.py` | Orientamento A/B e costruzione delle 21 feature. |
| `player_history.py` | Conteggi storici e aggiornamenti dei match conclusi. |

Dal training si riutilizzano `tennis_rules.py` e `player_rankings.py`, oltre
agli artefatti e ai dati preparati. Dockerfile e Compose mantengono questi
input in sola lettura; il volume dei checkpoint rimane scrivibile.

## Cosa è cambiato senza momentum

La previsione richiede il punteggio corrente, non la sequenza degli ultimi punti.
Non si conservano ultimo vincitore di punto, serie consecutive, vecchi game o
conteggi di punti live. Non si ricostruiscono punti mancanti per arrivare allo
snapshot corrente e non occorre attendere che `pointbypoint` raggiunga il riepilogo.
Un primo messaggio ricevuto a partita iniziata può già produrre una previsione.

Restano i controlli su formato, set precedenti, game, vantaggi, tie-break e
battitore. Uno snapshot incoerente ha probabilità `null` e un motivo esplicito.
Una correzione successiva viene valutata normalmente, anche con lo stesso score.
Gli incontri conclusi non producono nuove previsioni e rimuovono la propria cache.

Se `event_serve` manca in un game regolare, il battitore può essere letto dal
`player_served` dell'unico game aperto in `pointbypoint` con set e numero esatti.
Non si usano game precedenti o conclusi. Nel tie-break questo recupero non è
applicabile, perché il servizio cambia all'interno dello stesso game.

`pointbypoint` rimane inoltre nello schema JSON perché i punti dei match
conclusi servono a calcolare le statistiche storiche. Un risultato finale può
quindi essere riconosciuto anche quando il suo dettaglio non permette di
aggiornare lo storico: il mancato aggiornamento viene indicato nei log.
La ricostruzione dei punti osservati/inferiti rimane necessaria nella
preparazione offline e nei conteggi storici, non nell'inferenza dello score live.

## Stato necessario e recupero

Il checkpoint Spark conserva gli offset Kafka; non rende persistente una cache
Python. `<checkpoint>/match_states.json`, formato versione 2, conserva soltanto:

- ultimo ID del batch e relativi offset;
- ultima lista delle 21 feature e previsione per ciascun match in cache;
- output dell'ultimo batch, necessario per ripeterlo;
- impronta della pipeline e del suo schema, per invalidare le vecchie previsioni.

Ogni snapshot è autonomo: non si salva una seconda copia del punteggio o una
sequenza dei punti. Il vecchio formato viene letto per recuperare batch e
offset, ignorando i campi ormai inutili. La cache del modello precedente non
viene riutilizzata perché l'impronta è diversa. Al primo batch completato si
salva il formato nuovo.

Gli aggiornamenti rimangono locali al batch fino al completamento dell'inferenza.
Si scrive un file temporaneo e si sostituisce il JSON precedente. Un errore del
modello lascia invariato il salvataggio precedente. Se il processo termina dopo
il salvataggio applicativo e prima del commit degli offset Spark, il replay
restituisce l'output già salvato senza ripetere l'inferenza. Offset diversi o
batch precedenti vengono rifiutati.

Il servizio usa un solo driver/query per quel checkpoint. La console può
ristampare un batch ripetuto: non è un archivio con consegna esattamente una volta.
Al riavvio Spark riprende dagli offset salvati; `startingOffsets=earliest` vale
solo per un checkpoint nuovo. Non occorre eliminare il volume per riavviare.

## Compatibilità e storico

All'avvio si verificano target, fase `live`, ordine delle 21 feature e anno
del prior storico contro `schema.json`. Schemi di dataset e modello sono alla
versione 4; lo storico giocatori usa il formato 2 con `training_year` esplicito.

- A è sempre il primo giocatore dell'API (indice 0), B il secondo (indice 1).
  I modelli con un orientamento diverso vengono rifiutati: occorre rigenerare
  dataset e modello.
- `0/15/30/40/A` corrispondono a `0/1/2/3/4`; nel tie-break si usano conteggi dedicati.
- La superficie viene da `training/config/tournaments.json`; il formato segue
  `training/tennis_rules.py`: 5 set per gli Slam, 3 per gli altri tornei configurati.
- Il ranking è quello della stagione precedente; assente significa `0` con flag missing `1`.
- Lo storico consulta solo giorni precedenti (`previous_calendar_days_only`).
  Il prior globale cresce nel 2024 e poi resta congelato; serve ai giocatori senza storico.

`player_history.json` contiene i cumulativi esportati offline. Lo streaming non
monta né rilegge i raw. I nuovi incontri conclusi e decodificabili vengono
salvati in `<checkpoint>/player-history/`: un replay non li conta due volte e
una correzione sostituisce i conteggi. Sono consultabili dal giorno successivo.
Gli ID già inclusi nella base offline vengono ignorati. Anche rileggendo vecchi
eventi, date future non entrano nelle loro statistiche.

Resta il filtro preesistente: il dataset offline richiede qualificazione
esplicitamente falsa; il live esclude quella esplicitamente vera e marca il
campo assente come `unknown`. `unknown` non certifica il tabellone principale.

## Output

L'output mostra identità A/B, punteggio `api_score`, set/game, probabilità,
vincitore previsto, validità dello stato e posizione Kafka. Non contiene momentum.
`probability[1]` è la probabilità di vittoria di A; quella di B è il complemento.

| `prediction_status` | Significato |
| --- | --- |
| `predicted` | Input utilizzabile diverso dall'ultimo input del match. |
| `unchanged` | Le 21 feature sono uguali: previsione riutilizzata. |
| `invalid_state` | Punteggio, battitore o campi necessari non utilizzabili. |
| `finished` | Match concluso: nessuna nuova previsione. |
| `excluded` | Tipo incontro, torneo o qualificazione esclusi. |
| `invalid` | JSON o identificativi non validi. |

Un input diverso può produrre la stessa probabilità: la deduplicazione confronta
le feature, non il valore della previsione. `parse_ok` riguarda il JSON;
`state_ok` il punteggio; `state_source=api_score` indica la fonte dello stato.
Le righe senza previsione hanno `skip_reason` e probabilità `null`.

## Avvio e regressione

Dalla radice, se manca lo storico esportato:

```bash
.venv/bin/python -m training.prepare_dataset --history-only
```

Avvio:

```bash
docker compose up --build -d filebeat spark-streaming
docker compose logs -f spark-streaming
```

Fermare Spark prima di modificare sorgenti montati o sostituire il modello;
riavviarlo dopo mantenendo il checkpoint. Il processo carica la pipeline una
volta all'avvio.

```bash
docker compose stop spark-streaming
# Applicare le modifiche, poi:
docker compose up -d spark-streaming
```

Test con libreria standard:

```bash
python -m unittest streaming.test_streaming.MatchStateTests -v
```

Suite con modello e Kafka reali, con Spark già avviato:

```bash
docker compose exec -T -e TENNIS_SPARK_TESTS=1 -e TENNIS_KAFKA_TESTS=1 spark-streaming \
  spark-submit --master 'local[1]' --driver-memory 512m \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0 streaming/test_streaming.py
```

La suite usa directory temporanee, non pubblica messaggi e non modifica il
modello o il checkpoint principale. Il test Kafka richiede un topic con dati
utili di più incontri. Senza i flag relativi, i test Spark/Kafka vengono saltati.

La suite verifica 286 snapshot con vantaggi, cambi game/set e tie-break
7/10, assenza di pointbypoint, snapshot incoerenti e correttivi, ranking mancanti,
limite temporale dello storico, deduplicazione, errori dell'inferenza, riavvio,
replay e vecchio formato dei salvataggi. Verifica inoltre che lo storico
esportato dal training produca gli stessi tassi nel live e che A/B mantengano
l'ordine dell'API indipendentemente dal battitore.

Nella verifica del 4 novembre 2026 sono passati tutti i 20 test; Kafka ha fornito 419
eventi reali e la query è ripartita dal checkpoint temporaneo senza rileggere
gli offset già elaborati. Il servizio principale conserva il proprio checkpoint.

Durante la prova live il modello ha prodotto probabilità per Bellucci–Rinderknech
e Halys–Tiafoe; gli aggiornamenti con feature identiche hanno riutilizzato la
previsione (`unchanged`). Il modello installato usa lo schema 4 e lo storico 2.
