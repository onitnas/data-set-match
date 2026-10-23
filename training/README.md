# Download degli incontri

La fase attuale scarica gli incontri ATP dei quattro Slam e dei nove Masters
1000 elencati in `tournaments.json`, salvando un JSON per torneo e anno.
La preparazione punto per punto e il training saranno aggiunti successivamente.

## Esecuzione

Eseguire dalla radice del repository:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -r training/requirements.txt
read -rs -p 'API Tennis key: ' API_TENNIS_KEY
export API_TENNIS_KEY
.venv/bin/python -m training.fetch_matches
```

Inserire l'anno quando richiesto. La chiave viene letta dall'ambiente;
i file `.env` non vengono caricati automaticamente.

I file sono salvati in `training/data/raw/<anno>/<torneo>_<anno>.json`, con la lista
nel campo `tournament`. Una nuova esecuzione sovrascrive i file dello stesso
anno e torneo.

Sono conservati solo i record con `event_qualification` uguale a `false` o
`"False"` e con una lista `pointbypoint` non vuota. Flag mancanti, qualificazioni
e incontri senza punti sono esclusi. Lo stato Finished e la coerenza dei punti
saranno verificati nella successiva preparazione del dataset.

Chiave mancante, errori HTTP e risposte API non riuscite interrompono il
programma. I file dei tornei già scaricati rimangono disponibili.

## File

- `fetch_matches.py`: scarica, filtra e salva gli incontri.
- `tournaments.json`: chiavi, nomi e superficie dei 13 tornei.
- `requirements.txt`: dipendenze Python.

I dati in `training/data/` sono ignorati da Git. Verificare le chiavi per la
stagione scelta, specialmente per eventi che cambiano sede. Docker e Spark
saranno introdotti con il training.
