FROM python:3.13-slim
WORKDIR /app

RUN pip install --no-cache-dir "pyspark[ml]==4.2.0"

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

COPY streaming/*.py ./streaming/
COPY training/tennis_rules.py ./training/tennis_rules.py
COPY training/historical_features.py ./training/historical_features.py
COPY training/player_rankings.py ./training/player_rankings.py
COPY training/config/tournaments.json ./training/config/tournaments.json

CMD ["spark-submit", "--master", "local[2]", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0", "streaming/stream_matches.py"]
