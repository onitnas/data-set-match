FROM python:3.13-slim
WORKDIR /app

COPY training/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir "pyspark[ml]==4.2.0"

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

COPY training/train_model.py ./training/train_model.py

CMD ["python", "-m", "training.train_model"]

