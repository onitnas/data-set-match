FROM python:3.13-slim
WORKDIR /app

COPY training/requirements.txt ./training/requirements.txt

RUN pip install --no-cache-dir -r ./training/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

COPY training/*.py ./training/
COPY training/config/ ./training/config/

CMD ["python", "-m", "training.train_model"]

