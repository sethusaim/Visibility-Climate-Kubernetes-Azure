FROM python:3.7-slim-bullseye

COPY . /app

WORKDIR /app

ARG AZURE_CONN_STR

ARG MLFLOW_TRACKING_URI

ARG MLFLOW_TRACKING_USERNAME

ARG MLFLOW_TRACKING_PASSWORD

ENV AZURE_CONN_STR=$AZURE_CONN_STR

ENV MLFLOW_TRACKING_URI $MLFLOW_TRACKING_URI

ENV MLFLOW_TRACKING_USERNAME $MLFLOW_TRACKING_USERNAME

ENV MLFLOW_TRACKING_PASSWORD $MLFLOW_TRACKING_PASSWORD

RUN pip install --upgrade pip && pip install -r requirements.txt

CMD ["python","run.py"]