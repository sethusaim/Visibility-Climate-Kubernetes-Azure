FROM python:3.7-slim-bullseye

WORKDIR /app

COPY . /app

ARG AZURE_CONN_STR

ENV AZURE_CONN_STR=$AZURE_CONN_STR

RUN pip install --upgrade pip && pip install -r requirements.txt

CMD [ "python","run.py" ]