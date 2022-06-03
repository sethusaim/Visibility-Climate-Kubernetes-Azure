FROM python:3.7-slim-bullseye

WORKDIR /app

COPY . /app

ARG AZURE_CONN_STR

ENV AZURE_CONN_STR=$AZURE_CONN_STR

RUN pip3 install --upgrade pip && pip3 install -r requirements.txt

CMD [ "python3","-W ignore run.py" ]