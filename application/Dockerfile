FROM python:3.7-slim-bullseye

WORKDIR /app

COPY . /app 

ARG AZURE_CONN_STR

ARG KFP_HOST

ENV AZURE_CONN_STR=${AZURE_CONN_STR} 

ENV KFP_HOST=${KFP_HOST}

RUN pip install --upgrade pip && pip install -r requirements.txt

CMD ["python","main.py"]