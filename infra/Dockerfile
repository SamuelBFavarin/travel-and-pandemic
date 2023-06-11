FROM python:3.10.9-slim

LABEL data_pipeline travel_covid

WORKDIR /app
COPY ../ /app/

# INSTALL REQUIRED LIBS
RUN pip install -r ./infra/requirements.txt

RUN python ./pipeline/ingest_covid_19_data.py
RUN python ./pipeline/ingest_news_data.py
ENTRYPOINT python pipeline/transform_data.py