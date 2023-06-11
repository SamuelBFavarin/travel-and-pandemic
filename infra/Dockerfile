FROM python:3.10.9-slim

LABEL data_pipeline all_recipes

# INSTALL REQUIRED LIBS
RUN pip install -r requirements.txt

WORKDIR /app
COPY ./pipeline /app/

RUN python ingest_covid_19_data.py
RUN python ingest_newsdata.py
ENTRYPOINT python run_transformation.py