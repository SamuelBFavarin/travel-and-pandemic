# Travel & Covid ✈️
This repository contains a data pipeline process to provide information about covid and travel. The data pipeline processes, performs data cleaning and transformation, and stores the data in a datalake. As the result, this pipeline will provide a model with covid metrics and most important news.

## File Structure

    travel-and-pandemic/
    ├── infra/
    │   ├── Dockerfile
    │   ├── requirements.txt
    ├── pipeline/
    |	├── __init__.py
    │   ├── bigquery_utils.py
    │   ├── ingest_covid_19_data.py
    │   ├── ingest_news_data.py
    │   ├── schema.py
    │   ├── transform_data
    ├── secrets/
    │   ├── api_secret.example.json
    │   ├── api_secret.json
    │   ├── secret.example.json
    │   ├── secret.json
    ├── doc/
    │   ├── architecture.jpg
    ├── .gitignore
    ├── Makefile
    └── README.md

This project is structured into several folders to organize the different parts of the data pipelineI:
- The `pipeline` folder contains the scripts responsible for ingesting and transforming the data. The covid data is ingested by a external BigQuery connection, and the news data, is ingested via New York Times API. After all the transformation, the data is  storage on BigQuery
- The `infra` folder provides the files required to install and run the pipeline;
- The `doc` folder provides assets for documenting the project;
- The `secrets` folder provides all secrets required.
- The `Makefile` provides an easy way to run all parts of the repo. For more datails, please, check the "How to run" documentation.

## Project Architecture

This diagram illustrates the architecture of this project.

![enter image description here](https://github.com/SamuelBFavarin/travel-and-pandemic/blob/main/doc/architecture.jpg?raw=true)

## How to execute

To run the project, you need to meet the following requirements:
-   Have Docker installed on your machine;
-   Be able to run the Makefile using the `make` command on your terminal

####  Step 1:
To clone this repo to your local machine using GitHub CLI, run the following command in your terminal:
`gh repo clone SamuelBFavarin/travel-and-pandemic`

####  Step 2:
You must to setup the secrets files, the `api_secrets.json` contains the api key of the New York Times. The `secret.json` contains the Bigquery credentials.

####  Step 3:
To run the pipeline process that ingest, load and transform, run the following command in your terminal. Please note that this process may take up to 10 minutes to complete.
`make run-pipeline`
