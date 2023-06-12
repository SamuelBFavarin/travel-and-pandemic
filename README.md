# Travel & Covid ✈️
This repository contains a data pipeline process to provide insights on COVID-19 and travel. The data pipeline performs data cleaning and transformation, and stores the data in a datalake. As a result, this pipeline generates a comprehensive result model that includes COVID-19 metrics and crucial news for each country.

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

This project is structured into several folders to organize the different parts of the data pipeline:
- The `pipeline` folder houses essential scripts that handle the ingestion and transformation of data. COVID-19 data is acquired through an external BigQuery connection, while news data is obtained via the New York Times API. During the transformation step, we merge the raw data and construct a final model that contains metrics and news about travel and COVID-19. By combining these datasets, we create a model that provides insights;
- The `infra` folder provides the files required to install and run the pipeline;
- The `doc` folder provides assets for documenting the project;
- The `secrets` folder provides all secrets required.;
- The `Makefile` provides an easy way to run all parts of the repo. For more datails, please, check the "How to run" documentation.

## Project Architecture

This diagram illustrates the architecture of this project.

![enter image description here](https://github.com/SamuelBFavarin/travel-and-pandemic/blob/main/doc/architecture.jpg?raw=true)

Our data sources come from an external BigQuery connection that provides COVID-19 data, and the New York Times, which provides news data. During the ingestion process, we execute Python scripts to consume the data from these sources and store it in a raw layer within our Data Warehouse. Following this step, we run the transformation script, which utilizes the tables in the raw layer to generate a final table of results. This final table provides metrics and news pertaining to COVID-19 and travel, offering valuable insights.

## How to execute

To run the project, you need to meet the following requirements:
-   Have Docker installed on your machine;
-   Be able to run the Makefile using the `make` command on your terminal

####  Step 1:
To clone this repo to your local machine using GitHub CLI, run the following command in your terminal:
`gh repo clone SamuelBFavarin/travel-and-pandemic`

####  Step 2:
To proceed with the setup, please create the following secret files:
- The `api_secrets.json` must contains the api key of the New York Times;
- The `secret.json` must contains the Bigquery credentials.

Please, follow the files structure in the examples on `/secrets` folder.

####  Step 3:
To run the pipeline process that ingest, load and transform, run the following command in your terminal. Please note that this process may take up to 10 minutes to complete.
`make run-pipeline`
