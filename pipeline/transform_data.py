from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from typing import List
from ingest_covid_19_data import CovidIngestion
from ingest_news_data import NyTimesIngestion


class DataTransformation:

    def __init__(self):
        self.credentials: service_account.Credentials = service_account.Credentials.from_service_account_file('../secret.json')
        self.client: bigquery.Client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.dataset_id: str = f"{self.credentials.project_id}.raw"

if __name__ == "__main__":

    covid_client = CovidIngestion()
    ny_client = NyTimesIngestion()

    for row in covid_client.list_data():
        print(row)

    # for row in ny_client.list_data():
    #     print(row)