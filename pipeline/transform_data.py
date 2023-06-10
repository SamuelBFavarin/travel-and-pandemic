from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from typing import List
from ingest_covid_19_data import CovidIngestion
from ingest_news_data import NyTimesIngestion


class DataTransformation:

    def __init__(self):
        self.credentials: service_account.Credentials = service_account.Credentials.from_service_account_file('../secrets/secret.json')
        self.client: bigquery.Client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.dataset_id: str = f"{self.credentials.project_id}.raw"


    def _get_ingestion_query(self) -> str:
        return f"""
            SELECT
                covid.country_name,
                covid.date,
                covid.total_new_confirmed,
                covid.total_new_deceased,
                covid.total_new_persons_vaccinated,
                news.abstract                           AS post_1_abstract,
                news.author                             AS post_1_author,
                news.word_count                         AS post_1_word_count,
                news.published_at                       AS post_1_published_at
            FROM `{self.dataset_id}.covid_2020` AS covid
            LEFT JOIN `{self.dataset_id}.news_2020` AS news
            ON covid.country_name = news.country_name AND
                covid.date = DATE(news.published_at)
            WHERE news.abstract IS NOT NULL
            LIMIT 1000;
        """

    def transform(self):
        query_job = self.client.query(self._get_ingestion_query())
        return query_job.result()

if __name__ == "__main__":

    covid_client = CovidIngestion()
    ny_client = NyTimesIngestion()

    client = DataTransformation()
    client.transform()

    for row in client.transform():
        print(row)
