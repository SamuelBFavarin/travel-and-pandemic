from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from typing import List
from ingest_covid_19_data import CovidIngestion
from ingest_news_data import NyTimesIngestion

from schema import Schema



class DataTransformation:

    def __init__(self):
        self.credentials: service_account.Credentials = service_account.Credentials.from_service_account_file('../secrets/secret.json')
        self.client: bigquery.Client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.dataset_id: str = f"{self.credentials.project_id}.raw"
        self.result_dataset_id: str = f"{self.credentials.project_id}.samuel_favarin"

    def _get_query(self) -> str:
        return f"""
            WITH cte_news AS (
                SELECT
                    news.country_name,
                    news.published_at,
                    news.snippet,
                    news.abstract,
                    news.author,
                    news.word_count,
                    ROW_NUMBER() OVER (PARTITION BY news.country_name, DATE(news.published_at) ORDER BY news.published_at DESC) AS row_num
                FROM `{self.dataset_id}.news_2020` AS news
            )

            SELECT
                covid.country_name                        AS country_name,
                covid.date                                AS date,
                covid.total_new_confirmed                 AS total_new_confirmed,
                covid.total_new_deceased                  AS total_new_deceased,
                covid.total_new_persons_vaccinated        AS total_new_persons_vaccinated,
                news_1.abstract                           AS news_1_abstract,
                news_1.snippet                            AS news_1_snippet,
                news_1.author                             AS news_1_author,
                news_1.word_count                         AS news_1_word_count,
                news_1.published_at                       AS news_1_published_at,
                news_2.abstract                           AS news_2_abstract,
                news_2.snippet                            AS news_2_snippet,
                news_2.author                             AS news_2_author,
                news_2.word_count                         AS news_2_word_count,
                news_2.published_at                       AS news_2_published_at,
                news_3.abstract                           AS news_3_abstract,
                news_3.snippet                            AS news_3_snippet,
                news_3.author                             AS news_3_author,
                news_3.word_count                         AS news_3_word_count,
                news_3.published_at                       AS news_3_published_at

            FROM `{self.dataset_id}.covid_2020` AS covid
            LEFT JOIN `cte_news` AS news_1
                ON covid.country_name = news_1.country_name AND
                    covid.date = DATE(news_1.published_at) AND
                        news_1.row_num = 1
            LEFT JOIN `cte_news` AS news_2
                ON covid.country_name = news_2.country_name AND
                    covid.date = DATE(news_2.published_at) AND
                        news_2.row_num = 2
            LEFT JOIN `cte_news` AS news_3
                ON covid.country_name = news_3.country_name AND
                    covid.date = DATE(news_3.published_at) AND
                        news_3.row_num = 3;
        """

    def _create_dataset_if_not_exists(self, table_id: str):

        try:
            # check if table already exists
            self.client.get_dataset(self.result_dataset_id)
            print("Dataset {} already exists.".format(self.result_dataset_id))

        except NotFound:
            # create dataset
            dataset = bigquery.Dataset(f"{self.result_dataset_id}")
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, timeout=30)
            print("Created dataset {}.{}".format(self.client.project, self.result_dataset_id))

    def _create_table_if_not_exists(self, table_id: str):

        try:
            # check if table already exists
            self.client.get_table(table_id)
            print("Table {} already exists.".format(table_id))

        except NotFound:
            # create table
            table = bigquery.Table(table_id, schema=Schema.get_result_schema())
            table = self.client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def _store_results(self, table_id: str):

        # add results to table
        job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition="WRITE_TRUNCATE")
        query_job = self.client.query(self._get_query(), job_config=job_config)
        query_job.result()
        print("Query results loaded to the table {}".format(table_id))

    def list_data(self):
        table_id = f"{self.result_dataset_id}.result"
        query_job = self.client.query(f" SELECT COUNT(*) FROM {table_id} ")
        return query_job.result()

    def transform(self):

        table_id = f"{self.result_dataset_id}.result"

        self._create_dataset_if_not_exists(table_id)
        self._create_table_if_not_exists(table_id)
        self._store_results(table_id)




if __name__ == "__main__":

    covid_client = CovidIngestion()
    ny_client = NyTimesIngestion()

    client = DataTransformation()
    client.transform()

    for row in client.list_data():
        print(f"{row.country_name}, {row.date}, {row.news_1_published_at}, {row.news_2_published_at}, {row.news_3_published_at}")
