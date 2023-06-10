from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from typing import List


class CovidIngestion:

    def __init__(self):
        self.credentials: service_account.Credentials = service_account.Credentials.from_service_account_file('../secret.json')
        self.client: bigquery.Client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)
        self.dataset_id: str = f"{self.credentials.project_id}.raw"

    @staticmethod
    def _get_schema() -> List[bigquery.SchemaField]:
        return  [
            bigquery.SchemaField("country_name", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("country_code", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("date", "DATE", mode="REQUIRED"),
            bigquery.SchemaField("total_new_confirmed", "INTEGER"),
            bigquery.SchemaField("total_new_deceased", "INTEGER"),
            bigquery.SchemaField("total_new_persons_vaccinated", "INTEGER"),
        ]

    @staticmethod
    def _get_ingestion_query() -> str:
        return """
            SELECT
                country_name,
                country_code,
                date,
                SUM(new_confirmed) AS total_new_confirmed,
                SUM(new_deceased) AS total_new_deceased,
                SUM(new_persons_vaccinated) AS total_new_persons_vaccinated
            FROM `bigquery-public-data.covid19_open_data.covid19_open_data`
            WHERE
                country_name IN ("Spain", "Greece", "Turkey", "Italy", "Egypt", "Portugal") AND
                date BETWEEN '2020-01-01' AND '2020-12-31'
            GROUP BY
                country_name,
                country_code,
                date
            ORDER BY date;
        """

    def _create_dataset_if_not_exists(self, table_id: str):

        try:
            # check if table already exists
            self.client.get_dataset(self.dataset_id)
            print("Dataset {} already exists.".format(self.dataset_id))

        except NotFound:
            # create dataset
            dataset = bigquery.Dataset(f"{self.credentials.project_id}.{self.dataset_id}")
            dataset.location = "US"
            dataset = self.client.create_dataset(dataset, timeout=30)
            print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))

    def _create_table_if_not_exists(self, table_id: str):

        try:
            # check if table already exists
            self.client.get_table(table_id)
            print("Table {} already exists.".format(table_id))

        except NotFound:
            # create table
            table = bigquery.Table(table_id, schema=self._get_schema())
            table = self.client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))

    def _store_results(self, table_id: str):

        # add results to table
        job_config = bigquery.QueryJobConfig(destination=table_id, write_disposition="WRITE_TRUNCATE")
        query_job = self.client.query(self._get_ingestion_query(), job_config=job_config)
        query_job.result()
        print("Query results loaded to the table {}".format(table_id))


    def list_data(self):
        table_id = f"{self.dataset_id}.covid_2020"
        query_job = self.client.query(f" SELECT * FROM {table_id} ")
        return query_job.result()

    def run_ingestion(self):

        table_id = f"{self.dataset_id}.covid_2020"

        self._create_dataset_if_not_exists(table_id)
        self._create_table_if_not_exists(table_id)
        self._store_results(table_id)


if __name__ == '__main__':

    covid_ingestion = CovidIngestion()
    # covid_ingestion.run_ingestion()

    covid_ingestion.list_data()
