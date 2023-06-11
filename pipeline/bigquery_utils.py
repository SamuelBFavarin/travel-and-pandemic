from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
import pandas as pd
import pandas_gbq
from typing import List
from schema import Schema

class BigQueryUtils:

    def __init__(self):
        self.credentials: service_account.Credentials = service_account.Credentials.from_service_account_file('../secrets/secret.json')
        self.client: bigquery.Client = bigquery.Client(credentials=self.credentials, project=self.credentials.project_id)

    def create_dataset_if_not_exists(self, dataset_id: str) -> bool:
        try:
            self.client.get_dataset(dataset_id)
            print("Dataset {} already exists.".format(dataset_id))
            return True
        except NotFound:
            try:
                dataset = bigquery.Dataset(f"{dataset_id}")
                dataset.location = "US"
                dataset = self.client.create_dataset(dataset, timeout=30)
                print("Created dataset {}.{}".format(self.client.project, dataset_id))
            except:
                return False

    def create_table_if_not_exists(self, table_id: str, schema: Schema) -> bool:
        try:
            # check if table already exists
            self.client.get_table(table_id)
            print("Table {} already exists.".format(table_id))
        except NotFound:
            try:
                # create table
                table = bigquery.Table(table_id, schema=schema)
                table = self.client.create_table(table)
                print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            except:
                False

    def recreate_table(self, table_id: str, schema: Schema) -> bool:
        print(f"{self.credentials.project_id}.{table_id}")
        try:
            self.client.delete_table(f"{self.credentials.project_id}.{table_id}", not_found_ok=True)
            table = bigquery.Table(f"{self.credentials.project_id}.{table_id}", schema=schema)
            table = self.client.create_table(table)
            print("Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id))
            return True
        except Exception as e:
            print(f"Error in _recreate_table: {e}")
            return False

    def store_results_by_query(self, table_id: str, query: str) -> bool:
        try:
            # add results to table
            job_config = bigquery.QueryJobConfig(destination=f"{self.credentials.project_id}.{table_id}", write_disposition="WRITE_TRUNCATE")
            query_job = self.client.query(query, job_config=job_config)
            query_job.result()
            print("Query results loaded to the table {}".format(table_id))
            return True
        except Exception as e:
            print(f"Error in _store_results_by_query: {e}")
            return False

    def store_results_via_batch(self, table_id:str, data: List[dict]) -> bool:
        try:
            # Due we are using the BigQuery without billing setup, we can't use this streaming ingestion
            # errors = self.client.insert_rows(table_ref, data)
            # an alternative was ingest the data using Pandas DataFrame
            table_ref = self.client.get_table(f"{self.credentials.project_id}.{table_id}")
            pandas_gbq.to_gbq(pd.DataFrame.from_dict(data),
                              table_id,
                              credentials=self.credentials,
                              project_id=self.credentials.project_id)
            return True
        except Exception as e:
            print(f"Error in _store_results_via_batch: {e}")
            return False

    def get_all_data(self, table_id: str) -> List:
        query_job = self.client.query(f" SELECT * FROM {table_id} ")
        return query_job.result()

    def get_count_data(self, table_id: str) -> List:
        query_job = self.client.query(f" SELECT COUNT(1) AS total FROM {table_id} ")
        return query_job.result()