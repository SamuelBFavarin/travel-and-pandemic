from google.cloud import bigquery
from google.oauth2 import service_account

credentials = service_account.Credentials.from_service_account_file('../secret.json')
client = bigquery.Client(credentials=credentials, project=credentials.project_id,)

def store_raw_data_on_bq():
    pass

def get_raw_data():

    query = """
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

    return client.query(query)

if __name__ == '__main__':

    for row in get_raw_data():
        print(row['country_name'], row['date'], row['total_new_confirmed'])
