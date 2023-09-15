import pendulum
import datetime

from airflow import DAG
from airflow.operators.generic_transfer import GenericTransfer

local_tz = pendulum.timezone("Asia/Jakarta")

default_args = {
    'owner': 'Aditiya Rahman',
    'depends_on_past': False,
    'start_date': datetime.datetime(2023, 9, 1, tzinfo=local_tz),
    'retries': 1,
}

dag =  DAG(
    'business_question',
    default_args=default_args,
    description='Task to answer business questions',
    schedule_interval='0 9 * * *',
    catchup=False,
)

daily_earnings_query = """
SELECT "InvoiceDate", SUM("TotalPrice")
FROM "artist_revenue"
GROUP BY "InvoiceDate"
ORDER BY "InvoiceDate"
"""

daily_earnings = GenericTransfer(
    task_id = 'insert_into_daily_earnings',
    preoperator = [
        'DROP TABLE IF EXISTS daily_earnings',
        """
        CREATE TABLE daily_earnings (
        "InvoiceDate" TIMESTAMP NOT NULL,
        "Earnings" NUMERIC(10,2) NOT NULL
        )
        """
    ],
    sql = daily_earnings_query,
    destination_table = 'daily_earnings',
    source_conn_id = 'datawarehouse',
    destination_conn_id = 'datamart',
    dag=dag,
)

most_productive_artist_query = """
SELECT "ArtistId", "ArtistName", COUNT("TrackId") AS "TotalTrack"
FROM "songs"
GROUP BY "GenreName", "ArtistId", "ArtistName"
ORDER BY "TotalTrack" DESC
"""

most_productive_artist = GenericTransfer(
    task_id = 'insert_into_most_productive_artist',
    preoperator = [
        'DROP TABLE IF EXISTS most_productive_artist',
        """
        CREATE TABLE most_productive_artist (
        "ArtistId" INT NOT NULL,
        "ArtistName" VARCHAR(120),
        "TotalTrack" INT NOT NULL
        )
        """
    ],
    sql = most_productive_artist_query,
    destination_table = 'most_productive_artist',
    source_conn_id = 'datawarehouse',
    destination_conn_id = 'datamart',
    dag=dag,
)

artist_earnings_query = """
SELECT "ArtistId", "ArtistName", SUM("TotalPrice") AS "Earnings"
FROM "artist_revenue"
GROUP BY "ArtistId", "ArtistName"
ORDER BY "Earnings" DESC
"""

artist_earnings = GenericTransfer(
    task_id = 'insert_into_artist_earnings',
    preoperator = [
        'DROP TABLE IF EXISTS artist_earnings',
        """
        CREATE TABLE artist_earnings (
        "ArtistId" INT NOT NULL,
        "ArtistName" VARCHAR(120),
        "Earnings" NUMERIC(10,2) NOT NULL
        )
        """
    ],
    sql = artist_earnings_query,
    destination_table = 'artist_earnings',
    source_conn_id = 'datawarehouse',
    destination_conn_id = 'datamart',
    dag=dag,
)

city_with_most_purchases_query = """
SELECT "City", COUNT("InvoiceId") AS "TotalPurchases"
FROM "transactions"
GROUP BY "City"
ORDER BY "TotalPurchases" DESC
"""

city_with_most_purchases = GenericTransfer(
    task_id = 'insert_into_city_with_most_purchases',
    preoperator = [
        'DROP TABLE IF EXISTS city_with_most_purchases',
        """
        CREATE TABLE city_with_most_purchases (
        "City" VARCHAR(40),
        "TotalPurchase" INT NOT NULL
        )
        """
    ],
    sql = city_with_most_purchases_query,
    destination_table = 'city_with_most_purchases',
    source_conn_id = 'datawarehouse',
    destination_conn_id = 'datamart',
    dag=dag,
)

daily_earnings >> most_productive_artist >> artist_earnings >> city_with_most_purchases