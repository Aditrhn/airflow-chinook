import pendulum
import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

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

daily_earnings = PostgresOperator(
    task_id = 'daily_earnings',
    postgres_conn_id = 'datawarehouse',
    sql = """
    SELECT SUM("DailyEarnings") as "TotalEarning"
    FROM (
            SELECT "InvoiceDate", SUM("TotalPrice") as "DailyEarnings"
            FROM "artist_revenue"
            GROUP BY "InvoiceDate"
    ) as a
    """,
    dag=dag,
)

most_productive_artist = PostgresOperator(
    task_id = 'most_productive_artist',
    postgres_conn_id = 'datawarehouse',
    sql = """
    SELECT "ArtistId", "ArtistName", COUNT("TrackId") as "TotalTrack"
    FROM "songs"
    GROUP BY "GenreName", "ArtistId", "ArtistName"
    ORDER BY "TotalTrack" DESC
    """,
    dag=dag,
)

artist_earnings = PostgresOperator(
    task_id = 'artist_earnings',
    postgres_conn_id = 'datawarehouse',
    sql = """
    SELECT "ArtistId", "ArtistName", SUM("TotalPrice") as "Earnings"
    FROM "artist_revenue"
    GROUP BY "ArtistId", "ArtistName"
    ORDER BY "Earnings" DESC
    """,
    dag=dag,
)

city_with_most_purchases = PostgresOperator(
    task_id = 'city_with_most_purchases',
    postgres_conn_id = 'datawarehouse',
    sql = """
    SELECT "City", COUNT("InvoiceId") as "TotalPurchases"
    FROM "transactions"
    GROUP BY "City"
    ORDER BY "TotalPurchases" DESC
    """,
    dag=dag,
)

daily_earnings >> most_productive_artist >> artist_earnings >> city_with_most_purchases