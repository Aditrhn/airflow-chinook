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
    'artis_revenue',
    default_args=default_args,
    description='Task to create and insert data into artist_revenue table',
    schedule_interval='0 7 * * *',
    catchup=False,
)

insert_query = """
SELECT a."ArtistId", 
       a."Name", 
       b."Title", 
       c."Name", 
       (d."Quantity" * d."UnitPrice"),
       d."InvoiceLineId",
       e."InvoiceId",
       e."InvoiceDate"
FROM "Artist" a
JOIN "Album" b
ON a."ArtistId" = b."ArtistId"
JOIN "Track" c
ON b."AlbumId" = c."AlbumId"
JOIN "InvoiceLine" d
ON c."TrackId" = d."TrackId"
JOIN "Invoice" e
ON d."InvoiceId" = e."InvoiceId"
"""

insert_operator = GenericTransfer(
    task_id = 'insert_into_artist_revenue',
    preoperator = [
        "DROP TABLE IF EXISTS artist_revenue",
        """
        CREATE TABLE artist_revenue (
        "ArtistId" INT NOT NULL,
        "ArtistName" VARCHAR(120),
        "AlbumTitle" VARCHAR(160) NOT NULL,
        "TrackName" VARCHAR(200) NOT NULL,
        "TotalPrice" NUMERIC(10,2) NOT NULL,
        "InvoiceLineId" INT NOT NULL,
        "InvoiceId" INT NOT NULL,
        "InvoiceDate" TIMESTAMP NOT NULL)
        """
    ],
    sql = insert_query,
    destination_table = 'artist_revenue',
    source_conn_id = 'postgres',
    destination_conn_id = 'datawarehouse',
    dag=dag,
)

insert_operator