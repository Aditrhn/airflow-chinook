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
    'songs',
    default_args=default_args,
    description='Task to create and insert data into songs table',
    schedule_interval='0 7 * * */3',
    catchup=False,
)

insert_query = """
SELECT a."ArtistId", 
       a."Name", 
       b."AlbumId",
       b."Title",
       c."TrackId", 
       c."Name", 
       c."UnitPrice",
       c."Bytes",
       d."GenreId",
       d."Name"
FROM "Artist" a
JOIN "Album" b
ON a."ArtistId" = b."ArtistId"
JOIN "Track" c
ON b."AlbumId" = c."AlbumId"
JOIN "Genre" d
ON c."GenreId" = d."GenreId"
"""

insert_operator = GenericTransfer(
    task_id = 'insert_into_songs',
    preoperator = [
        "DROP TABLE IF EXISTS songs",
        """
        CREATE TABLE songs (
        "ArtistId" INT NOT NULL,
        "ArtistName" VARCHAR(120),
        "AlbumId" INT NOT NULL,
        "AlbumTitle" VARCHAR(160) NOT NULL,
        "TrackId" INT NOT NULL,
        "TrackName" VARCHAR(200) NOT NULL,
        "TrackUnitPrice" NUMERIC(10,2) NOT NULL,
        "TrackBytes" INT,
        "GenreId" INT NOT NULL,
        "GenreName" VARCHAR(120)
        )
        """
    ],
    sql = insert_query,
    destination_table = 'songs',
    source_conn_id = 'postgres',
    destination_conn_id = 'datawarehouse',
    dag=dag,
)

insert_operator