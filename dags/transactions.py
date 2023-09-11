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
    'transactions',
    default_args=default_args,
    description='Task to create and insert data into transactions table',
    schedule_interval='0 */3 * * *',
    catchup=False,
)

insert_query = """
SELECT a."TrackId",
       b."InvoiceId",
       d."CustomerId",
       c."InvoiceDate",
       c."Total",
       concat(d."FirstName", ' ', d."LastName"),
       d."Address",
       d."City",
       d."State",
       d."Country",
       d."PostalCode",
       d."Email"
FROM "Track" a 
JOIN "InvoiceLine" b
ON  a."TrackId" = b."TrackId"
JOIN "Invoice" c
ON  b."InvoiceId" = c."InvoiceId"
JOIN "Customer" d
ON c."CustomerId" = d."CustomerId"
"""

insert_operator = GenericTransfer(
    task_id = 'insert_into_transactions',
    preoperator = [
        "DROP TABLE IF EXISTS transactions",
        """
        CREATE TABLE transactions (
        "TrackId" INT NOT NULL,
        "InvoiceId" INT NOT NULL,
        "CustomerId" INT NOT NULL,
        "InvoiceDate" TIMESTAMP NOT NULL,
        "Total" NUMERIC(10,2) NOT NULL,
        "FullName" VARCHAR(50) NOT NULL,
        "Address" VARCHAR(70),
        "City" VARCHAR(40),
        "State" VARCHAR(40),
        "Country" VARCHAR(40),
        "PostalCode" VARCHAR(10),
        "Email" VARCHAR(60) NOT NULL
        )
        """
    ],
    sql = insert_query,
    destination_table = 'transactions',
    source_conn_id = 'postgres',
    destination_conn_id = 'datawarehouse',
    dag=dag,
)

insert_operator