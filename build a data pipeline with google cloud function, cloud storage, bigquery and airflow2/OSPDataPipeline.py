import json
from datetime import timedelta, datetime

from airflow import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator


from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.contrib.operators.bigquery_check_operator import BigQueryCheckOperator

from airflow.macros import ds_format, ds_add


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,    
    'start_date': datetime(2024, 8, 24),
    'email': ['airflow@airflow.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Set Schedule: Run pipeline once a day. 
# Use cron to define exact time. Eg. 8:15am would be "15 08 * * *"
schedule_interval = "32 15 * * *"

# Define DAG: Set ID and assign default args and schedule interval
with DAG(
    'OSPDataPipeline',
    start_date=datetime(2024, 8, 24),
    schedule_interval=schedule_interval,
    default_args=default_args,
    max_active_runs=1,
    template_searchpath="/opt/airflow/dags/sql",
    catchup=True,
) as dag:

    # Config variables
    HTTP_CONN_ID = "puton_conn_id"
    BQ_CONN_ID = "my_gcp_conn"
    BQ_PROJECT = "pigpig-project-432914"
    BQ_DATASET = "pigpigdata"

    ## Task 1: trigger the cloud function to fetch crime data  
    trigger_cloud_function = SimpleHttpOperator(
        task_id='trigger_cloud_function',
        http_conn_id='puton_conn_id', 
        endpoint='<cloud-function-endpoint>/scraping-function-001/?date={{ ds }}',
        headers={"Content-Type": "application/json"},
        method='POST',
        response_check=lambda response: response.status_code == 200
    )

    # ## Task 2: load the downloaded csv into the BigQuery table
    load_csv_to_bigquery = GCSToBigQueryOperator(
            task_id='load_csv_to_bigquery',
            gcp_conn_id='my_gcp_conn',
            bucket='puton_bucket_002',  # Replace with your GCS bucket name
            source_objects=['tmp/public_estate_geo.csv'],  # Replace with the path to your CSV file in the bucket
            destination_project_dataset_table='{0}.{1}.osp_public_estate_geo_raw'.format(
                BQ_PROJECT, BQ_DATASET
            ),  
            create_disposition='CREATE_IF_NEEDED',  # Create the table if it doesn't exist
            write_disposition='WRITE_TRUNCATE',  # Overwrite the table if it already exists
            skip_leading_rows=1,  # Skip the header row
            source_format='CSV',
            autodetect=True,  # Automatically detect the schema from the CSV file
        )

    # ## Task 3: fetch and clean the raw data from the insert the data to the dataset
    cleaning_OSP_data_insert = BigQueryOperator(
        task_id='cleaning_OSP_data_insert',
        sql='''
            SELECT 
                string_field_0 AS name,
                SPLIT(REPLACE(REPLACE(REPLACE(string_field_1, '[', ''), ']', ''), "'", ''), ' ') AS coord_array
                FROM `pigpig-project-432914.pigpigdata.osp_public_estate_geo_raw`      
        ''',
        destination_dataset_table=f"{ BQ_PROJECT }.{ BQ_DATASET }.osp_public_estate_geo",    
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id=BQ_CONN_ID,
        dag=dag
    )

   # ## Task 4: agg data
    agg_OSP_data_insert = BigQueryOperator(
        task_id='agg_OSP_data_insert',
        sql='''
            SELECT 
            name,
            ST_GEOGFROMTEXT(CONCAT('POLYGON(( ',
                STRING_AGG(CONCAT(SPLIT(coord, ',')[OFFSET(1)], ' ', SPLIT(coord, ',')[OFFSET(0)]), ', '), 
                ' ))')) AS polygon
            FROM `pigpig-project-432914.pigpigdata.osp_public_estate_geo`,
            UNNEST(coord_array) AS coord 
            GROUP BY name    
        ''',
        destination_dataset_table=f"{ BQ_PROJECT }.{ BQ_DATASET }.osp_public_estate_geo",    
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        allow_large_results=True,
        use_legacy_sql=False,
        gcp_conn_id=BQ_CONN_ID,
        dag=dag
    )

    # #Setting up Dependencies
trigger_cloud_function >> load_csv_to_bigquery >> cleaning_OSP_data_insert >> agg_OSP_data_insert

