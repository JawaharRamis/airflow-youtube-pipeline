from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.email_operator import EmailOperator

from py_functions import get_popular_videos, transform, upload_to_postgres, upload_to_s3_yt, analyse_function, upload_to_s3_analysis
import sql_statements


default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 7, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['jawaharramis@gmail.com']
}

with DAG(
    'youtube_data_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    description='Youtube ETL',
    catchup=False
) as dag:
    
    start_operator = DummyOperator(
    task_id='start_operator',
    dag=dag,
)

    create_table_task = PostgresOperator(
        task_id='create_table',
        sql=sql_statements.create_yt_table,
        postgres_conn_id='postgres_conn'
    )

    extract_task = PythonOperator(
        task_id='extract_task',
        python_callable=get_popular_videos,
        op_args=[['US', 'GB', 'CA', 'DE', 'IN']]
    )
    
    transform_task = PythonOperator(
        task_id='transform_task',
        python_callable=transform,
    )

    postgres_upload = PythonOperator(
        task_id='upload_to_postgres',
        python_callable=upload_to_postgres,
    )

    s3_upload_yt_data = PythonOperator(
        task_id='s3_upload_yt_data',
        python_callable=upload_to_s3_yt,
        op_kwargs={
            'filename': '/opt/airflow/data/yt_data.csv',
            'keyname': 'popular_videos',
            'bucket': 'youtube-airflow'
        }
    )

    s3_upload_analysis_data = PythonOperator(
        task_id='s3_upload_analysis_data',
        python_callable=upload_to_s3_analysis,
        op_kwargs={
            'filename': '/opt/airflow/data/analysis_results.csv',
            'keyname': 'analysis_results',
            'bucket': 'youtube-airflow'
        }
    )


    analysis_task = PythonOperator(
        task_id='analysis_task',
        python_callable=analyse_function,
        op_kwargs = {
            'path' : '/opt/airflow/data/analysis_results.csv'
        }
    )
    email_task = EmailOperator(
        task_id='send_email',
        to='jawaharramis@gmail.com',  # Replace with your Gmail address
        subject='Airflow DAG Execution Complete',
        html_content='Your Airflow DAG has completed successfully. Please find attached the results for the Youtube API data retrieval attached to this mail.',
        files=['/opt/airflow/data/yt_data.csv', '/opt/airflow/data/analysis_results.csv']
    )
    
    end_operator = DummyOperator(
        task_id='end_operator',
        dag=dag,
    )

    

    # Define task dependencies
    start_operator >> create_table_task >> extract_task >> transform_task
    transform_task >> postgres_upload >> [s3_upload_yt_data, s3_upload_analysis_data] >> analysis_task
    analysis_task >> email_task >> end_operator