# YouTube Data Analysis Pipeline using Apache Airflow

This project sets up an Apache Airflow DAG (Directed Acyclic Graph) to automate the extraction, transformation, and loading (ETL) process of YouTube data. The pipeline retrieves popular YouTube videos' data from various regions, performs analysis on the data, and uploads the results to AWS S3.

## Prerequisites

- Apache Airflow installed
- Python 3.x
- AWS S3 bucket for data storage
- PostgreSQL database connection

## DAG Tasks Overview

### `create_table_task`
Creates the required table in the PostgreSQL database.

### `extract_task`
Extracts popular YouTube videos data for specified regions using the YouTube API.

### `transform_task`
Applies data transformation and cleaning to the extracted YouTube data.

### `postgres_upload`
Uploads the cleaned YouTube data to the PostgreSQL database.

### `s3_upload_yt_data`
Uploads the cleaned YouTube data to an AWS S3 bucket.

### `analysis_task`
Performs analysis on the YouTube data and generates analysis results.

### `s3_upload_analysis_data`
Uploads the analysis results to an AWS S3 bucket.

### `email_task`
Sends an email notification with the execution summary and attached data files.

## Setup

1. Configure your AWS S3 bucket details in the relevant task parameters.
2. Set up a PostgreSQL connection in Airflow with the name `'postgres_conn'`.
3. Set up a aws connection in Airflow with the name `'s3_conn'`.
4. Set up your email for `'AIRFLOW__SMTP__SMTP_USER'` and `'AIRFLOW__SMTP__SMTP_MAIL_FROM'` in docker-compose.yaml

## Usage

1. Place this DAG definition file in your Airflow DAGs directory.
2. Access the Airflow web interface and trigger the DAG manually or schedule its execution.

## Contact

For questions or issues related to this project, feel free to contact [jawaharramis@gmail.com](mailto:jawaharramis@gmail.com).
