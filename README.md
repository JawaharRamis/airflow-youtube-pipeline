# YouTube Data Analysis Pipeline using Apache Airflow

This project sets up an Apache Airflow DAG (Directed Acyclic Graph) to automate the extraction, transformation, and loading (ETL) process of YouTube data. The pipeline retrieves popular YouTube videos' data from various regions, performs analysis on the data, and uploads the results to AWS S3.
Certainly! Here's an updated section for your README.md that mentions how to run the code and ensure access to the YouTube API:

## How to Run the Code

To successfully run the code and retrieve YouTube video data using this Airflow DAG, follow these steps:

### Prerequisites

1. **Docker Compose:** Ensure you have Docker Compose installed on your system. If not, you can download and install it from the official Docker website: [Docker Compose Installation](https://docs.docker.com/compose/install/).

2. **YouTube API Access:** This DAG relies on the YouTube API to retrieve video data. Before running the DAG, you need to obtain API credentials from the [Google Cloud Console](https://console.developers.google.com/) and enable the YouTube Data API for your project. Make sure to save your API key securely.

3. **AWS S3 bucket for data storage** 

### Steps

1. **Clone Repository:** Clone this repository to your local machine using Git:

   ```bash
   git clone https://github.com/JawaharRamis/airflow-youtube-pipeline.git
   cd airflow-youtube-pipeline
   ```

2. **API Key Configuration:**

   a. Open the `/dags/yt_dag.py` file in a text editor.

   b. Find the section where the API key is required and replace `'YOUR_YOUTUBE_API_KEY'` with your actual YouTube API key.

3. **Running the Airflow DAG:**

   a. In the project root directory, run the following command to start the Airflow services using Docker Compose:

   ```bash
   docker-compose up -d
   ```

   This command will launch the Airflow web server, scheduler, and worker containers.

   b. Access the Airflow web UI by opening your browser and navigating to `http://localhost:8080`.

   c. In the Airflow web UI, navigate to the DAGs section and trigger the DAG you want to run.

4. **Monitoring and Results:**

   a. In the Airflow web UI, monitor the progress of the DAG run and check the logs for each task.

   b. Once the DAG run is completed, the results such as the retrieved YouTube data and analysis results will be available.

Remember to replace `'YOUR_YOUTUBE_API_KEY'` with your actual YouTube API key. Ensure that you have a stable internet connection and that the required dependencies are correctly installed in the Docker containers.

By following these steps, you will be able to run the Airflow DAG and retrieve YouTube video data. Make sure to refer to the other sections in this README for information on individual tasks and components.

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
