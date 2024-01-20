from datetime import datetime, timedelta
import os
import logging

import pandas as pd
from googleapiclient.discovery import build
from airflow.exceptions import AirflowException
from airflow.hooks.S3_hook import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

import sql_statements


def get_popular_videos(region_codes):
    # Set up YouTube Data API client
    api_key = os.environ.get("YT_API_KEY")
    youtube = build('youtube', 'v3', developerKey=api_key)
    
    try:
        videos =[]
        for region_code in region_codes:
        # Call the YouTube Data API to retrieve popular videos
            try:
                response = youtube.videos().list(
                    part='snippet',
                    chart='mostPopular',
                    regionCode=region_code,
                    maxResults=10,  # You can adjust the number of results as needed
                    # publishedBefore=now_timestamp
                ).execute()
            except Exception as e:
                error_code = e.resp.status
                error_message = e._get_reason()
                print(f"Error: {error_code} - {error_message}")
                raise AirflowException(f"An unexpected error occurred: {e}")
            
            # Retrieve video ratings (likes and dislikes)
            video_ids = [item['id'] for item in response['items']]
            video_stats = youtube.videos().list(
                part='statistics',
                id=','.join(video_ids)
            ).execute()

            # Process the response and extract video details
            for item in response['items']:
                video_id = item['id']
                title = item['snippet']['title']
                release_date = item['snippet']['publishedAt']
                channel = item['snippet']['channelTitle']
                # language = item['snippet']['defaultAudioLanguage']
                stats = video_stats['items'][video_ids.index(video_id)]['statistics']
                views = int(stats['viewCount']) if 'viewCount' in stats else 0
                likes = int(stats['likeCount']) if 'likeCount' in stats else 0
                commentCount = int(stats['commentCount']) if 'commentCount' in stats else 0
                videos.append({'video_id': video_id, 'title': title, 'region_code': region_code,
                                   'release_date': release_date, 'channel': channel,
                                   'views_num': views, 'likes_num': likes, 'commentCount': commentCount})
        return videos

    except Exception as e:
        print(f'An error occurred: {str(e)}')
        raise AirflowException(f"An unexpected error occurred: {e}")
    
def transform(**kwargs):
    """
    Transforms the extracted data and saves it as a CSV file.

    This function takes the extracted data from the XCom variable named 'extract_task',
    transforms it, and saves it as a CSV file in the specified path.

    Parameters:
    **kwargs: A dictionary containing task instance information, including the 'ti' key.

    Returns:
    pandas.DataFrame: The transformed DataFrame.
    """
    ti = kwargs['ti']
    my_dict = ti.xcom_pull(task_ids='extract_task')  # Fetch the dictionary from XCom
    if not my_dict or isinstance(my_dict, dict):
        raise ValueError("XCom data not found or not in the expected format.")
    dataframe = pd.DataFrame(my_dict)
    dataframe['release_date'] = pd.to_datetime(dataframe['release_date'])
    dataframe['release_date'] = dataframe['release_date'].dt.strftime('%Y-%m-%d %H:%M')
    dataframe['views'] = dataframe['views_num'].apply(lambda x: '{:,.2f}M'.format(x / 1000000))
    dataframe['likes'] = dataframe['likes_num'].apply(lambda x: '{:,.2f}K'.format(x / 1000))
    dataframe['commentCount'] = dataframe['commentCount'].apply(lambda x: '{:,.2f}K'.format(x / 1000))
    path= "/opt/airflow/data"
    try:
        dataframe.to_csv(r'{}/yt_data.csv'.format(path), index=False, header=True)
        logging.info(f"DataFrame has been successfully saved as CSV at {path}.")
    except Exception as e:
        logging.error(f"Failed to save DataFrame as CSV: {e}")
        raise
    return dataframe

def upload_to_postgres(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform_task')
    postgres_sql_upload = PostgresHook(postgres_conn_id='postgres_conn', schema='youtube') 
    postgres_sql_upload.insert_rows('youtube_videos', rows = data.values.tolist(), 
                                    target_fields= ["video_id","title","region_code","release_date","channel","views_num","likes_num","comments, views, likes"])

def upload_to_s3_yt(filename, keyname, bucket):
    """
    Uploads a file to Amazon S3 with a folder structure based on the current date and hour:minutes.

    Parameters:
    filename (str): The local file path of the file to be uploaded.
    keyname (str): The key name for the S3 object. This will be the prefix of the folder structure.
    bucket (str): The name of the S3 bucket where the file will be uploaded.

    Returns:
    None
    """
    current_datetime = datetime.now()
    today_date = current_datetime.strftime('%Y-%m-%d')
    hour_minutes = current_datetime.strftime('%H:%M')
    # full_datetime = f'{today_date} {hour_minutes}'

    filename_with_datetime = f"{os.path.basename(filename).split('.')[0]}_{hour_minutes}.{filename.split('.')[-1]}"

    key = "{}/{}/{}".format(keyname, today_date, filename_with_datetime)
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, bucket_name=bucket, key=key)

def upload_to_s3_analysis(filename, keyname, bucket):
    """
    Uploads a file to Amazon S3 with a folder structure based on the current date and hour:minutes.

    Parameters:
    filename (str): The local file path of the file to be uploaded.
    keyname (str): The key name for the S3 object. This will be the prefix of the folder structure.
    bucket (str): The name of the S3 bucket where the file will be uploaded.

    Returns:
    None
    """
    current_datetime = datetime.now()
    today_date = current_datetime.strftime('%Y-%m-%d')
    hour_minutes = current_datetime.strftime('%H:%M')
    # full_datetime = f'{today_date} {hour_minutes}'

    filename_with_datetime = f"{os.path.basename(filename).split('.')[0]}_{hour_minutes}.{filename.split('.')[-1]}"

    key = "{}/{}/{}".format(keyname, today_date, filename_with_datetime)
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, bucket_name=bucket, key=key)

def analyse_function(path):
    """
    Fetches analysis results from the database and writes them to a CSV file.

    This function executes a set of SQL queries to retrieve analysis results from a PostgreSQL database
    using a PostgresHook. It then writes the results to a CSV file in the specified location.

    The analysis results include:
    - Top 1 video per region with the most views
    - Top 3 most liked videos
    - Top 5 videos with the highest like per view ratio

    Parameters:
    path (str): path to the write file

    Returns:
    None
    """
    pg_hook = PostgresHook(postgres_conn_id='postgres_conn')
    
    # Fetch the results from the database
    results = []
    for query in sql_statements.analysis_sql_queries:
        query_results = pg_hook.get_records(sql=query)
        results.append(query_results)

    with open(path, 'w') as file:
        file.write("Top 1 video per region with the most views:\n")
        for row in results[0]:
            file.write(f"{row[0]} - {row[1]}\n")

        file.write("\nTop 3 most liked videos:\n")
        for row in results[1]:
            file.write(f"{row[0]} - {row[1]}\n")

        file.write("\nTop 5 videos with highest like per view \n")
        for row in results[2]:
            file.write(f"{row[0]} - {row[1]}\n")
