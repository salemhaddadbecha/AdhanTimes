import boto3
import requests
import pandas as pd
from io import StringIO
import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log
cities = ['Tunis', 'Sfax']

# Define the extraction function
def extract_data(**kwargs):
    execution_date = kwargs['execution_date']
    year = execution_date.year
    url = 'http://api.aladhan.com/v1/calendarByCity'


    for city in cities:
        all_dataframes = []
        for month in range(1, 13):
            params = {
                'city': city,
                'country': 'Tunisia',
                'month': month,
                'year': year,
                'method': 2
            }
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                calendar = data.get('data', [])
                df = pd.json_normalize(calendar, sep='.')
                selected_columns = ['date.readable', 'timings.Fajr', 'timings.Sunrise', 'timings.Dhuhr',
                                    'timings.Asr', 'timings.Maghrib', 'timings.Isha', 'timings.Imsak']
                df_selected = df[selected_columns].rename(columns={
                    'date.readable': 'Date',
                    'timings.Fajr': 'Fajr',
                    'timings.Sunrise': 'Sunrise',
                    'timings.Dhuhr': 'Dhuhr',
                    'timings.Asr': 'Asr',
                    'timings.Maghrib': 'Maghrib',
                    'timings.Isha': 'Isha',
                    'timings.Imsak': 'Imsak'
                })
                df_selected['City'] = city
                all_dataframes.append(df_selected)
            else:
                log.error(f'Failed for {year}-{month:02d}: {response.status_code}')

        # Combine all months into one DataFrame
        full_df = pd.concat(all_dataframes, ignore_index=True)
        file_path = f'/opt/airflow/data/prayer_times_{year}_{city}.csv'
        full_df.to_csv(file_path, index=False)
        log.info(f"Full-year data saved to {file_path}")
def load_data_s3(**kwargs):
    execution_date = kwargs['execution_date']  # Comes from Airflow context

    for city in cities:
        try:
            file_path = f'/opt/airflow/data/prayer_times_{execution_date.year}_{city}.csv'
            if os.path.exists(file_path):
                s3_client = boto3.client('s3',
                                         aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                                         aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
                try:
                    with open(file_path, 'rb') as file_data:  # open in binary mode
                        s3_client.put_object(Bucket='bucketadhen',
                                             Key=f'prayer_times_{execution_date.year}_{city}.csv',
                                             Body=file_data)

                    log.info("Upload successful.")
                except Exception as e:
                    log.error(f"Error uploading to S3: {str(e)}")

            else:
                log.warning("No file to upload.")
        except Exception as e:
            log.error(f"An error occurred: {e}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow', #Person or team resonsible for the task
    'start_date': datetime(2025, 1, 1),  # Define start date for scheduling
}

# Define the DAG
dag = DAG(
    dag_id="prayer_times_data_pipeline",
    default_args=default_args,
    description='A simple DAG to fetch prayer times and upload to S3',
    schedule_interval='0 6 1 * *',  # Set this cron schedule as needed (daily at 6:00 AM)
    catchup=False
)

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)

upload_task = PythonOperator(
    task_id='upload_data_to_s3_task',
    python_callable=load_data_s3,
    provide_context=True,
    dag=dag
)

# Set task dependencies
extract_task >> upload_task

#Pushh to git: after that do the part of the multiple task in parallel

"""
# # To test:
# Run all the application on AWS EKS Kuberenets + Terraform or Airflow on Kuberentes #https://www.youtube.com/watch?v=ueo__msW2Os
#S3Hook: Replace boto3 with Airflow’s native S3Hook or S3FileTransformOperator.
# email alerts: Once the job is completed or if there is a problem with template. 
# email sending the jobs by email. 
#put the metadata of airflow on metadata database(postegre 
#Learn XCom	Pass data between tasks (e.g., file name, success status)."""

#https://chatgpt.com/c/67fe5cdc-626c-800c-97b6-eb3366056665
"""-------Looping over cities dynamically
3) I want to do that:Loop over a list of cities (e.g., Tunis, Sfax, Paris) and fetch each as a separate task using taskflow or TaskGroup. #https://chatgpt.com/c/67fe5cdc-626c-800c-97b6-eb3366056665
#  Loop Over Multiple Cities
#This is Tunis Time. I want to extract time prayer time, sfax time, all 24 prayer time and put each of them in a separate csv file
#Parallel execution for all those tasks
#the csv should be prayer_time_city_date
#do a power bi that will show those data based on  a filter by city.
"""