from airflow.decorators import dag, task
from airflow.utils.dates import datetime
import pandas as pd
import requests
import boto3
import os
from airflow.utils.log.logging_mixin import LoggingMixin

log = LoggingMixin().log
CITIES = ['Tunis', 'Sfax', 'Sousse', 'Gabes', 'Nabeul']


@dag(
    dag_id="prayer_times_dynamic_city_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval='@yearly',  # Once a year to get the whole year per city
    catchup=False,
    tags=['prayer_times', 'dynamic', 's3'],
)
def prayer_times_pipeline():

    @task
    def extract_city_data(city: str, execution_date=None):
        """Extract data for one city across all months of the year."""
        year = execution_date.year
        all_dataframes = []

        for month in range(1, 13):
            params = {
                'city': city,
                'country': 'Tunisia',
                'month': month,
                'year': year,
                'method': 2
            }
            response = requests.get('http://api.aladhan.com/v1/calendarByCity', params=params)
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
                all_dataframes.append(df_selected)
            else:
                log.error(f'Failed to fetch data for {city} {year}-{month:02d}: {response.status_code}')

        full_df = pd.concat(all_dataframes, ignore_index=True)
        file_path = f'/opt/airflow/data/prayer_times_{year}_{city}.csv'
        full_df.to_csv(file_path, index=False)
        return file_path

    @task
    def upload_city_data_to_s3(file_path: str):
        """Upload the file to S3."""
        city_file = os.path.basename(file_path)
        try:
            if os.path.exists(file_path):
                s3_client = boto3.client('s3',
                                         aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                                         aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
                with open(file_path, 'rb') as file_data:
                    s3_client.put_object(Bucket='bucketadhentunis', Key=city_file, Body=file_data)
                log.info(f"Uploaded {city_file} to S3.")
            else:
                log.warning(f"File {file_path} does not exist.")
        except Exception as e:
            log.error(f"Failed to upload {file_path}: {str(e)}")

    # Map extract + upload tasks dynamically per city
    upload_city_data_to_s3.expand(
        file_path=extract_city_data.expand(city=CITIES)
    )

dag_instance = prayer_times_pipeline()


"""
# # To test:
#Do a Dev branch
# Run all the application on AWS EKS Kuberenets + Terraform or Airflow on Kuberentes #https://www.youtube.com/watch?v=ueo__msW2Os
#S3Hook: Replace boto3 with Airflow’s native S3Hook or S3FileTransformOperator.
# email alerts: Once the job is completed or if there is a problem with template. 
# email sending the jobs by email. 
#put the metadata of airflow on metadata database(postegre 
#Learn XCom	Pass data between tasks (e.g., file name, success status)."""


#do a power bi that will show those data based on  a filter by city.

