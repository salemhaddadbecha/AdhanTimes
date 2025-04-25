import boto3
import requests
import pandas as pd
from io import StringIO
import os

def extract_data():
    url = 'http://api.aladhan.com/v1/calendarByCity'
    params = {
        'city': 'Tunis',
        'country': 'Tunisia',
        'month': 3,
        'year': 2025,
        'method': 2  # Adjust the method as needed for calculation
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        # Extract the 'data' key which contains the calendar
        calendar = data.get('data', [])
        # Normalize the JSON data to create a DataFrame
        df = pd.json_normalize(calendar, sep='.')

        selected_columns = ['date.readable', 'timings.Fajr', 'timings.Sunrise', 'timings.Dhuhr', 'timings.Asr', 'timings.Maghrib', 'timings.Isha', 'timings.Imsak' ]
        df_selected = df[selected_columns]
        df_selected = df_selected.rename(columns={
            'date.readable': 'Date',
            'timings.Fajr': 'Fajr',
            'timings.Sunrise': 'Sunrise',
            'timings.Dhuhr': 'Dhuhr',
            'timings.Asr': 'Asr',
            'timings.Maghrib': 'Maghrib',
            'timings.Isha': 'Isha',
            'timings.Imsak': 'Imsak'
        })
        df_selected.to_csv('prayer_times_march.csv', index=False)
        return df_selected
    else:
        print(f'Error fetching data: {response.status_code}')
        return None

def load_data_s3(df):
    try:
        s3_client = boto3.client('s3', aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
                                 aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'))
        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        # Upload CSV to S3
        s3_client.put_object(Bucket='bucketadhentunis', Key='prayer_times_march.csv', Body=csv_buffer.getvalue())
        print("Upload successful.")
    except Exception as e:
        print(f"An error occurred: {e}")

def main():
    df = extract_data()
    if df is not None:
        load_data_s3(df)
    else:
        print("No data to upload.")

if __name__ == "__main__":
    main()