# 🕌 Prayer Times Data Pipeline with Python, Airflow, S3, Athena & Power BI

This project automates the collection, storage, and analysis of **prayer times across Tunisian cities** using:

- **Airflow** to schedule and orchestrate the workflow
- **API** (Aladhan) to fetch prayer times
- **Amazon S3** to store data as CSV files
- **AWS Athena** to query the data using SQL
- **Power BI** to visualize prayer times dynamically

---
The pipeline consists of the following steps:

1.  **Extraction (Airflow):** An Airflow DAG (`prayer_times_dynamic_city_dag.py`) fetches prayer times data for each specified city (Tunis, Sfax, Sousse, Gabes, Nabeul) for the entire year 2025 from the `http://api.aladhan.com/v1/calendarByCity` API. The extracted data is saved as individual CSV files in the Airflow worker's local storage.
2.  **Loading to S3 (Airflow):** The same Airflow DAG then uploads these local CSV files to an Amazon S3 bucket (`bucketadhentunis`). The filenames in S3 will be in the format `prayer_times_2025_CityName.csv`.
3.  **Table Creation in Athena:** An Athena external table (`prayer_times.prayer_times_2025_partitioned`) is created, defining the schema of the prayer times data and specifying the S3 location as the source. This table is partitioned by `ville` (city).
4.  **Partitioning in S3 (Manual):** The data in S3 needs to be organized into a partitioned structure that Athena can understand. This involves moving the city-specific CSV files into directories named according to the partition key (e.g., `ville=Sfax`).
5.  **Loading Partitions in Athena:** The `MSCK REPAIR TABLE` command in Athena is used to automatically discover and register the partitions based on the directory structure in S3.
6.  **Querying in Athena:** Once the partitions are loaded, you can query the data in Athena using standard SQL, filtering by city (the partition key).
7.  **Connecting Power BI to Athena:** You can then connect Power BI to the Athena database and table to visualize and analyze the prayer times data.

## Prerequisites

* **AWS Account:** You need an active AWS account with access to S3 and Athena.
* **S3 Bucket:** An S3 bucket named `bucketadhentunis` should exist.
* **Airflow Environment:** An Airflow environment configured with:
    * Python 3.6 or higher.
    * `apache-airflow` package installed.
    * `pandas` package installed.
    * `requests` package installed.
    * `apache-airflow-providers-amazon` package installed (for S3 integration).
    * AWS credentials configured as environment variables (`AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`) or through an IAM role attached to the Airflow worker.
* **Athena Setup:** Athena should be configured in your AWS account.
* **Power BI Desktop:** Power BI Desktop installed if you intend to connect to Athena for visualization.
* **Athena JDBC Driver:** The Amazon Athena JDBC driver needs to be installed and configured in Power BI Desktop to establish a connection.

## Step-by-Step Instructions

### 1. Airflow DAG (`prayer_times_dynamic_city_dag.py`)

1.  Place the `prayer_times_dynamic_city_dag.py` file in your Airflow DAGs folder (e.g., `/opt/airflow/dags/`).
2.  Ensure the Airflow scheduler and webserver are running.
3.  Unpause the `prayer_times_dynamic_city_dag` in the Airflow UI.
4.  Trigger the DAG manually or wait for its scheduled run (once in 2025).
5.  Monitor the task logs in the Airflow UI to ensure the data is extracted and uploaded to `s3://bucketadhentunis/`. You should see files like `prayer_times_2025_Tunis.csv`, `prayer_times_2025_Sfax.csv`, etc., in your S3 bucket.


### 2. Athena Table Creation

1.  Open the AWS Management Console and navigate to the Athena service.
2.  Ensure you have selected the correct AWS region.
3.  In the query editor, execute athena_scripts.sql file.

### 3. Organize Data in S3 for Partitioning

1.  Open the AWS Management Console and navigate to the S3 service.
2.  Go to your `bucketadhentunis` bucket.
3.   Move the respective CSV files into these directories. For example:

    ```bash
    aws s3 mv s3://bucketadhentunis/prayer_times_2025_Tunis.csv s3://bucketadhentunis/ville=Tunis/prayer_times.csv
    aws s3 mv s3://bucketadhentunis/prayer_times_2025_Sfax.csv s3://bucketadhentunis/ville=Sfax/prayer_times.csv
    aws s3 mv s3://bucketadhentunis/prayer_times_2025_Sousse.csv s3://bucketadhentunis/ville=Sousse/prayer_times.csv
    aws s3 mv s3://bucketadhentunis/prayer_times_2025_Gabes.csv s3://bucketadhentunis/ville=Gabes/prayer_times.csv
    aws s3 mv s3://bucketadhentunis/prayer_times_2025_Nabeul.csv s3://bucketadhentunis/ville=Nabeul/prayer_times.csv
    ```
    
### 4. Load Partitions into Athena

1.  Go back to the Athena query editor.
2.  Execute the `MSCK REPAIR TABLE` command:

    ```sql
    MSCK REPAIR TABLE prayer_times.prayer_times_2025_partitioned;
    ```

    This command tells Athena to scan the S3 location specified in the table definition and automatically discover the partition directories (`ville=Tunis`, `ville=Sfax`, etc.).

### 5. Verify Partitions

1.  After the `MSCK REPAIR TABLE` command completes, you can verify that the partitions have been loaded by running:

    ```sql
    SHOW PARTITIONS prayer_times.prayer_times_2025_partitioned;
    ```

    You should see a list of partitions corresponding to each city (e.g., `ville=Gabes`, `ville=Nabeul`, `ville=Sfax`, `ville=Sousse`, `ville=Tunis`).

### 6. Query Data in Athena

1.  Now you can query the prayer times data, filtering by the `ville` partition:

    ```sql
    SELECT *
    FROM prayer_times.prayer_times_2025_partitioned
    WHERE ville = 'Sfax'
    LIMIT 10;

    SELECT Date, Fajr, Maghrib
    FROM prayer_times.prayer_times_2025_partitioned
    WHERE ville = 'Tunis'
    AND Date LIKE '%April%';
    ```

### 7. Connect Power BI to Athena

1.  **Install Athena JDBC Driver:** Download and install the Amazon Athena JDBC driver on your Power BI Desktop machine.
2.  **Get Data in Power BI:** Open Power BI Desktop and click "Get Data".
3.  **Search for Amazon Athena:** Search for "Amazon Athena" and select it as a data source.
4.  **Configure Connection:**
    * **Driver:** Choose the installed Athena JDBC driver.
    * **Connection String:** Enter the JDBC connection string for your Athena instance. 
    * You might need to configure additional properties based on your setup.
5.  **Navigate and Select Table:** Once connected, you should see the `prayer_times_2025_partitioned` table. Select it and click "Load" or "Transform Data".
6.  **Filter and Visualize:** You can now use Power BI to filter the data by the `ville` column and create visualizations based on the prayer times.

## Troubleshooting

* **No Results in Athena:** Ensure that:
    * The `LOCATION` in your `CREATE EXTERNAL TABLE` statement points to the correct root S3 path (`s3://bucketadhentunis/`).
    * You have correctly organized your data in S3 with the `ville=CityName` directory structure.
    * You have successfully run `MSCK REPAIR TABLE` to load the partitions. Check the Athena query history for any errors.
    * Your `WHERE` clause in your `SELECT` statement on the partitioned table uses the correct partition key (e.g., `WHERE ville = 'Sfax'`).
* **Airflow Task Failures:** Check the Airflow task logs for any errors during API extraction or S3 upload. Ensure your AWS credentials are correctly configured.
* **Power BI Connection Issues:** Verify that the Athena JDBC driver is installed correctly and that your connection string is accurate, including the region and S3 output location.


         +-------------------+
         |  Airflow DAG      |
         | (Yearly Schedule) |
         +--------+----------+
                  |
                  v 
        +------------------------+                      
        | Aladhan API (by City)  |
        +------------------------+
                  |
                  v
       +------------------------+
       |   CSV (Local Airflow)  |
       +------------------------+
                  |
                  v
       +------------------------+
       |   Upload to S3 Bucket  |
       | s3://bucketadhentunis/ |
       +------------------------+
                  |
        (Partitioned by /ville=City/)
                  |
                  v
         +---------------------+
         |  AWS Athena Table   |
         +---------------------+
                  |
                  v
         +---------------------+
         |     Power BI        |
         |   (via ODBC)        |
         +---------------------+
