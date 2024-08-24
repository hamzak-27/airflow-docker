import io
import airflow 
from airflow import DAG  
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator 
from airflow.hooks.S3_hook import S3Hook
import pandas as pd 
import numpy as np 
import os 

s3_conn_id = 'my_s3_connection'

def load_and_transform_from_s3(s3_bucket, s3_key):
    s3_hook = S3Hook(s3_conn_id)
    file_contents = s3_hook.read_key(s3_key, bucket_name=s3_bucket)
    
    # Transform the data (replace this with your actual transformation logic)
    transformed_data = data_cleaning(file_contents)
    
    # Example: Write the transformed data to a local file (you can modify this based on your needs)
    transformed_data.to_csv('C:\\Users\\ihamz\\airflow-docker\\output.csv', index=False)


def data_cleaning(data):
    
    df = pd.read_csv(io.StringIO(data))
    
    df.head()
    
    df.info()
    df.isnull().sum().sort_values(ascending=False)

    df.drop(columns=['Individual_Price_US$','Year_Month','Time'],inplace=True)

    df['Amount US$'] = df['Amount US$'].str.replace(',', '')
    df['Amount US$'] = df['Amount US$'].fillna(0)
    df['Amount US$'] = df['Amount US$'].astype('int')

    df['Date'] = pd.to_datetime(df['Date'])

    df.info()

    return df

   

def success_message():
    print("Data successfully cleaned.")

default_args = {
    'owner': 'hkhann',
    'depends_on_past':False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email':'ihamzakhan89@gmail.com',
    'email_on_failure':True,
    'email_on_retry':False
}

dag = DAG(
    dag_id = 'Data_clean_etl',
    default_args = default_args,
    description = "A DAG for cleaning Ecommerce data.",
    start_date = datetime(2023,9,30),
    schedule_interval = timedelta(days=1),
)

load_transform_task = PythonOperator(
    task_id='load_and_transform_from_s3',
    python_callable=load_and_transform_from_s3,
    op_args=['airflow-hamza', 'csv/Us-Ecommerce Dataset.csv'], # Replace with your S3 bucket and key
    dag=dag,
)

'''clean_data = PythonOperator(
    task_id = 'data_cleaning',
    python_callable = data_cleaning,
    dag = dag,
)'''

message = PythonOperator(
    task_id = 'success_message',
    python_callable = success_message,
    dag = dag,
)

load_transform_task >> message

