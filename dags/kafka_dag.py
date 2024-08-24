from datetime import datetime 
from airflow import DAG 
from airflow.operators.python import PythonOperator 
import json 
import requests

default_args = {
    'owner':'y2j',
    'start_date':datetime(2023,10,10,00)
}

def get_data():

    res = requests.get('https://randomuser.me/api/')
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['postcode'] = location['podcast']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['phone'] = res['phone']

    return data

def stream_data():

    res = get_data()
    res = format_data(res)
    


     

with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id = 'streaming',
        python_callable=stream_data
    )


streaming_task
    
