from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'hkhann',
    'depends_on_past':False,
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
    'email':'ihamzakhan89@gmail.com',
    'email_on_failure':False,
    'email_on_retry':False
}

dag = DAG(
    dag_id = 'spotift_etl',
    default_args = default_args,
    description = "A DAG for performing ETL on Spotify Data.",
    start_date = datetime(2023,9,20,2),
    schedule_interval = timedelta(days=1),
)
def just_a_func():
    print("Just a demo function.")

run_etl = PythonOperator(
    task_id = 'whole_spotify',
    python_callable = just_a_func,
    dag = dag,

)

run_etl

'''with DAG(
    dag_id = 'our_first_dag_v2',
    default_args = default_args,
    description = 'This is the first DAG.',
    start_date = datetime(2023,9,20,2),
    schedule_interval = '@daily'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = 'Hello My first DAG!!!'
    )

    task2 = BashOperator(
        task_id = "second_task",
        bash_command = "Hello My second DAG!!!"
    )'''

    #task1.set_downstream(task2)

    #or

    # task1 >> task2