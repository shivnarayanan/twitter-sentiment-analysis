import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.hive_operator import HiveOperator
from datetime import date, timedelta, datetime
from python_functions import fetchTweets, readTweets

today = date.today()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('my-twitter-dag', default_args=default_args, schedule_interval="@once")

task1 = PythonOperator(task_id = 'fetching_tweets', python_callable = fetchTweets, op_kwargs={"todayDate":today}, dag=dag)
task2 = PythonOperator(task_id = 'reading_tweets', python_callable = readTweets, op_kwargs={"todayDate":today}, dag=dag)

task1.set_downstream(task2)