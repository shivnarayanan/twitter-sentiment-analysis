import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.models.dagrun import DagRun
import time
from datetime import date, datetime, timedelta
from python_functions import fetchTweets, readTweets, importDatabase

today = date.today()
now = time.strftime('%H')
timestamp = str(today) + '_' + now

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': '2022-3-30',
    'email': ['shiv.narayanan@icloud.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('my-twitter-dag', default_args=default_args, schedule_interval="@daily")

task1 = PythonOperator(task_id = 'fetching_tweets', python_callable = fetchTweets, provide_context=True, dag=dag)
task2 = PythonOperator(task_id = 'reading_tweets', python_callable = readTweets, provide_context=True, dag=dag)

task3 = PostgresOperator(
    task_id= 'create_postgres_table', 
    postgres_conn_id='postgres_default', 
    sql="""
        create table if not exists my_tweets (
            tweet_id int,
            tweet_date character varying,
            tweet_content character varying,
            primary key (tweet_id) 
        )
    """,
    dag=dag
)

task4 = PythonOperator(task_id = 'importing_database', python_callable = importDatabase, provide_context=True, dag=dag)

task1.set_downstream(task2)
task2.set_downstream(task3)
task3.set_downstream(task4)


