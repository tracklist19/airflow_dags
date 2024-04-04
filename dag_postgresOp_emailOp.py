##  Airflow DAG for pipelining operations like handling tasks in a PostgreSQL database and Email notification afterwards


# _____________Imports______________________________________________________

import os
from datetime import datetime, timedelta
import requests

from airflow import DAG
from airflow.models import Variable

from airflow.operators.email_operator import EmailOperator
from airflow.utils.email import send_email_smtp

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.utils import timezone

from jinja2 import Environment, FileSystemLoader#, Template


# ____________DAG___________________________________________________________

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 7),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
}

with DAG("uebAtene", default_args=default_args, schedule_interval="@hourly") as dag:
                                                        


    # PostgresOperator : Create table, write & process spatial data (later on)

    Task_I = PostgresOperator(
        task_id="create_table",
        database="airflow_test",
        postgres_conn_id = "postgres_uebAtene",
        sql="""
        DROP TABLE IF EXISTS public.test_airflow3;
        CREATE TABLE public.test_airflow3()
        """
    )

    #___________________________________
    
    # PythonOperator
    def print_hello():
        return 'Hello World!'

    Task_II = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello
    )

    # BashOperator
    Task_III = BashOperator(
        task_id='run_bash',
        bash_command='echo 1',
        xcom_push=True
    )

    # DummyOperator
    Task_IV = DummyOperator(
        task_id='dummy_task'
    )
    #__________________________________


    # Send an emil
    Task_V = EmailOperator(
        task_id="send_email",
        to="airflow@example.com",
        #provide_context=True,
        subject="Postgres-Copy-Job done on " + str(datetime.utcnow()),
        html_content=""" <h3>Email Test</h3> """						# <a href="http://example.org">example.org</a>
    )


    Task_I >> Task_II >> Task_III >> Task_IV >> Task_V

