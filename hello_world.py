from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
#import pymssql


#sys.path = ["/opt/airflow/dags"] + sys.path

def checklib():
    print('test logs')
    #print(f"pymssql version = {pymssql.__version__}  and syspath = {sys.path}")


with DAG(dag_id="checklib",

         start_date=datetime(2024,3,14),

         schedule="*/5 * * * *",

         catchup=False) as dag:


    task1 = PythonOperator(

            task_id="checklib",

            python_callable=checklib)


    task1
