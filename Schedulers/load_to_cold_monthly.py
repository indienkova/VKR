from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
sys.path.insert(1, '/home/anastasia/Рабочий стол/DataTransfer/')

import DataTransfererCopy

dag = DAG('load_data_to_cold_monthly',
        start_date=datetime(2021, 1 ,1),
        schedule_interval='@monthly',
        catchup=False)

PythonOperator(dag=dag,
               task_id='load_to_cold',
               python_callable=DataTransfererCopy.launch)