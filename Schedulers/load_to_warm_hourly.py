from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import sys
sys.path.insert(1, '/home/anastasia/Рабочий стол/DataTransfer/')

import loadToGP

dag = DAG('load_data_to_GP_hourly',
        start_date=datetime(2022, 1, 1),
        schedule_interval='@hourly',
        catchup=False)

PythonOperator(dag=dag, 
                task_id='load_to_warm',
                python_callable=loadToGP.launch)