import time
from datetime import datetime
from airflow.models.dag import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.hooks.base_hook import BaseHook
import pandas as pd
from sqlalchemy import create_engine

def on_failure_callback(**context):
  print(f"Task {context['task_instance_key_str']} failed.")


#Extract
@task()
def get_data():
  try:
    hook = MongoHook(conn_id="mongodb://localhost:27017/")
    client = hook.get_conn()
    db = client.nrc10052
    imports = db.imports
    print(f"Connected to MongoDB - {client.server_info()}")
    res = imports.find({"_id":53})
    print(res)
  except Exception as e:
    print("Error connecting to MongoDB -- {e}")


with DAG(
    dag_id="load_data_from_mongo",
    schedule_interval=None,
    start_date=datetime(2023,7,11),
    catchup=False,
    tags= ["currency"],
    default_args={
        "owner": "Rob",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        'on_failure_callback': on_failure_callback
    }
) as dag:

    t1 = SimpleHttpOperator(
        task_id='get_currency',
        method='GET',
        endpoint='2022-01-01..2022-06-30',
        headers={"Content-Type": "application/json"},
        do_xcom_push=True,
        dag=dag)

    t2 = PythonOperator(
        task_id='load-mongodb',
        python_callable=get_data,
        dag=dag
        )

    t1 >> t2

