from datetime import datetime

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
  'start_date': datetime(2022, 1, 1),
}

with DAG(dag_id='stock-data-pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['stock'],
         catchup=False) as dag:
  high = SparkSubmitOperator(
    application="/home/steve/Capstone2/Spark/Korean_stock_high.py", task_id="High_Value", conn_id="stock_spark_local"
  )
  open = SparkSubmitOperator(
    application="/home/steve/Capstone2/Spark/Korean_stock_open.py", task_id="Open_Value", conn_id="stock_spark_local"
  )
  
  high >> open