from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
  'start_date': datetime(2022, 10, 1),
}


with DAG(dag_id='stock-data-pipeline',
        #  schedule_interval='@daily',
              schedule='@daily',
              default_args=default_args,
              tags=['stock'],
              catchup=False) as dag:
      High_Value = SparkSubmitOperator(
        # EC2에선 /home/ubuntu 사용
        
        application="/home/steve/Capstone2/Spark/Korean_stock_high.py", task_id="High_Value", conn_id="stock_spark_local"
      )
      Open_Value = SparkSubmitOperator(
        application="/home/steve/Capstone2/Spark/Korean_stock_open.py", task_id="Open_Value", conn_id="stock_spark_local"
      )
      Close_Value = SparkSubmitOperator(
        application="/home/steve/Capstone2/Spark/Korean_stock_close.py", task_id="Close_Value", conn_id="stock_spark_local"
      )
      
      High_Value >> Open_Value >> Close_Value