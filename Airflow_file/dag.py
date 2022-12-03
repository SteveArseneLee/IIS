from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
  'start_date': datetime(2022, 10, 1),
}


with DAG(dag_id='stock-data-pipeline',
        #  schedule_interval='@daily',
              schedule='@daily',
              default_args=default_args,
              tags=['stock'],
              catchup=False) as dag:

      Main_Process = BashOperator(
        task_id="main_process",
        bash_command='python3 /home/steve/Stock/main.py'
      )
      Consume_All = BashOperator(
        task_id = "consume_all",
        bash_command='nohup python3 /home/steve/Stock/Kafka/consumer.py & > /dev/null'
      )
      High_Value = SparkSubmitOperator(
        application="/home/steve/Stock/Spark/Korean_stock_high.py",
        task_id="High_Value",
        conn_id="stock_spark_local"
      )
      Open_Value = SparkSubmitOperator(
        application="/home/steve/Stock/Spark/Korean_stock_open.py",
        task_id="Open_Value",
        conn_id="stock_spark_local"
      )
      Close_Value = SparkSubmitOperator(
        application="/home/steve/Stock/Spark/Korean_stock_close.py",
        task_id="Close_Value",
        conn_id="stock_spark_local"
      )
      
      Main_Process >> Consume_All >> High_Value >> Open_Value >> Close_Value