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

      Consume_All = BashOperator(
        task_id = "consume_all",
        bash_command="nohup python3 /home/steve/Cryptocurrency/Kafka/consumer.py & > /dev/null"
      )
      Consume_High = BashOperator(
        task_id = "consume_high",
        bash_command="nohup python3 /home/steve/Cryptocurrency/Kafka/consumer_cryp_high.py & > /dev/null" 
      )
      Consume_Now = BashOperator(
        task_id = "consume_now",
        bash_command="nohup python3 /home/steve/Cryptocurrency/Kafka/consumer_cryp_now.py & > /dev/null" 
      )
      Value_generator = SparkSubmitOperator(
        application="/home/steve/Cryptocurrency/Spark/Cryp.py",
        task_id="Value_generator",
        conn_id="stock_spark_local"
      )
      High_Value = SparkSubmitOperator(
        application="/home/steve/Cryptocurrency/Spark/Cryp_high.py",
        task_id="High_Value",
        conn_id="stock_spark_local"
      )
      Now_Value = SparkSubmitOperator(
        application="/home/steve/Cryptocurrency/Spark/Korean_stock_open.py",
        task_id="Now_Value",
        conn_id="stock_spark_local"
      )
      Consume_All >> Consume_High >> Consume_Now >> Value_generator >> High_Value >> Now_Value