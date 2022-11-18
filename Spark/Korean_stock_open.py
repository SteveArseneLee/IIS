import os
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import csv 
import json 
import time
MAX_MEMORY="5g"

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
producer = KafkaProducer(bootstrap_servers = brokers)

# dir_path = "/home/ubuntu/IIS/Korean_stock/Korean_stock_data"
dir_path = "/home/steve/Capstone2/Korean_stock/Korean_stock_data"

for (root, directories, files) in os.walk(dir_path):
    for d in directories:
        d_path = os.path.join(root, d)
        print(d_path)

    for file in files:
        f = file.split('.')
        spark = SparkSession.builder.appName(f"Korean-stock-Open-{f[0]}")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()
        print(f[0],"의 개장가에 대해 분석합니다")
        data = spark.read.csv(f"file:///{dir_path}/{file}", inferSchema = True, header = True)
        name = f[0].encode('utf-8').decode('ascii', 'ignore')
        name = name.strip()
        data.createOrReplaceTempView(name)
        
        query = f"""
        SELECT
            Date, Open
        FROM
            {name}
        """
        data_df = spark.sql(query)
        
        # data_dir="/home/ubuntu/IIS/Spark_Result/Open"
        # data_dir="/home/steve/Capstone2/Spark_Result/Open"
        
        # data_df.write.format("json").mode('overwrite').save(f"{data_dir}/{f[0]}")
        
        topicName = f[0]
        data_json=data_df.toJson()
        producer.send(topicName, json.dumps(data_json).encode("utf-8"))
        time.sleep(1)
        