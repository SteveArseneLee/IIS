import os
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json

MAX_MEMORY = "5g"
TOPIC_NAME = "Cryptocurrency_High"

dir_path = "/home/steve/Cryptocurrency/Cryp_data"


# dir_path내의 모든 파일들 실행
for (root, directories, files) in os.walk(dir_path):
    for d in directories:
        d_path = os.path.join(root, d)
        # print(d_path)
    
    for file in files:
        f = file.split('-')
        name = f[0]
        spark = SparkSession.builder.appName(F"Cryptocurrency-{name}")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()
        print(name,"의 최고가에 대해 분석합니다")
        data = spark.read.csv(f"file:///{dir_path}/{file}", inferSchema=True, header=True)
        
        data.createOrReplaceTempView(name)
        query = f"""
        SELECT
            date,high
        FROM
            {name}
        """
        data_df = spark.sql(query)
        # data_col = data_df.toJSON().collect()
        data_json = data_df.toJSON().map(lambda x: json.loads(x)).collect()
      
        
        # Kafka Producer 실행
        brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
        producer = KafkaProducer(bootstrap_servers = brokers)
        producer.send(TOPIC_NAME, value=json.dumps(data_json).encode("utf-8"), key = name.encode("utf-8"))
