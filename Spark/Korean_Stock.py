import os
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import time
from pyspark.sql.functions import lit


MAX_MEMORY="5g"

dir_path = "/home/steve/Stock/Korean_stock/Korean_stock_data"
TOPIC_NAME = "Stock"
for (root, directories, files) in os.walk(dir_path):
    for d in directories:
        d_path = os.path.join(root, d)
        print(d_path)

    for file in files:
        f = file.split('.')
     
        spark = SparkSession.builder.appName(f"Korean-stock-Close-{f[0]}")\
                .config("spark.executor.memory", MAX_MEMORY)\
                .config("spark.driver.memory", MAX_MEMORY)\
                .getOrCreate()
        print(f[0],"의 폐장가에 대해 분석합니다")
        data = spark.read.csv(f"file:///{dir_path}/{file}", inferSchema = True, header = True)
        # name = f[0].encode('utf-8').decode('ascii', 'ignore')
        # name = name.strip()
        name = f[1]
        data.createOrReplaceTempView(name)
        
        query = f"""
        SELECT
            DATE_FORMAT(TO_DATE(Date), 'yyyy-M-dd') AS DATEDATA, 
            Open as Open_value,
            High as High_value,
            Low as Low_value,
            Close as Close_Value,
            `Adj Close` as Value_now
        FROM
            {name}
        """
        data_df = spark.sql(query)
        data_df = data_df.withColumn("company", lit(f[0]))
        
        data_json = data_df.toJSON().map(lambda x: json.loads(x)).collect()
        brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
        producer = KafkaProducer(compression_type = 'gzip', 
                                 bootstrap_servers = brokers,
                                  )
                
        producer.send(TOPIC_NAME, value=json.dumps(data_json).encode("euc-kr"), key = f[0].encode("utf-8"))
        # producer.send(TOPIC_NAME, value=json.dumps(data_json).encode("utf-8-sig"), key = f[0].encode("utf-8"))

        print(data_json)
        time.sleep(1)
