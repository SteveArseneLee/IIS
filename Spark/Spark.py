import os
from pyspark.sql import SparkSession
from kafka import KafkaProducer
import json
import time
from pyspark.sql.functions import lit


MAX_MEMORY = "5g"

dir_path = "/home/steve/Apartment/Apartment_now"
TOPIC_NAME = "Apartment"

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
        print(f[0], "를 분석합니다")
        data = spark.read.csv(f"file:///{dir_path}/{file}", inferSchema = True, header = True)
        
        name = "Apart"
        data.createOrReplaceTempView(name)
        query = f"""
        SELECT
            `아파트명` as ApartmentName,
            `면적` as Area,
            `법정동주소` as StatutoryDongAddress,
            `도로명주소` as RoadNameAddress,
            `세대수` as NumberOfHousholds,
            `임대세대수` as RentalNumber,
            `가격` as Price,
            `매매호가` as SellingPrice,
            `전세호가` as CharteredPrice,
            `월세호가` as MonthlyRentPrice,
            `실거래가` as ActualTransactionPrice
        FROM
        {name}
        """
        data_df = spark.sql(query)
        data_df = data_df.withColumn("location", lit(f[0]))
        print(data_df.show())
        data_json = data_df.toJSON().map(lambda x: json.loads(x)).collect()
        brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
        producer = KafkaProducer(compression_type = 'gzip', 
                                 bootstrap_servers = brokers,
                                  )
                
        producer.send(TOPIC_NAME, value=json.dumps(data_json).encode("euc-kr"), key = f[0].encode("utf-8"))
        # producer.send(TOPIC_NAME, value=json.dumps(data_json).encode("utf-8-sig"), key = f[0].encode("utf-8"))

        print(data_json)
        time.sleep(1)