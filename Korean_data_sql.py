from pyspark.sql import SparkSession
# import pandas as pd
import os

spark = SparkSession.builder.appName("Korean_data_prac").getOrCreate()

directory = "/home/steve/Capstone2/Korean_stock/Korean_stock_data"
filename = "DL.csv"
data = spark.read.csv(f"file:///{directory}/{filename}", inferSchema = True, header = True)

f = filename.split('.')
print(f[0],"에 대해 분석합니다")

data.createOrReplaceTempView(f[0])

spark.sql(f"SELECT HIGH FROM {f[0]}").show()