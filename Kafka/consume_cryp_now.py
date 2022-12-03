from kafka import KafkaConsumer
import json
import boto3
import datetime
from io import StringIO
import pandas as pd
from google.cloud import storage
from google.oauth2 import service_account

brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
TOPIC_NAME = "Cryptocurrency-AdjClose"

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=brokers,
                        #  auto_offset_reset='earliest',
                         enable_auto_commit=False,
)

# GCS
BUCKET_NAME = "pipeline-cryptocurrency"
KEY_PATH = "/home/steve/Cryptocurrency/integratedinvestmentservice-b5d16891816b.json"

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = storage.Client(credentials=credentials, project=credentials.project_id)
bucket = client.bucket(BUCKET_NAME)


n = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
t = str(n.year)+str(n.month)+str(n.day)+str(n.hour)+str(n.minute)+str(n.second)

for msg in consumer:
    # print(msg)
    bucket_file = f"{msg.key.decode()}_{t}.csv"
    
    # test = json.loads(msg.value.decode())
    # print(type(test), test)
    raw_data = pd.DataFrame(json.loads(msg.value.decode()))
    # print(type(df),df)
    csv_buffer = StringIO()
    raw_data.to_csv(csv_buffer, encoding="euc-kr", index=False)
    
    blob = bucket.blob(bucket_file)
    # blob.upload_from_string(csv_buffer.getvalue())
    print(bucket_file, "가 저장됨")
    