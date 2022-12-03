from kafka import KafkaConsumer
import json
import boto3
import datetime
from io import StringIO
import pandas as pd
brokers = ["localhost:9091", "localhost:9092", "localhost:9093"]
TOPIC_NAME = "Apartment"

consumer = KafkaConsumer(TOPIC_NAME, bootstrap_servers=brokers,
                        #  auto_offset_reset='earliest',
                         enable_auto_commit=False,
)

# AWS Config



client = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_DEFAULT_REGION
                      )

BUCKET_NAME = "pipeline-apartment"

n = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
t = str(n.year)+str(n.month)+str(n.day)+str(n.hour)+str(n.minute)+str(n.second)


# args={"ACL":"public-read"}

for message in consumer:
    try:
        s3_path = f"{message.key.decode()}_{t}.csv"
        print(f"data get : {s3_path}")
        
        csv_buffer = StringIO()
        raw_data = pd.DataFrame(json.loads(message.value.decode()))
        
        raw_data.to_csv(csv_buffer, encoding="euc-kr", index=False)
        print(raw_data)
        client.put_object(Body=csv_buffer.getvalue(), Bucket=BUCKET_NAME,Key=s3_path)
        # print(raw_data)
    except Exception as e:
        print(f"error : {e.__str__()}")
        print(e)
        