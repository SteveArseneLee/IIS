import pandas as pd
import xml.etree.ElementTree as ET
import requests
# import datetime
import boto3
import os

code_file = "법정동코드 전체자료/법정동코드 전체자료.txt"
code = pd.read_csv(code_file, sep='\t')

code.columns = ['code', 'name', 'is_exist']
code = code[code['is_exist'] == '존재']

code['code'] = code['code'].apply(str)

year = [str("%02d" %(y)) for y in range(2021,2023)]
month = [str("%02d" %(m)) for m in range(1,13)]
base_date_list = ["%s%s" %(y,m) for y in year for m in month]

def get_data(gu_code, base_date):
    url ="http://openapi.molit.go.kr:8081/OpenAPI_ToolInstallPackage/service/rest/RTMSOBJSvc/getRTMSDataSvcAptTrade?"
    service_key = servicekey    
    payload = "LAWD_CD=" + gu_code + "&" + \
              "DEAL_YMD=" + base_date + "&" + \
              "serviceKey=" + service_key + "&"

    res = requests.get(url + payload)
    
    return res

def get_items(response):
    root = ET.fromstring(response.content)
    item_list = []
    for child in root.find('body').find('items'):
        elements = child.findall('*')
        data = {}
        for element in elements:
            tag = element.tag.strip()
            text = element.text.strip()
            # print tag, text
            data[tag] = text
        item_list.append(data)
    return item_list


gu_list = ["광진구", "강남구", "동작구"]
# gu = "광진구"
for gu in gu_list:
    gu_code = code[(code['name'].str.contains(gu))]
    gu_code = gu_code['code'].reset_index(drop=True)
    gu_code = str(gu_code[0])[0:5]
    print(gu_code)

    items_list = []
    for base_date in base_date_list:
        res = get_data(gu_code, base_date)
        items_list += get_items(res)
        
    len(items_list)
    items = pd.DataFrame(items_list) 
    print(items.head())
    items.to_csv(os.path.join("/home/steve/Apartment/Apartment_batch_data/test/%s_%s~%s.csv" %(gu, year[0], year[-1])), index=False,encoding="euc-kr")
    
dir_path = "/home/steve/Apartment/Apartment_batch_data"
BUCKET_NAME = "apartment-batch"
client = boto3.client('s3',
                      aws_access_key_id=AWS_ACCESS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                      region_name=AWS_DEFAULT_REGION
                      )
for (root, directories, files) in os.walk(dir_path):
    for d in directories:
        d_path = os.path.join(root, d)
        print(d_path)
    for file in files:
        if 'csv' in file:
            client.upload_file(Filename = file, Bucket=BUCKET_NAME, Key = file)
            