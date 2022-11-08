import pandas as pd
from datetime import datetime
import time

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


period1 = int(time.mktime(datetime(2020,1,1,00,00).timetuple()))
period2 = int(time.mktime(datetime(2022,11,1,23,59).timetuple()))
interval = '1d' # '1d, 1m


# 종목 타입에 따라 download url이 다름. 종목코드 뒤에 .KS .KQ등이 입력되어야해서 Download Link 구분 필요
stock_type = {
    'kospi': 'stockMkt',
    'kosdaq': 'kosdaqMkt'
}

default_args = {
  'start_date': datetime(2022, 1, 1),
}

# 회사명으로 주식 종목 코드를 획득할 수 있도록 하는 함수
def get_code(df, name):
  code = df.query("name=='{}'".format(name))['code'].to_string(index=False)
  
  # 위와같이 code명을 가져오면 앞에 공백이 붙어있는 상황이 발생하여 앞뒤로 sript() 하여 공백 제거
  code = code.strip()
  return code

# download url 조합
def get_download_stock(market_type=None):
  market_type_param = stock_type[market_type]
  download_link = 'http://kind.krx.co.kr/corpgeneral/corpList.do'
  download_link = download_link + '?method=download'
  download_link = download_link + '&marketType=' + market_type_param

  df = pd.read_html(download_link, header=0)[0]
  return df

# kospi 종목코드 목록 다운로드
def get_download_kospi():
  df = get_download_stock('kospi')
  df.종목코드 = df.종목코드.map('{:06d}.KS'.format)
  return df

# kosdaq 종목코드 목록 다운로드
def get_download_kosdaq():
  df = get_download_stock('kosdaq')
  df.종목코드 = df.종목코드.map('{:06d}.KQ'.format)
  return df

def tickers_save():
        # kospi, kosdaq 종목코드 각각 다운로드
    kospi_df = get_download_kospi()
    kosdaq_df = get_download_kosdaq()

    # data frame merge
    code_df = pd.concat([kospi_df, kosdaq_df])

    # data frame정리
    code_df = code_df[['회사명', '종목코드']]

    # data frame title 변경 '회사명' = name, 종목코드 = 'code'
    code_df = code_df.rename(columns={'회사명': 'name', '종목코드': 'code'})

    code_df.to_csv("/home/steve/Capstone2/Korean_stock/Korean_stock.csv",index=False)

def each_data_save():
    test = pd.read_csv("/home/steve/Capstone2/Korean_stock_sample.csv")
    
    for i in range(len(test)):
        print(test.iloc[i,0])
        query_string = f'https://query1.finance.yahoo.com/v7/finance/download/{test.iloc[i,1]}?period1={period1}&period2={period2}&interval={interval}&events=history&includeAdjustedClose=true'
        df = pd.read_csv(query_string)
        print(df)
        df.to_csv("/home/steve/Capstone2/Korean_stock/Korean_stock_data/"+test.iloc[i,0]+".csv", index=False)

if __name__ == "__main__":
    tickers_save()
    each_data_save()
    
    with DAG(dag_id='stock-data-pipeline',
         schedule_interval='@daily',
         default_args=default_args,
         tags=['stock'],
         catchup=False) as dag:
      high = SparkSubmitOperator(
        application="/home/steve/Capstone2/Spark/Korean_stock_high.py", task_id="High_Value", conn_id="stock_spark_local"
      )
      open = SparkSubmitOperator(
        application="/home/steve/Capstone2/Spark/Korean_stock_open.py", task_id="Open_Value", conn_id="stock_spark_local"
      )
      
      high >> open