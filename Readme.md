# Details of Pipeline/Stock
Stock의 디렉토리에 대한 설명
- **Stock_screener에는 전세계와 미국 나스닥의 ticker들이 있다.**
- **Tickers에는 각 회사별로 history를 저장해두었다.**
- **Korean_stock에는 국내 기업의 Kospi와 Kosdaq상장 기업들의 리스트가 있다.**

<br>

## 본 프로젝트에서는 Korean_stock의 kospi와 kosdaq의 일부 데이터만을 이용할 예정이다.


<br>

### Requirements
- pip install apache-airflow
- pip install apache-airflow-providers-apache-spark
- pip install airflow-provider-kafka