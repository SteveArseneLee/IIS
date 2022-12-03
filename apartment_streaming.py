import requests
import json
import pandas as pd
import time
def get_apt_info(apt_code):
    down_url = 'https://new.land.naver.com/api/complexes/' + \
        apt_code+'?sameAddressGroup=false'
    r = requests.get(down_url, data={"sameAddressGroup": "false"}, headers={
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2Njg1MDMwMTQsImV4cCI6MTY2ODUxMzgxNH0.z7paw3k8BIR8IOBW7j_33CH5KlHFDNqv0Eq9GJRzH7w",
        "Referer": "https://new.land.naver.com/complexes/"+apt_code+"?ms=37.482968,127.0634,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    })
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    return temp

def get_school_info(apt_code):
    down_url = 'https://new.land.naver.com/api/complexes/'+apt_code+'/schools'
    r = requests.get(down_url,headers={
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2Njg1MDMwMTQsImV4cCI6MTY2ODUxMzgxNH0.z7paw3k8BIR8IOBW7j_33CH5KlHFDNqv0Eq9GJRzH7w",
        "Referer": "https://new.land.naver.com/complexes/"+apt_code+"?ms=37.482968,127.0634,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    },
                    #  verify=False
                     )
    r.encoding = "utf-8-sig"
    temp_school=json.loads(r.text)
    return temp_school

##################가격정보
def apt_price(apt_code,index):
    p_num=temp["complexPyeongDetailList"][index]["pyeongNo"]
    down_url = 'https://new.land.naver.com/api/complexes/'+apt_code+'/prices?complexNo='+apt_code+'&tradeType=A1&year=5&priceChartChange=true&areaNo='+p_num+'&areaChange=true&type=table'

    r = requests.get(down_url,headers={
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6IlJFQUxFU1RBVEUiLCJpYXQiOjE2Njg1MDMwMTQsImV4cCI6MTY2ODUxMzgxNH0.z7paw3k8BIR8IOBW7j_33CH5KlHFDNqv0Eq9GJRzH7w",
        "Referer": "https://new.land.naver.com/complexes/"+apt_code+"?ms=37.4830877,127.0579863,15&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    })
    r.encoding = "utf-8-sig"
    temp_price=json.loads(r.text)
    return temp_price

def get_sido_info():
    down_url = 'https://new.land.naver.com/api/regions/list?cortarNo=0000000000'
    r = requests.get(down_url, data={"sameAddressGroup": "false"}, headers={
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    })
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    temp = list(pd.DataFrame(temp["regionList"])["cortarNo"])
    return temp


def get_gungu_info(sido_code):
    down_url = 'https://new.land.naver.com/api/regions/list?cortarNo='+sido_code
    r = requests.get(down_url, data={"sameAddressGroup": "false"}, headers={
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    })
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    temp = list(pd.DataFrame(temp['regionList'])["cortarNo"])
    return temp


def get_dong_info(gungu_code):
    down_url = 'https://new.land.naver.com/api/regions/list?cortarNo='+gungu_code
    r = requests.get(down_url, data={"sameAddressGroup": "false"}, headers={
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    })
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    temp = list(pd.DataFrame(temp['regionList'])["cortarNo"])
    return temp


def get_apt_list(dong_code):
    down_url = 'https://new.land.naver.com/api/regions/complexes?cortarNo=' + \
        dong_code+'&realEstateType=APT&order='
    r = requests.get(down_url, data={"sameAddressGroup": "false"}, headers={
        "Accept-Encoding": "gzip",
        "Host": "new.land.naver.com",
        "Referer": "https://new.land.naver.com/complexes/102378?ms=37.5018495,127.0438028,16&a=APT&b=A1&e=RETAIL",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.198 Safari/537.36"
    })
    r.encoding = "utf-8-sig"
    temp = json.loads(r.text)
    try:
        temp = list(pd.DataFrame(temp['complexList'])["complexNo"])
    except:
        temp = []
    return temp

def real_func():
    sido_list=get_sido_info()
    for m in range(len(sido_list)):
        gungu_list=get_gungu_info(sido_list[m])
        gungu_apt_list=[0]*len(gungu_list)
        for j in range(len(gungu_list)):#구 마다 하나씩 저장
            dong_list=get_dong_info(gungu_list[j])
            dong_apt_list=[0]*len(dong_list)
            for k in range(len(dong_list)):#동마다 하나씩 저장
                apt_list=get_apt_list(dong_list[k])
                apt_list_data=[0]*len(apt_list)
                for n in range(len(apt_list)):#아파트 마다 하나씩 저장
                    temp=get_apt_info(apt_list[n])
                    try:
                        area_list=temp["complexDetail"]["pyoengNames"].split(", ")
                        ex_flag=1
                    except KeyError:   
                        ex_flag=0
                        temp_data=pd.DataFrame(columns=temp_data.columns)
                    if ex_flag==1:
                        temp_school=get_school_info(apt_list[n])
                        temp_data=pd.DataFrame(index=range(len(area_list)))
                        for i in range(len(area_list)):
                            print(temp["complexDetail"]["address"],temp["complexDetail"]["complexName"])
                            temp_data.loc[i,"아파트명"]=temp["complexDetail"]["complexName"]
                            temp_data.loc[i,"면적"]=area_list[i]
                            temp_data.loc[i,"법정동주소"]=temp["complexDetail"]["address"]+" "+temp["complexDetail"]["detailAddress"]
                            try:
                                temp_data.loc[i,"도로명주소"]=temp["complexDetail"]["roadAddressPrefix"]+" "+temp["complexDetail"]["roadAddress"]
                            except KeyError:
                                temp_data.loc[i,"도로명주소"]=temp["complexDetail"]["roadAddressPrefix"]
                        
                            temp_data.loc[i,"세대수"]=temp["complexDetail"]["totalHouseholdCount"]
                            temp_data.loc[i,"임대세대수"]=temp["complexDetail"]["totalLeaseHouseholdCount"]

                            temp_price=apt_price(apt_list[0],i)
                            try:
                                temp_data.loc[i,"가격"]=temp_price["marketPrices"][0]["dealAveragePrice"]
                            except KeyError:   
                                temp_data.loc[i,"가격"]=""
                            
                            try:
                                temp_data.loc[i,"매매호가"]=temp["complexPyeongDetailList"][i]["articleStatistics"]["dealPriceString"]
                            except KeyError:   
                                temp_data.loc[i,"매매호가"]=""
                            try:
                                temp_data.loc[i,"전세호가"]=temp["complexPyeongDetailList"][i]["articleStatistics"]["leasePriceString"]
                            except KeyError:   
                                temp_data.loc[i,"전세호가"]=""
                            try:
                                temp_data.loc[i,"월세호가"]=temp["complexPyeongDetailList"][i]["articleStatistics"]["rentPriceString"]
                            except KeyError:   
                                temp_data.loc[i,"월세호가"]=""
                            try:
                                temp_data.loc[i,"실거래가"]=temp["complexPyeongDetailList"][i]["articleStatistics"]["rentPriceString"]
                            except KeyError:   
                                temp_data.loc[i,"실거래가"]=""
                            
                        time.sleep(1)
                    apt_list_data[n]=temp_data
                if apt_list_data==[]:
                    dong_apt_list[k]=pd.DataFrame(columns=temp_data.columns)
                else:
                    dong_apt_list[k]=pd.concat(apt_list_data)
            gungu_apt_list[j]=pd.concat(dong_apt_list)
            gungu_apt_list[j].to_csv("/home/steve/Apartment/Apartment_now/"+temp["complexDetail"]["roadAddressPrefix"]+".csv",encoding="utf-8", index=False)
        final_data=pd.concat(gungu_apt_list)

if __name__ == "__main__":
    real_func()