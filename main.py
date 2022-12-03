from datetime import datetime
from yahoo_fin.stock_info import get_data

f = open("crp.txt",'r')

end = datetime.today()
start = datetime(end.year, end.month, end.day-10)

lines = f.readlines()
for line in lines:
    line = line.strip()
    try:
        trend = get_data(f"{line}", start_date=start, end_date=end,
                     index_as_date=False, interval="1d")
        trend.to_csv("/home/steve/Cryptocurrency/Cryp_data/"+line+".csv", index=False)
    except:
        pass