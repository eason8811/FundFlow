import requests
import pymysql
import random
import json
import time
import pandas
import numpy
from tqdm import tqdm

user_agent_list = [
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/22.0.1207.1 Safari/537.1",
    "Mozilla/5.0 (X11; CrOS i686 2268.111.0) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.57 Safari/536.11",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1092.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.6 (KHTML, like Gecko) Chrome/20.0.1090.0 Safari/536.6",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/19.77.34.5 Safari/537.1",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.9 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.0) AppleWebKit/536.5 (KHTML, like Gecko) Chrome/19.0.1084.36 Safari/536.5",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_8_0) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1063.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1062.0 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.1 Safari/536.3",
    "Mozilla/5.0 (Windows NT 6.2) AppleWebKit/536.3 (KHTML, like Gecko) Chrome/19.0.1061.0 Safari/536.3",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
    "Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/535.24 (KHTML, like Gecko) Chrome/19.0.1055.1 Safari/535.24",
]

time_stamp = int(time.mktime(time.localtime(time.time()))) * 1000
url = f'https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery11230947834040294975_{time_stamp}&sortColumns=ADD_MARKET_CAP&sortTypes=-1&pageSize=50&pageNumber=1&reportName=RPT_MUTUAL_STOCK_NORTHSTA&columns=ALL&source=WEB&client=WEB&filter=(TRADE_DATE%3D%272024-03-11%27)(INTERVAL_TYPE%3D%221%22)'

header = {'User-Agent': random.choice(user_agent_list)}
respond = requests.get(url=url, headers=header)
print(respond)
result = json.loads('{' + respond.text.split('({')[1].split(');')[0])['result']
total_page = result['pages']
rows = []

if respond:
    for page in tqdm(range(1, total_page + 1)):
        time_stamp = int(time.mktime(time.localtime(time.time()))) * 1000
        url = f'https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery11230947834040294975_{time_stamp}&sortColumns=ADD_MARKET_CAP&sortTypes=-1&pageSize=50&pageNumber={page}&reportName=RPT_MUTUAL_STOCK_NORTHSTA&columns=ALL&source=WEB&client=WEB&filter=(TRADE_DATE%3D%272024-03-11%27)(INTERVAL_TYPE%3D%221%22)'
        # https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery112302971041671593382_{time_stamp}&sortColumns=TRADE_DATE&sortTypes=-1&pageSize=50&pageNumber={page}&columns=ALL&source=WEB&client=WEB&reportName=RPT_MUTUAL_STOCK_HOLDRANKS&filter=(INTERVAL_TYPE%3D%221%22)(RN%3D1)(TRADE_DATE%3D%272024-03-11%27)
        respond = requests.get(url=url, headers=header)
        result = json.loads('{' + respond.text.split('({')[1].split(');')[0])['result']
        for i in range(len(result['data'])):
            row = {}
            row['股票代码'] = result['data'][i]['SECUCODE'].split('.')[0]
            row['股票名称'] = result['data'][i]['SECURITY_NAME']
            row['今日收盘价'] = float(result['data'][i]['CLOSE_PRICE'])
            row['今日持股股数'] = int(float(result['data'][i]['HOLD_SHARES']) * 10000)
            try:
                row['成交均价估计'] = round(
                    float(result['data'][i]['ADD_MARKET_CAP']) / float(result['data'][i]['ADD_SHARES_REPAIR']), 3)
            except ZeroDivisionError:
                print(row['股票代码'], row['股票名称'])
                continue
            row['所属板块'] = result['data'][i]['INDUSTRY_NAME']
            row['交易所'] = '上海' if (result['data'][i]['SECUCODE'].split('.')[1] == 'SH') else '深圳'
            # row['unixTime'] = int(time.mktime(time.localtime(time.time())))
            row['unixTime'] = int(
                time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime(time.time())), '%Y-%m-%d')))
            row['数据日期'] = time.strftime('%Y-%m-%d', time.localtime(time.time()))
            rows.append(row)
df_stock_info = pandas.DataFrame(rows)
# print(df)

# db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, db='fund_flow', charset='utf8')
db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                         password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
cursor = db.cursor()

sql = (
    "insert into stock_info("
    "stock_code, stock_name, daily_close, daily_holding_amount, "
    "deal_price, belong_part, exchange, unixTime, data_time)"
    "values(%s, %s, %s, %s, %s, %s, %s, %s, %s)"
)

amount = 0
for msg in tqdm(df_stock_info.values):
    cursor.execute(sql, tuple(msg))
    amount += 1
    db.commit()
cursor.close()
db.close()
print(f'录入完成，共 {amount} 条数据')
