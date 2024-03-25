import time
# 导入selenium包
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import pymysql
from tqdm import tqdm
import requests
import json
import random

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


def pin_url(url, data):
    params = '?'
    for key, item in data.items():
        params += key + '=' + item + '&'
    url += params[:len(params) - 1]
    return url


def date_generater(day_amount=200):
    date_list = []
    init_date_time = int(time.time()) // (24 * 60 * 60) * (24 * 60 * 60) - (8 * 60 * 60) - (24 * 60 * 60)
    date_time = init_date_time
    for i in range(day_amount):
        if (time.localtime(date_time)).tm_wday < 5:
            date_list.append(time.strftime('%Y-%m-%d', time.localtime(date_time)))
        date_time -= 24 * 60 * 60
    date_list.reverse()
    return date_list

def get_departments_mind():

    # 提取储存在数据库内的机构以及机构url
    # db = pymysql.connect(host='localhost', user='root', password='123456', port=3306, db='fund_flow', charset='utf8')
    db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                         password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
    cursor = db.cursor()
    cursor.execute("select * from department_url")
    result = cursor.fetchall()
    date_list = date_generater(day_amount=200)  # 默认200天
    # department = 80
    # date = 129
    amount = 0
    df_department = pd.DataFrame(result, columns=['id', 'department', 'url'])
    for item_url in tqdm(list(df_department.loc[:, 'url'].values)):
        department_code = item_url.split('/')[-1].split('.')[0]
        for date_time_item in tqdm(date_list[-1:]):
            data_time = time.strftime('%Y-%m-%d', time.localtime(time.time() - 24 * 60 * 60))
            url = f'https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery11230796573211467339_{int(time.mktime(time.localtime()) * 1000)}&sortColumns=HOLD_DATE%2CSECURITY_CODE&sortTypes=-1%2C1&pageSize=50&pageNumber=1&reportName=RPT_MUTUAL_HOLD_DET&columns=ALL&source=WEB&client=WEB&filter=(PARTICIPANT_CODE%3D%22{department_code}%22)(MARKET_CODE+in+(%22001%22%2C%22003%22))(HOLD_DATE%3D%27{date_time_item}%27)'
            header = {'User-Agent': random.choice(user_agent_list)}
            respond = requests.get(url=url, headers=header)
            result = json.loads('{' + respond.text.split('({')[1].split(');')[0])['result']
            if result is None or json.loads('{' + respond.text.split('({')[1].split(');')[0])['success'] is False:
                continue
            pages = result['pages']
            for page in range(1, pages + 1):
                url = f'https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery11230796573211467339_{int(time.mktime(time.localtime()) * 1000)}&sortColumns=HOLD_DATE%2CSECURITY_CODE&sortTypes=-1%2C1&pageSize=50&pageNumber={page}&reportName=RPT_MUTUAL_HOLD_DET&columns=ALL&source=WEB&client=WEB&filter=(PARTICIPANT_CODE%3D%22{department_code}%22)(MARKET_CODE+in+(%22001%22%2C%22003%22))(HOLD_DATE%3D%27{date_time_item}%27)'
                header = {'User-Agent': random.choice(user_agent_list)}
                respond = requests.get(url=url, headers=header)
                if respond is None or respond.text == '' or json.loads(
                        '{' + respond.text.split('({')[1].split(');')[0])['success'] is False:
                    break
                result = json.loads('{' + respond.text.split('({')[1].split(');')[0])['result']
                for i in range(len(result['data'])):
                    status = cursor.execute("select belong_part from stock_info where stock_code = %s",
                                            (result['data'][i]['SECURITY_CODE'].split('.')[0],))
                    if not status:
                        break
                    row = {'机构名称': result['data'][i]['ORG_NAME'],
                           '股票代码': result['data'][i]['SECURITY_CODE'].split('.')[0],
                           '股票名称': result['data'][i]['SECURITY_NAME_ABBR'],
                           '今日收盘价': float(result['data'][i]['CLOSE_PRICE']),
                           '今日持股股数': int(float(result['data'][i]['HOLD_NUM']) * 10000),
                           '今日持股市值': int(float(result['data'][i]['HOLD_MARKET_CAP']) * 10000),
                           '所属板块': cursor.fetchone()[0],
                           '交易所': '上海' if (result['data'][i]['SECUCODE'].split('.')[1] == 'SH') else '深圳',
                           'unixTime': int(
                               time.mktime(time.strptime(result['data'][i]['HOLD_DATE'], '%Y-%m-%d %H:%M:%S'))),
                           '数据日期': result['data'][i]['HOLD_DATE']}
                    # row['unixTime'] = int(time.mktime(time.localtime(time.time())))

                    sql = (
                        "insert into department_stock_info("
                        "department_name, stock_code, stock_name, daily_close, daily_holding_amount, "
                        "daily_holding_value, belong_part, exchange, unixTime, data_time)"
                        "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                    )
                    cursor.execute(sql, tuple(row.values()))
                    amount += 1
                    db.commit()

    print(f'录入完成，共 {amount} 条数据')

    cursor.close()
    db.close()


if __name__ == '__main__':
    get_departments_mind()

