import numpy as np
import pandas as pd
import requests
import pymysql
import time
import random
import json
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


def get_parts_code():
    # 获取86个板块的板块代码，用于获取url
    header = {'User-Agent': random.choice(user_agent_list)}
    respond = requests.get(f'https://14.push2.eastmoney.com/api/qt/clist/get?'
                           f'cb=jQuery112406345594699607533_{int(time.mktime(time.localtime()) * 1000)}&'
                           f'pn={1}&'
                           f'pz={86}&'
                           f'po=1&'
                           f'np=1&'
                           f'ut=bd1d9ddb04089700cf9c27f6f7426281&fltt=2&'
                           f'invt=2&'
                           f'wbp2u=|0|0|0|web&'
                           f'fid=f3&'
                           f'fs=m:90+t:2+f:!50&'
                           f'fields=f1,f2,f3,f4,f5,f6,f7,f8,f9,f10,f12,f13,f14,f15,f16,f17,f18,f20,f21,f23,f24,f25,f26,f22,f33,f11,f62,f128,f136,f115,f152,f124,f107,f104,f105,f140,f141,f207,f208,f209,f222&'
                           f'_={int(time.mktime(time.localtime()) * 1000)}',
                           headers=header)
    result = json.loads('{' + respond.text.split('({')[1].split(');')[0])
    # 提取出板块代码并储存到数据库(板块代码基本不会变化)
    db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                         password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
    cursor = db.cursor()
    for item in tqdm(result['data']['diff']):
        part_name = item['f14']
        part_code = item['f12']
        sql = "insert into parts_codes(belong_part, part_code) values(%s, %s);"
        cursor.execute(sql, (part_name, part_code))
        db.commit()
    print('已完成')
    cursor.close()
    db.close()


def get_parts_kLines(kLine_num=150):
    db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                         password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
    cursor = db.cursor()
    sql = "select * from parts_codes;"
    cursor.execute(sql)
    parts_list = cursor.fetchall()
    for item in tqdm(parts_list):
        part_name = item[1]
        part_code = item[2]
        url = (f'https://push2his.eastmoney.com/api/qt/stock/kline/get?'
               f'cb=jQuery35108092294390327979_{int(time.mktime(time.localtime()) * 1000)}&'
               f'secid=90.{part_code}&ut=fa5fd1943c7b386f172d6893dbfba10b&fields1=f1%2Cf2%2Cf3%2Cf4%2Cf5%2Cf6&fields2=f51%2Cf52%2Cf53%2Cf54%2Cf55%2Cf56%2Cf57%2Cf58%2Cf59%2Cf60%2Cf61&klt=101&fqt=1&end=20500101&lmt={kLine_num}&_={int(time.mktime(time.localtime()) * 1000)}')
        header = {'User-Agent': random.choice(user_agent_list)}
        respond = requests.get(url, headers=header)
        kLines = json.loads('{' + respond.text.split('({')[1].split(');')[0])['data']['klines']
        for kLine in tqdm(kLines[-2:-1]):
            kLine_datas = kLine.split(',')
            data_time = kLine_datas[0]
            kLine_open = float(kLine_datas[1])
            kLine_close = float(kLine_datas[2])
            kLine_high = float(kLine_datas[3])
            kLine_low = float(kLine_datas[4])
            kLine_amount = float(kLine_datas[5])
            kLine_value = float(kLine_datas[6])
            kLine_change_rate = float(kLine_datas[-1])
            sql = ("insert into parts_kline(belong_part, data_time, unixTime, "
                   "open, close, high, low, amount, value, change_rate) "
                   "values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            cursor.execute(sql, (
                part_name, data_time, time.mktime(time.strptime(data_time, '%Y-%m-%d')), kLine_open, kLine_close,
                kLine_high, kLine_low, kLine_amount, kLine_value, kLine_change_rate))
            db.commit()
    cursor.close()
    db.close()


if __name__ == '__main__':
    # get_parts_code()
    get_parts_kLines(150)
    print('已完成')
