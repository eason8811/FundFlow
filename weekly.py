import datetime
from chinese_calendar import is_workday
import time
import requests
import json
import pymysql
import pandas as pd
from tqdm import tqdm

knife = {'operate_date': '2024-04-17', 'status': True}  # status = False 为不可访问，即今日已经统计过， TRUE 则反


def excute_pys():
    global knife
    while True:
        today_date = datetime.datetime.now()
        today_week = today_date.weekday()
        # 判断今天日期与上一次操作日期是否是同一个日期knife['operate_date']
        if today_date.strftime("%Y-%m-%d") == knife['operate_date']:  # 是同一个日期则代表今天已经统计过，status置False
            knife['status'] = False
        else:  # 不是同一个日期则代表今天还未访问过，status置True
            knife['status'] = True
        # 每天 8:30 - 9:30启动，每周启动一次
        if knife['status'] and ((today_date.hour == 8 and 30 <= today_date.minute) or (
                today_date.hour == 9 and today_date.minute <= 30)):
            last_date = today_date - datetime.timedelta(days=1)
            if is_workday(today_date) and today_week < 5 and not (
                    is_workday(last_date) and last_date.weekday() < 5):  # 如果今天是放完假的第一个交易日
                db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                                     password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
                cursor = db.cursor()
                sql = 'select distinct belong_part from parts_codes;'
                cursor.execute(sql)
                parts = list(map(lambda x: x[0], cursor.fetchall()))
                output = []
                for part in tqdm(parts):
                    sql = 'select distinct unixTime from department_stock_info order by unixTime desc limit 5;'
                    cursor.execute(sql)
                    result = cursor.fetchall()
                    end_time = result[0][0]
                    start_time = result[-1][0]
                    sql = "select sum(daily_holding_amount) from department_stock_info where belong_part = %s and unixTime <= %s and unixTime >= %s;"
                    cursor.execute(sql, (part, end_time, start_time))
                    result = cursor.fetchone()[0]
                    output.append([part, result / 10000])
                df_output = pd.DataFrame(output)
                df_output.to_excel('/var/www/html/data_doc_he/part_nexIn_every_week.xlsx')

                knife['operate_date'] = today_date.strftime("%Y-%m-%d")
        print(f'刷新检测 时间：{today_date.strftime("%Y-%m-%d %H:%M:%S")}')
        time.sleep(60 * 5)
        # time.sleep(1)  # 测试用


if __name__ == '__main__':
    excute_pys()
