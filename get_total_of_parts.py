import pandas as pd
import numpy as np
import pymysql
from tqdm import tqdm
from get_departments_mind import date_generater
import time

def get_total_of_parts():
    db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                         password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
    cursor = db.cursor()
    sql = "select distinct belong_part from stock_info;"
    cursor.execute(sql)
    result = cursor.fetchall()
    part_list = []
    for item in result:
        belong_part = item[0]
        part_list.append(belong_part)

    # 获取所有日期
    date_list = date_generater()[-1:]

    for belong_part in tqdm(part_list):
        total_amount_part = []
        total_value_part = []
        for date_time in tqdm(date_list):
            sql = ("select sum(daily_holding_amount), sum(daily_holding_value) from department_stock_info "
                   "where unixTime = unix_timestamp(%s) and belong_part = %s;")
            cursor.execute(sql, (date_time, belong_part))

            # 获取每天的所有机构持有的不同板块的股票信息
            stocks_of_part = cursor.fetchone()
            if stocks_of_part[0] is None and stocks_of_part[1] is None:
                continue
            elif stocks_of_part[0] is None and stocks_of_part[1] is not None:
                sql = ("insert into total_of_belong_part("
                       "total_daily_holding_amount, total_daily_holding_value, belong_part, unixTime, data_time) "
                       "values (0.0, %s, %s, %s, %s)")
                cursor.execute(sql, (stocks_of_part[1], belong_part,
                                     time.mktime(time.strptime(date_time, '%Y-%m-%d')), date_time))
                db.commit()
            elif stocks_of_part[0] is not None and stocks_of_part[1] is None:
                sql = ("insert into total_of_belong_part("
                       "total_daily_holding_amount, total_daily_holding_value, belong_part, unixTime, data_time) "
                       "values (%s, 0.0, %s, %s, %s)")
                cursor.execute(sql, (stocks_of_part[0], belong_part,
                                     time.mktime(time.strptime(date_time, '%Y-%m-%d')), date_time))
                db.commit()
            else:
                sql = ("insert into total_of_belong_part("
                       "total_daily_holding_amount, total_daily_holding_value, belong_part, unixTime, data_time) "
                       "values (%s, %s, %s, %s, %s)")
                cursor.execute(sql, (stocks_of_part[0], stocks_of_part[1], belong_part,
                                     time.mktime(time.strptime(date_time, '%Y-%m-%d')), date_time))
                db.commit()
    cursor.close()
    db.close()

if __name__ == '__main__':
    get_total_of_parts()
    print('已完成')
