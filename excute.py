from get_departments_mind import get_departments_mind
from get_parts_kLines import get_parts_kLines
from get_total_of_parts import get_total_of_parts
import datetime
from chinese_calendar import is_workday
import time
import requests
import json


def excute_pys():
    while True:
        today_date = datetime.datetime.now()
        today_week = today_date.weekday()
        # 每天 9:30 - 10:30启动
        if (9 <= today_date.hour and 30 <= today_date.minute) and (today_date.hour <= 10 and today_date.minute <= 30):
            if is_workday(today_date) and today_week < 5:
                # 查找上一个符合这个条件的日期
                last_date = today_date - datetime.timedelta(days=1)
                while not (is_workday(last_date) and last_date.weekday() < 5):
                    last_date -= datetime.timedelta(days=1)
                url = (f'https://datacenter-web.eastmoney.com/api/data/v1/get?callback=jQuery11230796573211467339_'
                       f'{int(time.mktime(time.localtime()) * 1000)}&sortColumns=HOLD_DATE%2CSECURITY_CODE&sortTypes=-1%2C1'
                       f'&pageSize=50&pageNumber=1&reportName=RPT_MUTUAL_HOLD_DET&columns=ALL&source=WEB&client=WEB'
                       f'&filter=(PARTICIPANT_CODE%3D%22{"B01555"}%22)(MARKET_CODE+in+(%22001%22%2C%22003%22))'
                       f'(HOLD_DATE%3D%27{last_date.strftime("%Y-%m-%d")}%27)')
                respond = requests.get(url=url)
                if respond is not None:
                    if json.loads('{' + respond.text.split('({')[1].split(');')[0])['success'] is True:
                        # result = json.loads('{' + respond.text.split('({')[1].split(');')[0])['result']
                        print(f'调用 get_departments_mind() 时间：{today_date.strftime("%Y-%m-%d %H:%M:%S")}')
                        get_departments_mind()
                        print(f'调用 get_parts_kLines(150) 时间：{today_date.strftime("%Y-%m-%d %H:%M:%S")}')
                        get_parts_kLines(150)
                        print(f'调用 get_total_of_parts() 时间：{today_date.strftime("%Y-%m-%d %H:%M:%S")}')
                        get_total_of_parts()
        print(f'刷新检测 时间：{today_date.strftime("%Y-%m-%d %H:%M:%S")}')
        time.sleep(60 * 5)


if __name__ == '__main__':
    excute_pys()
