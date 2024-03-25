from matplotlib.pylab import date2num
import pymysql
import pandas as pd
from tqdm import tqdm
from decimal import Decimal
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import numpy as np
from chinese_calendar import is_holiday, is_workday
from chinese_calendar import is_in_lieu
import datetime

today_date = datetime.datetime.now().date()
print(today_date)

print('工作日: ', is_workday(today_date))
print('假期日: ', is_holiday(today_date))
# 判断法定节假日是不是调休
print('调休日: ', is_in_lieu(today_date))


