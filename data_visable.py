from datetime import datetime
from scipy.interpolate import make_interp_spline
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymysql
from matplotlib import ticker
from matplotlib.pylab import date2num
from tqdm import tqdm

from get_departments_mind import date_generater

plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
plt.rcParams['axes.unicode_minus'] = False  # 用来正常显示负号


# def date_generater(day_amount=200):
#     date_list = []
#     init_date_time = int(time.time()) // (24 * 60 * 60) * (24 * 60 * 60) - (8 * 60 * 60) - (24 * 60 * 60)
#     date_time = init_date_time
#     for i in range(day_amount):
#         if (time.localtime(date_time)).tm_wday < 5:
#             date_list.append(time.strftime('%Y-%m-%d', time.localtime(date_time)))
#         date_time -= 24 * 60 * 60
#     return date_list

def get_x_axis(x_axis, num=5):
    """
    x_axis: 原x轴坐标
    num: x轴坐标个数
    """
    temp = [_ for _ in range(len(x_axis) - 1, 0, len(x_axis) // num * (-1))]
    clear_x_axis = ['' for _ in range(len(x_axis))]
    for i in temp:
        clear_x_axis[i] = x_axis[i]
    return clear_x_axis


def date_to_num(dates):
    num_time = []
    num_date = date2num(dates.iloc[-1])
    for i in range(len(dates)):
        num_time.append(num_date)
        num_date += 1
    return num_time


def smooth_xy(lx, ly):
    """数据平滑处理

    :param lx: x轴数据，数组
    :param ly: y轴数据，数组
    :return: 平滑后的x、y轴数据，数组 [slx, sly]
    """
    x = np.array(lx)
    y = np.array(ly)
    x_smooth = np.linspace(x.min(), x.max(), 300)
    y_smooth = make_interp_spline(x, y)(x_smooth)
    return [x_smooth, y_smooth]


db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                     password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
cursor = db.cursor()

# 获取所有的所属板块
sql = "select distinct belong_part from stock_info;"
cursor.execute(sql)
result = cursor.fetchall()
part_list = []
for item in result:
    belong_part = item[0]
    part_list.append(belong_part)

# 获取所有日期
date_list = date_generater()

# 获取所有机构名称
sql = "select distinct department from department_url;"
cursor.execute(sql)
result = cursor.fetchall()
department_list = []
for item in result:
    department_list.append(item[0])

# 进行可视化数据整理
# for department in tqdm(department_list[:45]):   # 每一个机构分开统计

# 所有机构统一统计
total_amount_parts = {}
total_value_parts = {}
klines_parts = {}
for belong_part in tqdm(part_list):
    total_amount_part = []
    total_value_part = []
    sql = "select data_time, open, close, high, low from parts_kline where belong_part = %s;"
    cursor.execute(sql, (belong_part,))
    result = pd.DataFrame(cursor.fetchall(), columns=['date_time', 'open', 'close', 'high', 'low'])
    klines_parts[belong_part] = result
    # for date_time in tqdm(date_list):
    #     sql = ("select sum(daily_holding_amount), sum(daily_holding_value) from department_stock_info "
    #            "where unixTime = unix_timestamp(%s) and belong_part = %s;")
    #     cursor.execute(sql, (date_time, belong_part))
    #     # 获取每天的所有机构持有的不同板块的股票信息
    #     stocks_of_part = cursor.fetchone()
    #     # 将数据按照固定格式储存
    #     # {板块1: [日期1总量, 日期2总量...]}
    #     total_amount_part.append(stocks_of_part[0] if stocks_of_part[0] is not None else 0)
    #     total_value_part.append(stocks_of_part[1] if stocks_of_part[1] is not None else 0)
    #     # print(df_one_part)
    sql = ("select total_daily_holding_amount,"
           "total_daily_holding_value,"
           "data_time from total_of_belong_part where belong_part = %s;")
    cursor.execute(sql, (belong_part,))
    result = cursor.fetchall()
    if result == ():
        continue
    result = pd.DataFrame(result, columns=['total_daily_holding_amount', 'total_daily_holding_value', 'data_time'])
    total_amount_parts[belong_part] = result.loc[:, 'total_daily_holding_amount']
    total_amount_parts[belong_part].index = result.loc[:, 'data_time']
    total_value_parts[belong_part] = result.loc[:, 'total_daily_holding_value']
    total_value_parts[belong_part].index = result.loc[:, 'data_time']

# temp_i = 0
# while temp_i < len(list(total_amount_parts.values())[0]):
#     if list(total_amount_parts.values())[0][temp_i] == 0:
#         date_list.pop(temp_i)
#         for key in total_amount_parts.keys():
#             total_amount_parts[key].pop(temp_i)
#             total_value_parts[key].pop(temp_i)
#         continue
#     temp_i += 1

total_value_parts_rate = {}
temp = pd.DataFrame(total_value_parts)
for belong_part in tqdm(temp.columns):
    total_value_part_rate = []
    for i in tqdm(temp.index):
        # 将数据按照固定格式储存
        # {板块1: [日期1比率, 日期2比率...]}
        total_value_part_rate.append(temp.loc[i, belong_part] / sum(list(temp.loc[i, :])))
        # print(df_one_part)
    total_value_parts_rate[belong_part] = pd.DataFrame(total_value_part_rate, index=temp.index)

num_date_list_of_total = []
for i in date_list:
    num_date_list_of_total.append(date2num(datetime.strptime(i, '%Y-%m-%d')))

temp_total_value_parts_rate = {}
for belong_part in tqdm(part_list):
    temp_total_value_parts_rate[belong_part] = (sum(np.array(total_value_parts_rate[belong_part].values))
                                                / len(np.array(total_value_parts_rate[belong_part].values)))
df_temp_total_value_parts_rate = pd.DataFrame(temp_total_value_parts_rate).transpose()
df_temp_total_value_parts_rate.sort_values(by=0, ascending=False, inplace=True)
part_list = list(df_temp_total_value_parts_rate.index)

# 进行可视化(amount)
plt.clf()
# plt.figure(figsize=(80, 160))  # 设置图像画布大小
fig, ax = plt.subplots(29, 3, figsize=(25, 125))
fig.suptitle(f'板块资金流向分析', fontsize=80)

for i in tqdm(range(29)):
    for j in range(3):
        if i == 0 and j == 0:
            ax[i, j].set_title(f'图例', fontsize=30)
            ax[i, j].get_yaxis().set_visible(False)
            ax[i, j].get_xaxis().set_visible(False)
            line1, = ax[i, j].plot([], label='总持股股数', color='#4169E1')
            line2, = ax[i, j].plot([], label='持股市值比例', color='orange')
            ax[i, j].legend(handles=[line1, line2], loc='center', fontsize=30, frameon=False)  # ['总持股股数', '持股市值比例']
            continue
        if i * 3 + j - 1 >= len(list(total_amount_parts.keys())):
            break
        temp = []
        for m in total_amount_parts[part_list[i * 3 + j - 1]].index:
            temp.append(datetime.strftime(m, "%Y-%m-%d"))
        ax_kline = ax[i, j].twinx()
        temp_i = 0
        color_list = ['#FF0000', '#2E8B57']  # [红色, 绿色]
        for kline in klines_parts[part_list[i * 3 + j - 1]].values[-len(temp):]:
            kline_open = kline[1]
            kline_close = kline[2]
            kline_high = kline[3]
            kline_low = kline[4]
            color = ''
            if kline_close >= kline_open:
                color = color_list[0]
            else:
                color = color_list[1]
            ax_kline.plot([temp[temp_i], temp[temp_i]], [kline_high, kline_low], color=color, linewidth=0.55,
                          alpha=0.85)
            ax_kline.bar(x=temp[temp_i], height=max(kline_open, kline_close) - min(kline_open, kline_close),
                         bottom=min(kline_open, kline_close), color=color, width=0.5, align='center', alpha=0.85)
            ax_kline.get_yaxis().set_visible(False)
            temp_i += 1
        ax[i, j].plot(temp, total_amount_parts[part_list[i * 3 + j - 1]], color='#4169E1')
        ax[i, j].set_title(f'{part_list[i * 3 + j - 1]}', fontsize=30)
        ax[i, j].set_ylabel('总持股数', loc='top')
        # ax[i, j].spines['right'].set_visible(False)  # ax右轴隐藏
        ax_rate = ax[i, j].twinx()  # 创建与轴群ax共享x轴的轴群
        ax_rate.plot(temp, total_value_parts_rate[part_list[i * 3 + j - 1]], color='orange')
        ax_rate.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1, decimals=2))
        ax_rate.set_ylabel('持股市值比例', loc='top')
        rate_max = max(total_value_parts_rate[part_list[i * 3 + j - 1]].values)[0]
        rate_min = min(total_value_parts_rate[part_list[i * 3 + j - 1]].values)[0]
        amount_max = max(total_amount_parts[part_list[i * 3 + j - 1]].values)
        amount_min = min(total_amount_parts[part_list[i * 3 + j - 1]].values)
        ax_rate.set_ylim(bottom=rate_min * (1 - 0.15), top=rate_max * (1 + 0.15))
        ax[i, j].set_ylim(bottom=amount_min * (1 - 0.15), top=amount_max * (1 + 0.15))
        ax[i, j].set_xticks(temp[::len(temp) // 6])
        ax[i, j].grid(True)

plt.subplots_adjust(top=0.95, bottom=0.016, hspace=0.25, wspace=0.25)
plt.savefig(f'D:\\fund flow\\FundFlow\\img\\departments_fund_flow\\amount\\板块资金流向分析.png')
# plt.show()
cursor.close()
db.close()

print('已完成')
