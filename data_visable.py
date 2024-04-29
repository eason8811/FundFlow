import time
from datetime import datetime
from chinese_calendar import is_workday
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymysql
from matplotlib import ticker
from matplotlib.pylab import date2num
from scipy.interpolate import make_interp_spline
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


def create_graph():
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
        sql = ("select total_daily_holding_amount,"
               "total_daily_holding_value,"
               "data_time from total_of_belong_part where belong_part = %s;")
        cursor.execute(sql, (belong_part,))
        result = cursor.fetchall()
        if result == ():
            continue
        result = pd.DataFrame(result, columns=['total_daily_holding_amount', 'total_daily_holding_value', 'data_time'])
        total_amount_parts[belong_part] = result.loc[:, 'total_daily_holding_amount'] / 10000
        total_amount_parts[belong_part].index = result.loc[:, 'data_time']
        total_value_parts[belong_part] = result.loc[:, 'total_daily_holding_value'] / 10000
        total_value_parts[belong_part].index = result.loc[:, 'data_time']

    total_value_parts_rate = {}
    temp = pd.DataFrame(total_value_parts)
    for belong_part in tqdm(temp.columns):
        total_value_part_rate = []
        for i in tqdm(temp.index):
            # 将数据按照固定格式储存
            # {板块1: [日期1比率, 日期2比率...]}
            total_value_part_rate.append(temp.loc[i, belong_part] / sum(list(temp.loc[i, :])))
            # print(df_one_part)
        total_value_parts_rate[belong_part] = pd.Series(total_value_part_rate, index=temp.index)

    num_date_list_of_total = []
    for i in date_list:
        num_date_list_of_total.append(date2num(datetime.strptime(i, '%Y-%m-%d')))

    temp_total_value_parts_rate = {}
    for belong_part in tqdm(part_list):
        temp_total_value_parts_rate[belong_part] = [(sum(np.array(total_value_parts_rate[belong_part]))
                                                     / len(np.array(total_value_parts_rate[belong_part]))), ]
    df_temp_total_value_parts_rate = pd.DataFrame(temp_total_value_parts_rate).transpose()
    df_temp_total_value_parts_rate.sort_values(by=0, ascending=False, inplace=True)
    part_list = list(df_temp_total_value_parts_rate.index)

    # 构造当日持仓情况表格
    column_label = ['板块名称', '总持股数(亿)', '持股市值(亿)', '持股市值比例(%)']
    row_label = part_list
    day_hold_table = []
    for belong_part in row_label:
        row = [belong_part, round(total_amount_parts[belong_part].iloc[-1] / 100000000, 4),
               round(total_value_parts[belong_part].iloc[-1] / 100000000, 4),
               round(total_value_parts_rate[belong_part].iloc[-1] * 100, 2)]
        day_hold_table.append(row)
    day_hold_table = pd.DataFrame(day_hold_table, columns=column_label)
    day_hold_table.to_excel('E:\\公司项目\\FundFlow\\Daily Sector holdings (sorted by sector).xlsx')
    day_hold_table.sort_values(by=column_label[-1], ascending=False).to_excel(
        'E:\\公司项目\\FundFlow\\Daily sector holdings (sorted by proportion of the day).xlsx', columns=column_label)
    day_hold_table = np.array(day_hold_table.sort_values(by=column_label[-1], ascending=False))

    # 进行可视化(amount)
    plt.clf()
    fig = plt.figure(figsize=(32, 175))
    spec = fig.add_gridspec(nrows=31, ncols=3)
    fig.suptitle(f'板块资金流向分析\n{time.strftime("%Y-%m-%d", time.localtime())}', fontsize=80)
    days_num = 0  # 参与统计的天数
    for i in tqdm(range(31)):
        for j in range(3):
            if i == 0:
                ax = fig.add_subplot(spec[0, 1])
                ax.get_yaxis().set_visible(False)
                ax.get_xaxis().set_visible(False)
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['bottom'].set_visible(False)
                ax.spines['left'].set_visible(False)
                np_day_hold_table = np.hstack((day_hold_table[:15], day_hold_table[15:30]))
                np_day_hold_table = np.hstack((np_day_hold_table, day_hold_table[30:45]))
                t = ax.table(cellText=np_day_hold_table, colLabels=column_label * 3, loc='center',
                             cellLoc='center', colColours=['#CDBE70' for _ in range(len(column_label) * 3)])
                t.auto_set_font_size(False)
                t.set_fontsize(20)
                t.scale(4.25, 2)
                break

            if i == 1:
                output = []
                end_time = 0
                start_time = 0
                for part in tqdm(part_list):
                    sql = 'select distinct unixTime from department_stock_info order by unixTime desc limit 15;'
                    cursor.execute(sql)
                    result = list(map(lambda x: x[0], cursor.fetchall()))
                    result.insert(0, int(time.mktime(time.localtime())) // (24 * 60 * 60) * (24 * 60 * 60))
                    for date_i in range(len(result)):
                        if result[date_i] - result[date_i + 1] > 24 * 60 * 60:
                            end_time = result[date_i + 1]
                            start_time = result[date_i + 1 + 5 - 1]
                            break
                    # sql = "select sum(daily_holding_amount) from department_stock_info where belong_part = %s and unixTime <= %s and unixTime >= %s;"
                    sql = "select sum(daily_holding_amount) from department_stock_info where belong_part = %s and unixTime = %s;"
                    cursor.execute(sql, (part, end_time,))
                    result_end_amount = round(cursor.fetchone()[0] / 1000000000000, 2)
                    cursor.execute(sql, (part, start_time,))
                    result_start_amount = round(cursor.fetchone()[0] / 1000000000000, 2)
                    sql = "select sum(daily_holding_value) from department_stock_info where belong_part = %s and unixTime = %s;"
                    cursor.execute(sql, (part, end_time,))
                    result_end_value = round(cursor.fetchone()[0] / 1000000000000, 2)
                    cursor.execute(sql, (part, start_time,))
                    result_start_value = round(cursor.fetchone()[0] / 1000000000000, 2)

                    output.append([part,
                                   str(result_start_amount) + '亿/' + str(result_start_value) + '亿',
                                   str(result_end_amount - result_start_amount) + '亿/' + str(
                                       result_end_value - result_start_value) + '亿',
                                   str(result_end_amount) + '亿/' + str(result_end_value) + '亿',
                                   result_end_amount - result_start_amount])
                df_output = pd.DataFrame(output, columns=['板块', '初始值', '增加值', '结束值', 'sort']).sort_values(
                    by='sort', ascending=False)
                df_output_top10 = df_output.iloc[:10, :-1]
                df_output_bottom10 = df_output.sort_values(by='sort', ascending=True).iloc[:10, :-1]
                table_label = ['板块名称', '总持股数/总市值周初值', '总持股数/总市值增加值', '总持股数/总市值周末值']
                ax = fig.add_subplot(spec[1, 0])
                ax.set_title(f'每周增持榜  {time.strftime("%Y-%m-%d", time.localtime(start_time))} 至 '
                             f'{time.strftime("%Y-%m-%d", time.localtime(end_time))}', fontsize=30)
                ax.get_yaxis().set_visible(False)
                ax.get_xaxis().set_visible(False)
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['bottom'].set_visible(False)
                ax.spines['left'].set_visible(False)
                # np_week_hold_table = np.hstack((np.array(df_output_top10), np.array(df_output_bottom10)))
                t = ax.table(cellText=np.array(df_output_top10), colLabels=table_label, loc='center',
                             cellLoc='center', colColours=['#EE2C2C' for _ in range(len(table_label))])
                t.auto_set_font_size(False)
                t.set_fontsize(20)
                t.scale(1.8, 2)

                ax = fig.add_subplot(spec[1, 2])
                ax.set_title(f'每周减持榜  {time.strftime("%Y-%m-%d", time.localtime(start_time))} 至 '
                             f'{time.strftime("%Y-%m-%d", time.localtime(end_time))}', fontsize=30)
                ax.get_yaxis().set_visible(False)
                ax.get_xaxis().set_visible(False)
                ax.spines['top'].set_visible(False)
                ax.spines['right'].set_visible(False)
                ax.spines['bottom'].set_visible(False)
                ax.spines['left'].set_visible(False)
                # np_week_hold_table = np.hstack((np.array(df_output_top10), np.array(df_output_bottom10)))
                t = ax.table(cellText=np.array(df_output_bottom10), colLabels=table_label, loc='center',
                             cellLoc='center', colColours=['#00cc99' for _ in range(len(table_label))])
                t.auto_set_font_size(False)
                t.set_fontsize(20)
                t.scale(1.8, 2)
                break

            # ax[0, 1].set_title(f'当日外资持仓情况', fontsize=30)
            if i == 2 and j == 0:
                ax = fig.add_subplot(spec[i, j])
                ax.set_title(f'图例', fontsize=30)
                ax.get_yaxis().set_visible(False)
                ax.get_xaxis().set_visible(False)
                line1, = ax.plot([], label='总持股股数', color='#4169E1')
                line2, = ax.plot([], label='持股市值比例', color='orange')
                ax.legend(handles=[line1, line2], loc='center', fontsize=30, frameon=False)  # ['总持股股数', '持股市值比例']
                continue
            if (i - 2) * 3 + j - 1 >= len(list(total_amount_parts.keys())):
                break
            temp = []
            for m in total_amount_parts[part_list[(i - 2) * 3 + j - 1]].index:
                temp.append(datetime.strftime(m, "%Y-%m-%d"))
            ax = fig.add_subplot(spec[i, j])
            ax_kline = ax.twinx()
            temp_i = 0
            color_list = ['#FF0000', '#2E8B57']  # [红色, 绿色]
            for kline in klines_parts[part_list[(i - 2) * 3 + j - 1]].values[-len(temp):]:
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
            ax.plot(temp, total_amount_parts[part_list[(i - 2) * 3 + j - 1]], color='#4169E1')
            ax.set_title(f'{part_list[(i - 2) * 3 + j - 1]}', fontsize=30)
            ax.set_ylabel('总持股数', loc='top', fontsize=16)
            # ax[i, j].spines['right'].set_visible(False)  # ax右轴隐藏
            ax_rate = ax.twinx()  # 创建与轴群ax共享x轴的轴群
            ax_rate.plot(temp, total_value_parts_rate[part_list[(i - 2) * 3 + j - 1]], color='orange')
            ax_rate.yaxis.set_major_formatter(ticker.PercentFormatter(xmax=1, decimals=2))
            ax_rate.set_ylabel('持股市值比例', loc='top', fontsize=16)
            rate_max = max(total_value_parts_rate[part_list[(i - 2) * 3 + j - 1]])
            rate_min = min(total_value_parts_rate[part_list[(i - 2) * 3 + j - 1]])
            amount_max = max(total_amount_parts[part_list[(i - 2) * 3 + j - 1]].values)
            amount_min = min(total_amount_parts[part_list[(i - 2) * 3 + j - 1]].values)
            ax_rate.set_ylim(bottom=rate_min * (1 - 0.15), top=rate_max * (1 + 0.15))
            ax.set_ylim(bottom=amount_min * (1 - 0.15), top=amount_max * (1 + 0.15))
            ax.set_xticks(temp[::len(temp) // 6])
            ax.grid(True)
            days_num = len(temp)

    plt.subplots_adjust(top=0.95, bottom=0.016, hspace=0.35, wspace=0.25)
    plt.savefig(
        f'E:\\公司项目\\FundFlow\\img\\departments_fund_flow\\amount\\Fund_flow_in_the_stock_sector.svg')
    plt.savefig(
        f'E:\\公司项目\\FundFlow\\img\\departments_fund_flow\\amount\\Fund_flow_in_the_stock_sector.png')
    # plt.show()
    cursor.close()
    db.close()

    df_total_amount_parts = pd.DataFrame(total_amount_parts)
    df_total_amount_parts.to_excel(f'E:\\公司项目\\FundFlow\\The total number of shares held in {days_num} days.xlsx',
                                   columns=list(df_total_amount_parts.columns),
                                   index_label=list(df_total_amount_parts.index),
                                   sheet_name='总持股数')
    df_total_value_parts_rate = pd.DataFrame(total_value_parts_rate)
    df_total_value_parts_rate.to_excel(
        f'E:\\公司项目\\FundFlow\\The proportion of total market value in {days_num} days.xlsx',
        columns=list(df_total_value_parts_rate.columns),
        index_label=list(df_total_value_parts_rate.index),
        sheet_name='总市值占比')
    print('已完成')


if __name__ == '__main__':
    create_graph()
