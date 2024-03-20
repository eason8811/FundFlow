from matplotlib.pylab import date2num
import pymysql
import pandas as pd
from tqdm import tqdm
from decimal import Decimal
import matplotlib.pyplot as plt
import numpy as np
from scipy.interpolate import make_interp_spline
import time


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


if __name__ == '__main__':
    x_raw = [6, 7, 8, 9, 10, 11, 12]
    y_raw = [1.53, 5.92, 2.04, 7.24, 2.72, 1.10, 4.70]
    xy_s = smooth_xy(x_raw, y_raw)

    # 原始折线图
    plt.plot(x_raw, y_raw)

    # 处理后的平滑曲线
    plt.plot(xy_s[0], xy_s[1])
    plt.show()
