from matplotlib.pylab import date2num
import pymysql
import pandas as pd
from tqdm import tqdm
from decimal import Decimal
import matplotlib.pyplot as plt
import matplotlib.pyplot as plt
import numpy as np

fig = plt.figure(figsize=(20, 30))
spec = fig.add_gridspec(nrows=5, ncols=5)

# 绘制跨行列的子图，把一二行的二三列连成一块
ax = fig.add_subplot(spec[:2, 1:3])  # 选中了一二行和二三列
ax.scatter(np.random.rand(15), np.random.rand(15))
plt.savefig(
    f'D:\\fund flow\\FundFlow\\img\\departments_fund_flow\\amount\\test.png')
plt.show()

