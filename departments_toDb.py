import time
# 导入selenium包
from selenium import webdriver
from selenium.webdriver.common.by import By
import pandas as pd
import pymysql
from tqdm import tqdm

# 打开指定（Firefox）浏览器
browser = webdriver.Edge(executable_path='D:\\Edge download\\edgedriver_win32\\msedgedriver.exe')
# 指定加载页面
browser.get("https://data.eastmoney.com/hsgt/hsgtDetail/jgcx.html")
# 设置五秒后执行下一步
time.sleep(2)
department_list = []
department_url_list = []
i = 1
while True:
    print(i)
    try:
        element = browser.find_element(By.XPATH, f'//*[@id="main_content"]/div[1]/div[12]/div[2]/ul/li[{i}]/a')
    except Exception:
        break
    i += 1
    if element is None or element == '':
        break
    department_list.append(element.text)
    department_url_list.append(element.get_attribute('href'))

browser.find_element(By.XPATH, f'//*[@id="jgpk_filter"]/li[2]').click()
time.sleep(2)
i = 1
while True:
    print(i)
    try:
        element = browser.find_element(By.XPATH, f'//*[@id="main_content"]/div[1]/div[12]/div[3]/ul/li[{i}]/a')
    except Exception as e:
        print(e)
        break
    i += 1
    if element is None or element == '':
        break
    department_list.append(element.text)
    department_url_list.append(element.get_attribute('href'))

df_department_list = pd.DataFrame([department_list, department_url_list]).transpose()
db = pymysql.connect(host='rm-7xv6n273986pzur7e5o.mysql.rds.aliyuncs.com', user='data_process',
                         password='Dp@20220906!!', port=3306, db='zy_fund_flow', charset='utf8')
cursor = db.cursor()

sql = (
    "insert into department_url(department, url) values(%s, %s)"
)

amount = 0
for msg in tqdm(df_department_list.values):
    cursor.execute(sql, tuple(msg))
    amount += 1
    db.commit()
cursor.close()
db.close()
print(f'录入完成，共 {amount} 条数据')

# 关闭浏览器
browser.quit()
