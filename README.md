# 资金流向统计

## 1. 数据爬取

对网站 https://data.eastmoney.com/hsgt/hsgtDetail/ggzj.html 进行数据爬取，记录\
`机构名称`，`股票代码`，`股票名称`，`今日收盘价`，`今日持股股数`，`今日持股市值`，`所属板块`，`交易所`，`unixTime`，`日期`
的字段信息， 并记录到 mysql 数据库中的 `stock_info` 表中。

## 2. 数据可视化

将数据库中的数据读出到内存，按照`所属板块`分类计算每日的增持总额和各个个股的增持份额，并通过
`matplotlib`进行数据呈现。