# 使用akshare接口，读取大陆A股指数行情相关数据并保存到对应的表中： 

## stock_zh_index_spot_em
* 目标地址: https://quote.eastmoney.com/center/gridlist.html#index_sz
* 描述: 东方财富网-行情中心-沪深京指数
* 限量: 单次返回所有指数的实时行情数据
```python
import akshare
i_df = ak.stock_zh_index_spot_em(symbol="上证系列指数")
i_df.columns
print(i_df.columns)
```
```shell
Index(['序号', '代码', '名称', '最新价', '涨跌幅', '涨跌额', '成交量', '成交额', '振幅', '最高', '最低', '今开', '昨收', '量比'], dtype='object')
```
```python
print(i_df.head(3))
```
```shell
序号	代码	名称	最新价	涨跌幅	涨跌额	成交量	成交额	振幅	最高	最低	今开	昨收	量比
0	1	000917	300公用	2756.12	0.88	24.17	7816622	7.706927e+09	1.27	2764.04	2729.33	2731.93	2731.95	0.97
1	2	000111	380信息	5808.38	0.36	20.60	4066310	1.088880e+10	2.11	5890.35	5768.17	5778.39	5787.78	1.09
2	3	000915	300信息	1849.94	0.35	6.46	13914531	3.689534e+10	1.50	1869.11	1841.40	1841.76	1843.48	0.96
```

## index_zh_a_hist
* 目标地址: http://quote.eastmoney.com/center/hszs.html
* 描述: 东方财富网-中国股票指数-行情数据
* 限量: 单次返回具体指数指定 period 从 start_date 到 end_date 的之间的近期数据
```python
import akshare
from datetime import datetime, timedelta
start_date = (datetime.now() - timedelta(days=31)).strftime('%Y%m%d')
end_date = datetime.now().strftime('%Y%m%d')
i_df = ak.index_zh_a_hist(symbol="000016", period="daily", start_date=start_date, end_date=end_date)
print(i_df.columns)
```
```shell
Index(['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率'], dtype='object')
```
```python
print(i_df.head(3))
```
```shell
	日期	开盘	收盘	最高	最低	成交量	成交额	振幅	涨跌幅	涨跌额	换手率
0	2024-05-06	2495.47	2495.76	2505.60	2487.32	56007811	9.634919e+10	0.74	1.28	31.52	0.38
1	2024-05-07	2496.02	2499.77	2504.03	2489.37	38042631	6.286445e+10	0.59	0.16	4.01	0.26
2	2024-05-08	2495.62	2486.02	2497.15	2484.38	32306111	5.155148e+10	0.51	-0.55	-13.75	0.22
```


