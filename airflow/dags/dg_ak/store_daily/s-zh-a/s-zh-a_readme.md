# 使用akshare接口，读取大陆A股个股行情相关数据并保存到对应的表中： 
## stock_zh_a_hist
* 目标地址: https://quote.eastmoney.com/concept/sh603777.html?from=classic(示例)
* 描述: 东方财富-沪深京 A 股日频率数据; 历史数据按日频率更新, 当日收盘价请在收盘后获取
* 限量: 单次返回指定沪深京 A 股上市公司、指定周期和指定日期间的历史行情日频率数据
```python
import akshare
from datetime import datetime, timedelta
start_date = (datetime.now() - timedelta(days=31)).strftime('%Y%m%d')
end_date = datetime.now().strftime('%Y%m%d')
adjust = 'hfq'
period = 'daily'
s_code='000004'
stock_zh_a_hist_df = ak.stock_zh_a_hist(symbol=s_code, period=period, start_date=start_date, end_date=end_date, adjust=adjust)
print(stock_zh_a_hist_df.columns)
```
```shell
Index(['日期', '开盘', '收盘', '最高', '最低', '成交量', '成交额', '振幅', '涨跌幅', '涨跌额', '换手率'], dtype='object')
```
```python
print(stock_zh_a_hist_df.head(3))
```
```shell
	日期	开盘	收盘	最高	最低	成交量	成交额	振幅	涨跌幅	涨跌额	换手率
0	2024-04-16	36.95	35.74	37.56	35.74	29236	2.967038e+07	4.52	-11.23	-4.52	2.32
1	2024-04-17	32.43	37.15	38.16	32.43	82135	8.450310e+07	16.03	3.95	1.41	6.50
2	2024-04-18	37.56	37.56	38.36	35.94	70565	7.377003e+07	6.51	1.10	0.41	5.59
```
