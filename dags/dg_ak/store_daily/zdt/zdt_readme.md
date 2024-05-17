# 使用akshare接口，读取涨跌停相关数据并保存到对应的表中： 
## stock_zt_pool_dtgc_em
```python
import akshare
from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.strftime('%Y%m%d')
akshare.stock_zt_pool_dtgc_em(date=yesterday_date).columns
```
```shell
Index(['序号', '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值', '总市值', '动态市盈率', '换手率',
       '封单资金', '最后封板时间', '板上成交额', '连续跌停', '开板次数', '所属行业'],
      dtype='object')
```
```python
akshare.stock_zt_pool_dtgc_em(date=yesterday_date).head(3)
```
```shell
	序号	代码	名称	涨跌幅	最新价	成交额	流通市值	总市值	动态市盈率	换手率	封单资金	最后封板时间	板上成交额	连续跌停	开板次数	所属行业
0	1	600505	西昌电力	-9.985528	12.44	1067460960	4.535220e+09	4.535220e+09	-92.706047	22.887924	9949051	144012	207198152	1	5	电力行业
1	2	601878	浙商证券	-10.000000	12.06	3041714640	4.677103e+10	4.677103e+10	26.133562	6.293557	86372296	143854	250563018	1	5	证券
2	3	002166	莱茵生物	-10.000000	9.27	151406910	4.760415e+09	6.878218e+09	64.186279	3.180540	430461358	092500	1447012701	1	0	中药
```

## stock_zt_pool_em
```python
import akshare
from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.strftime('%Y%m%d')
akshare.stock_zt_pool_em(date=yesterday_date).columns
```
```shell
Index(['序号', '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值', '总市值', '换手率', '封板资金',
       '首次封板时间', '最后封板时间', '炸板次数', '涨停统计', '连板数', '所属行业'],
      dtype='object')
```
```python
akshare.stock_zt_pool_em(date=yesterday_date).head(3)
```
```shell
序号	代码	名称	涨跌幅	最新价	成交额	流通市值	总市值	换手率	封板资金	首次封板时间	最后封板时间	炸板次数	涨停统计	连板数	所属行业
0	1	601456	国联证券	10.038241	11.51	42981793	2.749892e+10	3.259371e+10	0.156304	1337651915	092502	092502	0	1/1	1	证券
1	2	002789	建艺集团	10.040567	10.85	54051019	1.423545e+09	1.731915e+09	3.797570	38292926	093000	093000	0	2/2	2	装修装饰
2	3	600355	精伦电子	10.000000	3.85	176756038	1.894543e+09	1.894543e+09	9.410125	62790404	093023	093335	3	2/2	2	通信设备
```

## stock_zt_pool_previous_em
```python
import akshare
from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.strftime('%Y%m%d')
akshare.stock_zt_pool_previous_em(date=yesterday_date).columns
```
```shell
Index(['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '换手率',
       '涨速', '振幅', '昨日封板时间', '昨日连板数', '涨停统计', '所属行业'],
      dtype='object')
```
```python
akshare.stock_zt_pool_previous_em(date=yesterday_date).head(3)
```
```shell
序号	代码	名称	涨跌幅	最新价	涨停价	成交额	流通市值	总市值	换手率	涨速	振幅	昨日封板时间	昨日连板数	涨停统计	所属行业
0	1	603636	南威软件	-2.212389	8.84	9.94	213670603	5.222615e+09	5.222615e+09	4.113678	0.913242	3.207964	142042	1	2/1	互联网服
1	2	603025	大豪科技	-3.372244	14.90	16.96	323345488	1.652670e+10	1.652670e+10	1.934833	0.880163	6.095979	140006	1	2/1	专用设备
2	3	600869	远东股份	-4.259635	4.72	5.42	650122576	1.047534e+10	1.047534e+10	6.159045	0.425532	7.707911	143834	1	4/2	电网设备
```

## stock_zt_pool_strong_em
```python
import akshare
from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.strftime('%Y%m%d')
akshare.stock_zt_pool_strong_em(date=yesterday_date).columns
```
```shell
Index(['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '换手率',
       '涨速', '是否新高', '量比', '涨停统计', '入选理由', '所属行业'],
      dtype='object')
```
```python
akshare.stock_zt_pool_strong_em(date=yesterday_date).head(3)
```
```shell
序号	代码	名称	涨跌幅	最新价	涨停价	成交额	流通市值	总市值	换手率	涨速	是否新高	量比	涨停统计	入选理由	所属行业
0	1	300819	聚杰微纤	20.03035	15.82	15.82	179534004	2.131939e+09	2.360423e+09	9.002662	0.0	1	5.128480	1/1	1	纺织服装
1	2	300159	新研股份	20.00000	2.52	2.52	419409296	3.709354e+09	3.777394e+09	11.726288	0.0	1	1.119653	1/1	1	航天航空
2	3	300641	正丹股份	20.00000	27.60	27.60	2224815056	1.402875e+10	1.403104e+10	18.008133	0.0	1	1.303092	3/2	3	化学制品
```



## stock_zt_pool_sub_new_em
```python
import akshare
from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.strftime('%Y%m%d')
akshare.stock_zt_pool_sub_new_em(date=yesterday_date).columns
```
```shell
Index(['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '转手率',
       '开板几日', '开板日期', '上市日期', '是否新高', '涨停统计', '所属行业'],
      dtype='object')
```
```python
akshare.stock_zt_pool_sub_new_em(date=yesterday_date).head(3)
```
```shell
序号	代码	名称	涨跌幅	最新价	涨停价	成交额	流通市值	总市值	转手率	开板几日	开板日期	上市日期	是否新高	涨停统计	所属行业
0	1	301596	C瑞迪	3.818616	87.00	NaN	844920080	1.198818e+09	4.795272e+09	68.237358	3	2024-05-13	2024-05-13	0	0/0	通用设备
1	2	688530	C欧莱	-4.104804	21.96	NaN	263505749	6.606875e+08	3.514584e+09	38.653934	5	2024-05-09	2024-05-09	0	0/0	半导体
2	3	301539	宏鑫科技	0.040535	24.68	29.6	175518857	8.660599e+08	3.652640e+09	19.899370	20	2024-04-15	2024-04-15	0	0/0	汽车零部
```



## stock_zt_pool_zbgc_em
```python
import akshare
from datetime import datetime, timedelta
yesterday = datetime.now() - timedelta(days=1)
yesterday_date = yesterday.strftime('%Y%m%d')
akshare.stock_zt_pool_zbgc_em(date=yesterday_date).columns
```
```shell
Index(['序号', '代码', '名称', '涨跌幅', '最新价', '涨停价', '成交额', '流通市值', '总市值', '换手率',
       '涨速', '首次封板时间', '炸板次数', '涨停统计', '振幅', '所属行业'],
      dtype='object')
```
```python
akshare.stock_zt_pool_zbgc_em(date=yesterday_date).head(3)
```
```shell
序号	代码	名称	涨跌幅	最新价	涨停价	成交额	流通市值	总市值	换手率	涨速	首次封板时间	炸板次数	涨停统计	振幅	所属行业
0	1	002339	积成电子	-6.571087	7.82	9.21	1675601456	3.744166e+09	3.942002e+09	40.085190	-0.255102	093030	2	5/4	18.040621	电网设备
1	2	300043	星辉娱乐	11.023622	2.82	3.05	553415328	3.506781e+09	3.508639e+09	15.048506	0.000000	093124	1	0/0	10.236220	游戏
2	3	003033	征和工业	5.025126	29.26	30.65	181725911	2.361200e+09	2.392005e+09	7.660889	-0.034165	093445	1	2/1	8.147882	汽车零部
```


