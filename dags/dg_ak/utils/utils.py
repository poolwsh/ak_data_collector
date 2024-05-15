
import pandas as pd
import numpy as np
import json as js
from datetime import date, datetime, timedelta
from subprocess import getstatusoutput

def date_encoder(obj):
    if isinstance(obj, date):
        return obj.isoformat()

    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')

class UtilTools(object):
    @staticmethod
    def dict2json(dict):
        return js.dumps(dict, default=date_encoder)  # .decode('utf-8')

    @staticmethod
    def json2dict(json_str):
        return js.loads(json_str)

    # a中有， b中没有
    # result_list = list_a - list_b
    @staticmethod
    def list_diff(list_a, list_b):
        return list(set(list_a).difference(set(list_b)))

    @staticmethod
    def kill_all_process(process_name):
        ret, val = getstatusoutput("ps -aux | grep {0} | grep -v grep | awk '{{print $2}}'".format(process_name))
        pids = val.replace('\n', ' ')
        ret, val = getstatusoutput("kill -9 " + pids)
        return 'All process {0} has been killed.'.format(process_name)

    # region date time
    @staticmethod
    def gen_quarter_end_day_list(begin_year=1980):
        rp_list = []
        quarter_end_days = ['03-31', '06-30', '09-30', '12-31']
        for year in range(begin_year, date.today().year+1):
            for ed in quarter_end_days:
                rp_list.append(str(year)+'-'+ed)
        return rp_list

    @staticmethod
    def get_quarter_end_day_array(begin_date, end_date=str(datetime.today())):
        quarter_ed_array = np.array(UtilTools.gen_quarter_end_day_list())
        r_array = quarter_ed_array[np.logical_and(quarter_ed_array >= begin_date, quarter_ed_array <= end_date)]
        return r_array

    @staticmethod
    def get_date_before(end_date, slicing_window, date_format='%Y-%m-%d'):
        return str((datetime.strptime(end_date, date_format) - timedelta(days=slicing_window)).date())

    @staticmethod
    def get_today_str(date_format='%Y-%m-%d'):
        return date.today().strftime(date_format)

    @staticmethod
    def get_yesterday_str(date_format='%Y-%m-%d'):
        today = date.today()
        yesterday = today - timedelta(days=1)
        return yesterday.strftime(date_format)
    # endregion date time

    # list_a = [3,5]
    # list_b = [3, 4, 5, 6, 7]
    # result_list = [4, 6, 7]
    @staticmethod
    def remove_elements(list_a, list_b):
        return [x for x in list_b if x not in list_a]

    @staticmethod
    def format_td8(td):
        if isinstance(td, date):
            return td.strftime('%Y%m%d')
        if isinstance(td, str):
            if len(td) == 8:
                try:
                    datetime.strptime(td, '%Y%m%d')
                    return td
                except ValueError:
                    pass
            if len(td) == 10:
                try:
                    datetime.strptime(td, '%Y-%m-%d')
                    return td.replace('-', '')
                except ValueError:
                    pass
        raise ValueError(f"Invalid date format: {td}")

    @staticmethod
    def format_td10(td):
        if isinstance(td, date):
            return td.strftime('%Y-%m-%d')
        if isinstance(td, str):
            if len(td) == 10:
                try:
                    datetime.strptime(td, '%Y-%m-%d')
                    return td
                except ValueError:
                    pass
            if len(td) == 8:
                try:
                    # 将字符串转换为日期对象验证，然后转换为标准格式
                    date_obj = datetime.strptime(td, '%Y%m%d')
                    return date_obj.strftime('%Y-%m-%d')
                except ValueError:
                    pass
        raise ValueError(f"Invalid date format: {td}")

    @staticmethod
    def try_to_td(maybe_td):
        if isinstance(maybe_td, str):
            if len(maybe_td) == 10:
                return datetime.strptime(maybe_td, '%Y-%m-%d')
            elif len(maybe_td) == 8:
                return datetime.strptime(maybe_td, '%Y%m%d')
            else:
                raise ValueError("Unknown td string format, the string format of td should be '%Y-%m-%d' or '%Y%m%d'.")
        if isinstance(maybe_td, date):
            return datetime.strptime(maybe_td.strftime('%Y-%m-%d'), '%Y-%m-%d')
        raise ValueError('Unknown type of td, the type of td should be string or date(time).')

    @staticmethod
    def fibonacci_sequence(min_val, max_val):
        _fib_sequence = [0, 1]
        while _fib_sequence[-1] + _fib_sequence[-2] <= max_val:
            _fib_sequence.append(_fib_sequence[-1] + _fib_sequence[-2])
        # Use set to remove duplicates and then convert back to list
        _fib_sequence = sorted(list(set(_fib_sequence)))
        return [_num for _num in _fib_sequence if min_val <= _num <= max_val]

    @staticmethod
    def get_col_dict(ak_func_json: dict) -> dict:
        removed_elements_list = UtilTools.remove_elements(
            ak_func_json["remove_list"], 
            ak_func_json["org_list"]
        )
        return dict(zip(removed_elements_list, ak_func_json["en_list"]))

    @staticmethod
    def remove_cols(df: pd.DataFrame, ak_func_json: dict) -> pd.DataFrame:
        if len(ak_func_json["remove_list"]) > 0:
            return df.drop(ak_func_json["remove_list"], axis=1, inplace=False)
        return df

    @staticmethod
    def add_td(df: pd.DataFrame, td:any) -> pd.DataFrame:
        df['td'] = td
        df['td'] = df['td'].astype('datetime64[ns]')
        return df

# begin region st
    @staticmethod
    def hhv(input_array, n=None):
        _max_value = input_array.max()
        _max_v_td = input_array.idxmax()
        _td_diff = len(input_array) - input_array.index.get_loc(_max_v_td) - 1
        _chg = (input_array.iloc[-1] - _max_value)/input_array.iloc[-1]
        keys = ['hhv', 'hhv_td', 'hhv_td_diff', 'hhv_chg']
        if n is not None:
            keys = [s.replace('hhv', f'hhv{n}') for s in keys]
        return dict(zip(keys, [_max_value, _max_v_td, _td_diff, _chg]))

    @staticmethod
    def llv(input_array, n=None):
        _min_value = input_array.min()
        _min_v_td = input_array.idxmin()
        _td_diff = len(input_array) - input_array.index.get_loc(_min_v_td) - 1
        _chg = (input_array.iloc[-1] - _min_value)/input_array.iloc[-1]
        keys = ['llv', 'llv_td', 'llv_td_diff', 'llv_chg']
        if n is not None:
            keys = [s.replace('llv', f'llv{n}') for s in keys]
        return dict(zip(keys, [_min_value, _min_v_td, _td_diff, _chg]))

# end region st
    
ak_cols_config = {
    "stock_zt_pool_em": {
        # cmv = circulation market value 流通市值
        # tv = total value 总市值
        "org_list": ['序号', '代码', '名称', '涨跌幅', '最新价', '成交额', '流通市值', '总市值', '换手率', '封板资金', '首次封板时间', '最后封板时间', '炸板次数', '涨停统计', '连板数', '所属行业'],
        "remove_list": ['序号'],
        "en_list": ['s_code', 's_name', 'pct_chg', 'c', 'a',    'circulation_mv',    'total_mv',    'turnover_rate', 'lock_fund', 'first_lock_time', 'last_lock_time', 'failed_account', 'zt_stat', 'zt_account', 'industry']
    }
}




