import os
import sys
import numpy as np
import pandas as pd
import datetime
from pathlib import Path
from typing import Callable, Dict, List, Tuple
import logging

current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
sys.path.append(project_root)

from dags.da_ak.utils.da_ak_config import daak_config as con
from dags.utils.dg_utils import AkUtilTools
from utils.logger import logger

from airflow.exceptions import AirflowException

# Logger debug switch
DEBUG_MODE = con.DEBUG_MODE

class DaAkUtilFuncs(AkUtilTools):

    @staticmethod
    def calculate_ma(series: pd.Series, window: int) -> pd.Series:
        """
        Calculate the moving average for a specified column over a given window.
        
        Args:
            series (pd.Series): Series of values to calculate the moving average for.
            window (int): Window size for the moving average.
        
        Returns:
            pd.Series: Series with the moving average values.
        """
        return series.rolling(window=window).mean()

    @staticmethod
    def gen_board_cons_dict(board_data: pd.DataFrame) -> Dict[datetime.date, Dict[str, List[str]]]:
        """
        Process board constituent information.
        
        Args:
            board_data (pd.DataFrame): DataFrame containing board data with 'td', 'b_name', and 's_code' columns.
        
        Returns:
            Dict[datetime.date, Dict[str, List[str]]]: Dictionary with dates as keys and dictionaries of board names and stock codes as values.
        """
        board_cons = {}
        grouped = board_data.groupby('td')

        for date, group in grouped:
            if group.empty:
                logger.warning(f"No data for date {date}, skipping.")
                continue
            board_cons[date] = {}
            for _, row in group.iterrows():
                b_name = row['b_name']
                s_code = row['s_code']
                if b_name not in board_cons[date]:
                    board_cons[date][b_name] = []
                board_cons[date][b_name].append(s_code)

        return board_cons

    @staticmethod
    def calculate_grouped_value_by_date(
        df: pd.DataFrame,
        group_dict: Dict[datetime.date, Dict[str, List[str]]],
        func: Callable[[np.ndarray], float],
        index_col: str,
        value_col: str,
        result_columns: List[str] = ['group', 'result']
    ) -> pd.DataFrame:
        """
        Calculate grouped values by date.
        
        Args:
            df (pd.DataFrame): DataFrame containing the data.
            group_dict (Dict[datetime.date, Dict[str, List[str]]]): Dictionary with group information.
            func (Callable[[np.ndarray], float]): Function to apply to each group.
            index_col (str): Column name to use as the index for grouping.
            value_col (str): Column name to use as the values for the function.
            result_columns (List[str]): Column names for the result DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame with the grouped and calculated values.
        """
        def _col_value(_traverse_values: List[datetime.date], _use_latest: bool) -> List[pd.DataFrame]:
            """
            Apply the function to the specified date range.
            
            Args:
                _traverse_values (List[datetime.date]): List of dates to traverse.
                _use_latest (bool): Whether to use the latest available group information.
            
            Returns:
                List[pd.DataFrame]: List of DataFrames with the calculated values.
            """
            _result = []
            for _val in _traverse_values:
                logger.debug(f'Processing date {_val}')
                _group_info = group_dict.get(_val)
                if _group_info is None:
                    try:
                        if _use_latest:
                            _nearest_date = max(_date for _date in group_dict.keys() if _date <= _val)
                            _group_info = group_dict[_nearest_date]
                            logger.debug(f'No group info found for {_val}, using nearest earlier date {_nearest_date}')
                        else:
                            _nearest_date = min(_date for _date in group_dict.keys() if _date >= _val)
                            _group_info = group_dict[_nearest_date]
                            logger.debug(f'No group info found for {_val}, using nearest later date {_nearest_date}')
                    except ValueError:
                        logger.warning(f'No suitable group info found for td={_val}')
                        continue
                else:
                    logger.debug(f'Using group info for {_val}: {_group_info}')
                for _group, _index_list in _group_info.items():
                    _filtered_data = df[df['td'] == _val]
                    _group_result = DaAkUtilFuncs.group_and_apply(_filtered_data, {_group: _index_list}, func, index_col, value_col, result_columns)
                    _group_result['td'] = _val
                    _result.append(_group_result)
            return _result

        df = df.copy()
        df['td'] = pd.to_datetime(df['td']).dt.date

        # Debug log for group_dict keys before converting
        logger.debug(f'Original group_dict keys: {list(group_dict.keys())}')

        # Ensure group_dict keys are of datetime.date type
        group_dict = {pd.to_datetime(k).date(): v for k, v in group_dict.items()}

        # Debug log for group_dict keys after converting
        logger.debug(f'Converted group_dict keys: {list(group_dict.keys())}')

        traverse_values = sorted(df['td'].unique())
        oldest_group_date = min(group_dict.keys())
        latest_group_date = max(group_dict.keys())
        logger.info(f'Oldest group date: {oldest_group_date}, Latest group date: {latest_group_date}')
        
        # 分成两部分：一部分是小于等于 oldest_group_date 的，另一部分是大于 oldest_group_date 的
        newer_traverse_values = [val for val in traverse_values if val >= oldest_group_date]
        older_traverse_values = [val for val in traverse_values if val < oldest_group_date]
        
        # 处理 traverse_values
        r_list = _col_value(older_traverse_values, False) + _col_value(newer_traverse_values, True)
        return pd.concat(r_list).reset_index(drop=True)

    @staticmethod
    def group_and_apply(
        df: pd.DataFrame,
        groups_dict: Dict[str, List[str]],
        func: Callable[[np.ndarray], float],
        df_index_name: str,
        df_value_name: str,
        result_columns: List[str] = ['group', 'result']
    ) -> pd.DataFrame:
        """
        Group and apply a function to the DataFrame.
        
        Args:
            df (pd.DataFrame): DataFrame containing the data.
            groups_dict (Dict[str, List[str]]): Dictionary with group information.
            func (Callable[[np.ndarray], float]): Function to apply to each group.
            df_index_name (str): Column name to use as the index for grouping.
            df_value_name (str): Column name to use as the values for the function.
            result_columns (List[str]): Column names for the result DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame with the grouped and calculated values.
        """
        # 将 DataFrame 转换为 NumPy 数组
        df_index_array = df[df_index_name].values
        df_value_array = df[df_value_name].values

        result = {}
        for key, values in groups_dict.items():
            # 使用 NumPy 的向量化操作进行过滤
            mask = np.isin(df_index_array, values)
            subset_values = df_value_array[mask]
            # 对过滤后的数据应用传入的函数
            result[key] = func(subset_values)

        # 将结果转换为 DataFrame
        result_df = pd.DataFrame(list(result.items()), columns=result_columns)
        return result_df
