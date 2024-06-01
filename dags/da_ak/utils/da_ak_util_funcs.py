
import os
import sys
from pathlib import Path
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
print(project_root)
# 将项目根目录添加到sys.path中
sys.path.append(project_root)


import utils.config as con
from utils.ak_utils import AkUtilTools
from utils.logger import logger

from airflow.exceptions import AirflowException

# Logger debug switch
DEBUG_MODE = con.DEBUG_MODE


class DaAkUtilFuncs(AkUtilTools):
    pass