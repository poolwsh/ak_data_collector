
import os
import sys
from pathlib import Path
current_path = Path(__file__).resolve().parent 
project_root = os.path.abspath(os.path.join(current_path, '..', '..', '..'))
print(project_root)

sys.path.append(project_root)


from dags.da_ak.utils.da_ak_config import daak_config as con
from dags.utils.dg_utils import AkUtilTools
from utils.logger import logger

from airflow.exceptions import AirflowException

# Logger debug switch
DEBUG_MODE = con.DEBUG_MODE


class DaAkUtilFuncs(AkUtilTools):
    pass