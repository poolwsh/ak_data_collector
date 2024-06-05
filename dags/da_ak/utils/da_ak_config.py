
from dags.utils.config import Config

class DaAkConfig(Config):
    def __init__(self):
        super().__init__()
        self.ZH_A_DEFAULT_START_DATE = "1980-01-01"

# Instantiate the config
daak_config = DaAkConfig()