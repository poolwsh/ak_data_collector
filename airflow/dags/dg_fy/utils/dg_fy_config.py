
from dags.utils.config import Config

class DgFyonfig(Config):
    def __init__(self):
        super().__init__()
        self.US_DEFAULT_START_DATE = "1800-01-01"

# Instantiate the config
dgfy_config = DgFyonfig()