
import os
import threading
import logging
from logging.handlers import RotatingFileHandler

# 实现 singleton 装饰器
def singleton(cls, *args, **kw):
    instances = {}
    def _singleton():
        if cls not in instances:
            instances[cls] = cls(*args, **kw)
        return instances[cls]
    return _singleton

log_root_dir = '/data/workspace/log/default'

if not os.path.exists(log_root_dir):
    os.makedirs(log_root_dir)

fmt = '%(asctime)s - %(threadName)s - %(funcName)s@%(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s'
format_str = logging.Formatter(fmt)

handlers = {
            logging.NOTSET: os.path.join(log_root_dir, 'notset.log'),
            logging.DEBUG: os.path.join(log_root_dir, 'debug.log'),
            logging.INFO: os.path.join(log_root_dir, 'info.log'),
            logging.WARNING: os.path.join(log_root_dir, 'warning.log'),
            logging.ERROR: os.path.join(log_root_dir, 'error.log'),
            logging.CRITICAL: os.path.join(log_root_dir, 'critical.log'),
        }

def createHandlers():
    logLevels = handlers.keys()
    for level in logLevels:
        path = os.path.abspath(handlers[level])
        handlers[level] = RotatingFileHandler(path, maxBytes=5*1024*1024, backupCount=9, encoding='utf-8')
        handlers[level].setFormatter(format_str)

createHandlers()

class TNLog(object):
    def __init__(self):

        self.__loggers = {}
        logLevels = handlers.keys()
        console_log = logging.StreamHandler()
        console_log.setFormatter(format_str)  # 设置屏幕上显示的格式
        for level in logLevels:
            logger = logging.getLogger(str(level))
            logger.addHandler(handlers[level])
            logger.addHandler(console_log)
            logger.setLevel(level)
            self.__loggers.update({level: logger})

    def notset(self, message):
        # message = self.getLogMessage("notset", message)
        self.__loggers[logging.NOTSET].info(message)

    def info(self, message):
        # message = self.getLogMessage("info", message)
        self.__loggers[logging.INFO].info(message)

    def error(self, message):
        # message = self.getLogMessage("error", message)
        self.__loggers[logging.ERROR].error(message)

    def warning(self, message):
        # message = self.getLogMessage("warning", message)
        self.__loggers[logging.WARNING].warning(message)

    def debug(self, message):
        # message = self.getLogMessage("debug", message)
        self.__loggers[logging.DEBUG].debug(message)

    def critical(self, message):
        # message = self.getLogMessage("critical", message)
        self.__loggers[logging.CRITICAL].critical(message)


@singleton
class LogHelper(object):
    _instance_lock = threading.Lock()
    def __init__(self):
        self.logger = TNLog()

class MQLogger(object):
    def __init__(self):
        # self.logger = PalsurClient('log')
        pass

logger = LogHelper().logger