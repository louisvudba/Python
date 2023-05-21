import os
import json
#import logging
#import logging.handlers
#from pythonjsonlogger import jsonlogger
from datetime import datetime
from libs.common import Common

class Logger:
    def add_fields(self, log_record, record, message_dict):
        super(Logger, self).add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):            
            log_record['timestamp'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if log_record.get('level'):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname

    # @staticmethod
    # def init_logger(loggerName, logFile):
    #     logFilePath = os.path.join(Common.getRootDir(),logFile)
    #     if os.path.isfile(logFilePath):
    #         pass
    #     else:
    #         logger = logging.getLogger(loggerName) 
    #         formatter = Logger.add_fields('%(asctime)s.%(levelname)s {%(module)s} [%(funcName)s].%(message)s')
    #         fh = logging.handlers.RotatingFileHandler(logFilePath, mode='a', maxBytes=1000000, backupCount=5)
    #         fh.setLevel(logging.DEBUG)
    #         fh.setFormatter(formatter)
    #         logger.addHandler(fh)
    #         logger.setLevel(logging.DEBUG)
    #     return logger

    @staticmethod
    def push_logger(result, log_file):
        file_path = os.path.join(Common.get_root_dir(),log_file)
        f = open(file_path, 'a+')
        for item in result:            
            f.write(json.dumps(item))
            f.write("\n")
        f.close()
        