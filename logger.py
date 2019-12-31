#!/usr/bin/env python
# coding=utf-8
import logging
from logging import handlers

class Logger(object):
    level_map = {
        'debug':logging.DEBUG,
        'info':logging.INFO,
        'warning':logging.WARNING,
        'error':logging.ERROR,
        'critical':logging.CRITICAL
    }
    def __init__(self, logfile=None, loggerName=None,when='D',backupCount=3,level='info', fmt='[%(asctime)s-%(levelname)s] %(message)s',loggerType=None):
        self.logger = logging.getLogger(loggerName)
        format = logging.Formatter(fmt=fmt,datefmt="%Y-%m-%d %H:%M:%S")
        self.logger.setLevel(self.level_map.get(level))
        sh = logging.StreamHandler()
        sh.setFormatter(format)
        self.logger.addHandler(sh)
        if logfile != None:
            if loggerType == "timeRotating":
                fh = handlers.TimedRotatingFileHandler(filename=logfile,when=when,interval=1,backupCount=backupCount)
            else:
                fh = logging.FileHandler(logfile,mode='w')
            fh.suffix = "_%Y%m%d.log"
            """
            when:S(second),M(minate),H(hour),D(day),W(week),midnight
            """
            fh.setFormatter(format)
            self.logger.addHandler(fh)
    
    def info(self,message):
        self.logger.info(message)

    def debug(self,message):
        self.logger.debug(message) 

    def error(self,message):
        self.logger.error(message)
