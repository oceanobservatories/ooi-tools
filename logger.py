__author__ = 'pcable'

import os
import logging

_logs = {}


def get_formatter():
    return logging.Formatter('%(asctime)s - %(name)s - %(levelname)-7s %(message)s')


def get_logger(name, level=logging.INFO, file_output=None, file_level=logging.DEBUG):
    if not name in _logs:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.name = 'console'
        ch.setLevel(level)

        # create formatter
        formatter = get_formatter()

        # add formatter to ch
        ch.setFormatter(formatter)

        # add ch to logger
        logger.addHandler(ch)

        if file_output:
            parent_dir = os.path.dirname(file_output)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)

            fh = logging.FileHandler(file_output)
            fh.setFormatter(formatter)
            fh.setLevel(file_level)
            logger.addHandler(fh)

        _logs[name] = logger
    return _logs[name]


def add_handler(name, level=logging.DEBUG):
    #create filehandler for each instrument
    file_path = 'output/%s.log' % name
    fh = logging.FileHandler(file_path)
    fh.name = name
    fh.setLevel(level)
    fh.setFormatter(get_formatter())
    for logger in _logs.values():
        logger.addHandler(fh)


def remove_handler(name):
    for logger in _logs.values():
        for handler in logger.handlers:
            if handler.name == name:
                logger.removeHandler(handler)
