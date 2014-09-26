__author__ = 'pcable'

import os
import logging

_logs = {}


def get_formatter():
    return logging.Formatter('%(asctime)s - %(name)s - %(levelname)-7s %(message)s')


def get_logger(name, level=logging.INFO, file_output=None, file_level=logging.DEBUG):
    if not name in _logs:
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

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


def create_handler(name):
    #create filehandler for each instrument
    file_path = 'output/%s.log' % name
    fh = logging.FileHandler(file_path)
    fh.setFormatter(get_formatter())
    return fh