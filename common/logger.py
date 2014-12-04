__author__ = 'pcable'

import os
import logging

_logs = {}
_files = {}


def get_formatter():
    return logging.Formatter('%(asctime)s - %(levelname)-7s %(message)s')


def get_logger(name='logger', level=logging.INFO, file_output=None, file_level=logging.DEBUG):
    if name not in _logs:
        logger = logging.getLogger(name)
        logger.setLevel(logging.DEBUG)

        # create console handler and set level to debug
        ch = logging.StreamHandler()
        ch.name = 'console'
        ch.setLevel(level)

        # add formatter to ch
        ch.setFormatter(get_formatter())

        # add ch to logger
        logger.addHandler(ch)
        _logs[name] = logger

    if name not in _files:
        logger = logging.getLogger(name)
        if file_output:
            parent_dir = os.path.dirname(file_output)
            if not os.path.exists(parent_dir):
                os.makedirs(parent_dir)

            fh = logging.FileHandler(file_output)
            fh.name = 'file'
            fh.setFormatter(get_formatter())
            fh.setLevel(file_level)
            logger.addHandler(fh)
            _files[name] = logger

    return _logs[name]


def add_handler(name, dir=".", level=logging.DEBUG):
    #create filehandler for each instrument
    file_path = os.path.join(dir, '%s.log' % name)
    parent_dir = os.path.dirname(file_path)
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
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
