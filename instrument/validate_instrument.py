#!/usr/bin/env python
"""Instrument Control

Usage:
  validate_instrument.py <host>
  validate_instrument.py <host> <test_cases>...
"""
import os
import sys

instrument_dir = os.path.dirname(os.path.realpath('__file__'))
tools_dir = os.path.dirname(instrument_dir)

sys.path.append(tools_dir)

import time
import yaml
import pprint
import docopt
import instrument_control

from common import logger

log_dir = os.path.join(instrument_dir, 'output_%s' % time.strftime('%Y%m%d-%H%M%S'))
log = logger.get_logger(file_output=os.path.join(log_dir, 'validate_instrument.log'))

MAX_ATTEMPTS = 5
RECORDS_PER_REQUEST = 1000


class TestCase(object):
    def __init__(self, config):
        self.config = config
        self.instrument = config.get('instrument')
        self.startup_config = config.get('startup_config')
        self.script = config.get('script', [])
        self.expected_particles = config.get('expected_particles')
        self.starting_state = config.get('starting_state')

    def __str__(self):
        return pprint.pformat(self.config)

    def __repr__(self):
        return self.config.__repr__()

    @staticmethod
    def read_test_cases(f):
        log.info('Finding test cases in: %s', f)
        if os.path.isdir(f):
            for filename in os.listdir(f):
                config = yaml.load(open(os.path.join(f, filename), 'r'))
                yield TestCase(config)
        elif os.path.isfile(f):
            config = yaml.load(open(f, 'r'))
            yield TestCase(config)




def test(test_case, hostname):
    log.info('Processing test case: %s', test_case)
    controller = instrument_control.Controller(hostname, test_case.instrument)
    controller.initialize_driver(test_case.starting_state,
                                 test_case.startup_config)

    if len(test_case.script) > 0:
        controller.run_script(test_case.script)


if __name__ == '__main__':
    options = docopt.docopt(__doc__)
    hostname = options.get('<host>')
    test_cases = []
    if len(options['<test_cases>']) == 0:
        test_cases = list(TestCase.read_test_cases('test_cases'))
    else:
        for each in options['<test_cases>']:
            test_cases.extend(list(TestCase.read_test_cases(each)))

    for test_case in test_cases:
        test(test_case, hostname)
