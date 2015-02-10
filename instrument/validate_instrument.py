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

from common import edex_tools
from common import logger

log_dir = os.path.join(instrument_dir, 'output_%s' % time.strftime('%Y%m%d-%H%M%S'))
log = logger.get_logger(file_output=os.path.join(log_dir, 'validate_instrument.log'))

MAX_ATTEMPTS = 5
RECORDS_PER_REQUEST = 1000


class TestCase(object):
    def __init__(self, config):
        self.config = config
        self.instrument = config.get('instrument')
        self.module = config.get('module')
        self.klass = config.get('klass')
        self.command_port = config.get('command_port')
        self.event_port = config.get('event_port')
        self.port_agent_config = config.get('port_agent_config')
        self.startup_config = config.get('startup_config')
        self.script = config.get('script')
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


class Scorecard(object):
    def __init__(self):
        self.results = {}

    def init_stream(self, stream):
        if stream not in self.results:
            self.results[stream] = {
                'pass': 0,
                'fail': 0,
                'expected': 0,
                'retrieved': 0,
                'failures': []
            }

    def record(self, stream, expected_count, retrieved_count, failures):
        self.init_stream(stream)
        if failures:
            self.results[stream]['fail'] += len(failures)
            self.results[stream]['failures'] += failures
        self.results[stream]['pass'] += expected_count - len(failures)
        self.results[stream]['expected'] += expected_count
        self.results[stream]['retrieved'] += retrieved_count

    def __repr__(self):
        return repr(self.results)

    def __str__(self):
        if not self.results:
            return ''
        base_format_string = "{: <%d} {: >15} {: >15} {: >15} {: >15}"
        result = []
        streams = self.results.keys()
        longest = max([len(x) for x in streams])
        format_string = base_format_string % longest
        banner = format_string.format('Stream', 'Expected', 'Retrieved', 'Pass', 'Fail')
        result.append(banner)
        for stream in sorted(streams):
            my_score = self.results[stream]
            result.append(format_string.format(stream,
                                               my_score['expected'],
                                               my_score['retrieved'],
                                               my_score['pass'],
                                               my_score['fail']))
        return '\n'.join(result)


def test_results(hostname, instrument, expected_results, scorecard=None, CHUNKSIZE=5000):
    num_items = sum([len(x) for x in expected_results.values()])
    log.info('testing %d results (%s)', num_items, type(expected_results))
    if scorecard is None:
        scorecard = Scorecard()

    subsite, node, sensor = instrument.split('-', 3)
    metadata = edex_tools.get_edex_metadata('localhost', subsite, node, sensor)

    for stream in expected_results:
        # slice up the stream into manageable chunks, query and compare
        for start in xrange(0, len(expected_results[stream]), CHUNKSIZE):
            stop = start + CHUNKSIZE
            stop = len(expected_results[stream]) if stop > len(expected_results[stream]) else stop
            slice = expected_results[stream][start:stop]
            times = [x.get(x.get('preferred_timestamp'), 0.0) for x in slice]

            retrieved = edex_tools.get_from_edex(hostname,
                                                 subsite,
                                                 node,
                                                 sensor,
                                                 'streamed',
                                                 stream,
                                                 times[0]-1,
                                                 times[-1]+1,
                                                 timestamp_as_string=True)

            failures = edex_tools.compare(retrieved, slice, metadata, lookup_preferred_timestamp=True)
            scorecard.record(stream, len(slice), len(retrieved), failures)

    return scorecard


def test(test_case, hostname):
    scorecard = {}
    log.debug('Processing test case: %s', test_case)
    controller = instrument_control.Controller(hostname,
                                               test_case.instrument,
                                               test_case.module,
                                               test_case.klass,
                                               test_case.command_port,
                                               test_case.event_port)
    controller.initialize_driver(test_case.starting_state,
                                 test_case.port_agent_config,
                                 test_case.startup_config)
    controller.run_script(test_case.script)
    # ensure ALL particles have been persisted (accumulator currently configured to publish every 5s)
    time.sleep(5)
    scorecard[test_case.instrument] = test_results(hostname, test_case.instrument, controller.samples)

    for instrument, card in scorecard.iteritems():
        print
        print instrument
        print
        print card


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
