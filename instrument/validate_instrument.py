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

    def record(self, stream, failures):
        failure_types = [x[0] for x in failures]
        self.init_stream(stream)
        if failures:
            self.results[stream]['fail'] += 1
            self.results[stream]['failures'].extend(failures)
        else:
            self.results[stream]['pass'] += 1
        self.results[stream]['expected'] += 1
        if not edex_tools.FAILURES.MISSING_SAMPLE in failure_types:
            self.results[stream]['retrieved'] += 1

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


def diff(a, b, ignore=None, rename=None):
    """
    Compare two data records
    :param a:
    :param b:
    :param ignore: fields to ignore
    :param rename: fields to rename before comparison
    :return: list of failures
    """
    if ignore is None:
        ignore = ['particle_object', 'quality_flag', 'driver_timestamp', 'stream_name', 'preferred_timestamp',
                  'pkt_format_id', 'pkt_version', 'time', 'internal_timestamp']
    if rename is None:
        rename = {'particle_type': 'stream_name'}

    failures = []

    # verify from expected to retrieved
    for k, v in a.iteritems():
        if k in ignore or k.startswith('_'):
            continue
        if k in rename:
            k = rename[k]
        if k not in b:
            failures.append((edex_tools.FAILURES.MISSING_FIELD, k))
            log.error('missing key: %r in retrieved record', k)
            continue
        if type(v) == dict:
            _round = v.get('round')
            value = v.get('value')
            rvalue = round(b[k], _round)
        else:
            value = v
            if type(value) in [str, unicode]:
                value = value.strip()
            rvalue = b[k]
            if type(rvalue) in [str, unicode]:
                rvalue = rvalue.strip()
        if value != rvalue:
            failures.append((edex_tools.FAILURES.BAD_VALUE, 'expected=%r retrieved=%r' % (v, b[k])))
            log.error('non-matching value: expected=%r retrieved=%r', v, b[k])

    # verify no extra (unexpected) keys present in retrieved data
    for k in b:
        if k in ignore:
            continue
        if k not in a:
            failures.append((edex_tools.FAILURES.UNEXPECTED_VALUE, k))
            log.error('item in retrieved data not in expected data: %r', k)

    return failures


def test_results(hostname, instrument, expected_results, scorecard=None, attempts=0):
    num_items = sum([len(x) for x in expected_results.values()])
    log.info('testing %d results (%s), attempt # %d', num_items, type(expected_results), attempts)
    if scorecard is None:
        scorecard = Scorecard()
    not_found = {}
    count = 0
    for stream in expected_results:
        times = sorted(expected_results[stream].keys())[:RECORDS_PER_REQUEST]
        retrieved = edex_tools.get_from_edex(hostname,
                                             stream_name=stream,
                                             sensor=instrument,
                                             start_time=times[0],
                                             stop_time=times[-1])

        for ts, particle in expected_results[stream].iteritems():
            if (ts, stream) not in retrieved:
                if attempts > MAX_ATTEMPTS:
                    scorecard.record(stream, [(edex_tools.FAILURES.MISSING_SAMPLE, (ts, stream))])
                else:
                    not_found.setdefault(stream, {})[ts] = particle
            else:
                scorecard.record(stream, diff(particle, retrieved[(ts,stream)]))

    attempts += 1
    if not_found:
        time.sleep(1)
        test_results(hostname, instrument, not_found, scorecard=scorecard, attempts=attempts)

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
        test_cases = list(TestCase.read_test_cases('instrument_test_cases'))
    else:
        for each in options['<test_cases>']:
            test_cases.extend(list(TestCase.read_test_cases(each)))

    for test_case in test_cases:
        test(test_case, hostname)
