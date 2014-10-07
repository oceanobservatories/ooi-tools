#!/usr/bin/env python
"""Instrument Control

Usage:
  validate_instrument.py <host>
  validate_instrument.py <host> <test_cases>...
"""

import os
import pprint
import docopt
import yaml
import json
import edex_tools
import logger
import instrument_control


log = logger.get_logger('validate_instrument', file_output='output/validate_instrument.log')


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
                'failures': []
            }

    def record(self, stream, failures):
        self.init_stream(stream)
        if failures:
            self.results[stream]['fail'] += 1
            self.results[stream]['failures'].extend(failures)
        else:
            self.results[stream]['pass'] += 1

    def __str__(self):
        return pprint.pformat(self.results)

def compare(stored, expected):
    """
    Compares a set of expected results against the retrieved values
    :param stored:
    :param expected:
    :return: list of failures
    """

    failures = []
    for record in expected:
        preferred_timestamp = record.get('preferred_timestamp')
        timestamp = record.get(preferred_timestamp)
        stream_name = record.get('stream_name')
        # Not all YAML files contain the particle type
        # if we don't find it, let's check the stored data
        # if all particles are the same type, then we'll proceed
        if stream_name is None:
            log.warn('Missing stream name from YML file, attempting to infer')
            keys = stored.keys()
            keys = [x[1] for x in keys]
            keys = set(keys)
            if len(keys) == 1:
                key = (timestamp, keys.pop())
            else:
                failures.append((edex_tools.FAILURES.AMBIGUOUS, 'Multiple streams in output, no stream in YML'))
                log.error('Ambiguous stream information in YML file and unable to infer')
                continue
        else:
            key = (timestamp, stream_name)
        if key not in stored:
            failures.append((edex_tools.FAILURES.MISSING_SAMPLE, key))
            log.error('No matching record found in retrieved data for key %s', key)
        else:
            f = diff(record, stored.get(key))
            if f:
                failures.append(f)
    return failures


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
                  'pkt_format_id', 'pkt_version', 'time']
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
            log.error('missing key: %s in retrieved record', k)
            continue
        if type(v) == dict:
            _round = v.get('round')
            value = v.get('value')
            rvalue = round(b[k], _round)
        else:
            value = v
            rvalue = b[k]
        if value != rvalue:
            failures.append((edex_tools.FAILURES.BAD_VALUE, 'expected=%s retrieved=%s' % (v, b[k])))
            log.error('non-matching value: expected=%s retrieved=%s', v, b[k])

    # verify no extra (unexpected) keys present in retrieved data
    for k in b:
        if k in ignore:
            continue
        if k not in a:
            failures.append((edex_tools.FAILURES.UNEXPECTED_VALUE, k))
            log.error('item in retrieved data not in expected data: %s', k)

    return failures


def test_results(hostname, instrument, results):
    scorecard = Scorecard()
    for sample in results:
        log.debug('Expected: %s', sample)
        sample = flatten(sample)
        stream = sample.get('stream_name')
        ts = sample.get(sample.get('preferred_timestamp'))
        result = edex_tools.get_from_edex(hostname,
                                          stream_name=stream,
                                          sensor=instrument,
                                          start_time=ts,
                                          stop_time=ts)
        log.debug('Expected: %s', sample)
        log.debug('Retrieved: %s', result)
        retrieved = result.values()
        if len(result.values()) == 0:
            scorecard.record(stream, ['Missing sample: '])
        else:
            scorecard.record(stream, diff(sample, result.values()[0]))

    print scorecard


def flatten(particle):
    for each in particle.get('values'):
        id = each.get('value_id')
        val = each.get('value')
        particle[id] = val
    del(particle['values'])
    return particle


def get_expected(filename):
    """
    Loads expected results from the supplied YAML file
    :param filename:
    :return: list of records containing the expected results
    """
    return_data = {}
    for each in open(filename, 'r').read().split('\n\n'):
        if not each:
            continue
        data = json.loads(each)
        if data is not None:
            particle = flatten(data)
            stream = particle.get('stream_name')

            if stream is not None:
                return_data.setdefault(stream, []).append(particle)

    return return_data


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
    test_results(hostname, test_case.instrument, controller.samples)


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
