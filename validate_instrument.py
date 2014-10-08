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
            scorecard.record(stream, [(edex_tools.FAILURES.MISSING_SAMPLE, (ts, stream))])
        else:
            scorecard.record(stream, diff(sample, result.values()[0]))

    return scorecard


def flatten(particle):
    for each in particle.get('values'):
        id = each.get('value_id')
        val = each.get('value')
        particle[id] = val
    del (particle['values'])
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
