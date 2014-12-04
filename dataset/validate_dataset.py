#!/usr/bin/env python
"""Validate dataset

Usage:
  validate_dataset.py [--ignore_null]
  validate_dataset.py [--ignore_null] <test_case>...

Options:
  --ignore_null  Don't fail on missing null values

"""
import os
import sys


dataset_dir = os.path.dirname(os.path.realpath('__file__'))
tools_dir = os.path.dirname(dataset_dir)

sys.path.append(tools_dir)

import time
import math
import Queue
import pprint
import ntplib
import docopt
import calendar

from threading import Thread
from qpid.messaging.exceptions import NotFound
from datetime import datetime
from yaml import load
from common import logger
from common import edex_tools

try:
    from yaml import CLoader as Loader
except ImportError:
    from yaml import Loader, Dumper


NUM_THREADS = 30
FLOAT_TOLERANCE = 0.001
IGNORE_NULLS = False


startdir = os.path.join(edex_tools.edex_dir, 'data/utility/edex_static/base/ooi/parsers/mi-dataset/mi')
drivers_dir = os.path.join(startdir, 'dataset/driver')
ingest_dir = os.path.join(edex_tools.edex_dir, 'data', 'ooi')

output_dir = os.path.join(dataset_dir, 'output_%s' % time.strftime('%Y%m%d-%H%M%S'))

if not os.path.exists(output_dir):
    os.makedirs(output_dir)

log = logger.get_logger(file_output=os.path.join(output_dir, 'everything.log'))


class TestCase(object):
    def __init__(self, config):
        self.config = config
        self.instrument = config.get('instrument')
        self.resource = os.path.join(drivers_dir, config.get('resource'))
        self.endpoint = os.path.join(ingest_dir, config.get('endpoint'))
        self.pairs = config.get('pairs', [])
        self.rename = config.get('rename', True)
        # Attempt to obtain a timeout value from the test_case yml.  Default it to
        # DEFAULT_STANDARD_TIMEOUT if no yml value was provided.
        self.timeout = config.get('timeout', edex_tools.DEFAULT_STANDARD_TIMEOUT)
        self.sensors = []

    def __str__(self):
        return pprint.pformat(self.config)

    def __repr__(self):
        return self.config.__repr__()


def read_test_cases(f):
    log.info('Finding test cases in: %s', f)
    if os.path.isdir(f):
        for filename in os.listdir(f):
            config = load(open(os.path.join(f, filename), 'r'), Loader=Loader)
            yield TestCase(config)
    elif os.path.isfile(f):
        config = load(open(f, 'r'), Loader=Loader)
        yield TestCase(config)


def get_expected(filename):
    """
    Loads expected results from the supplied YAML file
    :param filename:
    :return: list of records containing the expected results
    """
    try:
        fh = open(filename, 'r')
        data = load(fh, Loader=Loader)
        log.debug('Raw data from YAML: %s', data)
        header = data.get('header')
        data = data.get('data')
        particle_type = header.get('particle_type')
        if particle_type is not None:
            if particle_type != 'MULTIPLE':
                for record in data:
                    record['particle_type'] = particle_type
    except (IOError, KeyError):
        data = []

    for record in data:
        timestamp = record.get('internal_timestamp')
        if type(timestamp) == str:
            if not timestamp.endswith('Z'):
                timestamp += 'Z'
            # Check to see if we have a timestamp with a decimal point which
            # means we have the millis included
            if '.' in timestamp:
                timestamp_to_use = timestamp
            else:
                # So now we will add the millis, eliminating the Z (i.e. [:-1])
                # and reappend the Z
                timestamp_to_use = timestamp[:-1] + '.0Z'
            dt = datetime.strptime(timestamp_to_use, '%Y-%m-%dT%H:%M:%S.%fZ')
            timestamp = ntplib.system_to_ntp_time(
                calendar.timegm(dt.timetuple()) + (dt.microsecond / 1000000.0))

            record['internal_timestamp'] = timestamp

    expected_dictionary = {}

    for record in data:
        expected_dictionary.setdefault(record.get('particle_type'), []).append(record)

    return expected_dictionary


def same(a, b):
    string_types = [str, unicode]
    # log.info('same(%r,%r) %s %s', a, b, type(a), type(b))
    if a == b:
        return True

    if type(a) is not type(b):
        if type(a) not in string_types and type(b) not in string_types:
            return False

    if type(a) is dict:
        if a.keys() != b.keys():
            return False
        return all([same(a[k], b[k]) for k in a])

    if type(a) is list:
        if len(a) != len(b):
            return False
        return all([same(a[i], b[i]) for i in xrange(len(a))])

    if type(a) is float or type(b) is float:
        try:
            if type(a) is unicode:
                a = str(a)
            if type(b) is unicode:
                b = str(b)
            a = float(a)
            b = float(b)
            if abs(a-b) < FLOAT_TOLERANCE:
                return True
            if math.isnan(a) and math.isnan(b):
                return True
        except:
            pass
        log.info('FAILED floats: %r %r', a, b)

    if type(a) in string_types and type(b) in string_types:
        return a.strip() == b.strip()

    return False


def compare(stored, expected):
    """
    Compares a set of expected results against the retrieved values
    :param stored:
    :param expected:
    :return: list of failures
    """
    failures = []
    for record in expected:
        timestamp = '%12.3f' % record.get('internal_timestamp')
        stream_name = record.get('particle_type')
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
            f = diff(stream_name, record, stored.get(key))
            if f:
                failures.append(f)
    return failures


def diff(stream, a, b, ignore=None, rename=None):
    """
    Compare two data records
    :param a:
    :param b:
    :param ignore: fields to ignore
    :param rename: fields to rename before comparison
    :return: list of failures
    """
    if ignore is None:
        ignore = ['particle_object', 'quality_flag', 'driver_timestamp', 'stream_name', 'preferred_timestamp']
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
            message = '%s - missing key: %s in retrieved record' % (stream, k)
            log.error(message)
            if IGNORE_NULLS and v is None:
                log.info('Ignoring NULL value from expected data')
            else:
                failures.append((edex_tools.FAILURES.MISSING_FIELD, message))
            continue

        if type(v) == dict:
            v = v.get('value')

        if not same(v, b[k]):
            failures.append((edex_tools.FAILURES.BAD_VALUE,
                             'stream=%s key=%s expected=%s retrieved=%s' % (stream, k, v, b[k])))
            log.error('%s - non-matching value: key=%r expected=%r retrieved=%r', stream, k, v, b[k])

    # verify no extra (unexpected) keys present in retrieved data
    for k in b:
        if k not in a and k not in ignore:
            failures.append((edex_tools.FAILURES.UNEXPECTED_VALUE, (stream, k)))
            log.error('%s - item in retrieved data not in expected data: %s', stream, k)

    return failures


def wait_for_ingest_complete():
    """
    Wait for ingestion to complete.
    :return:  True when the EDEX log file indicates completion, False if message does not appear within expected
              timeout.
    """
    return edex_tools.watch_log_for('Ingest: EDEX: Ingest')


def test_results(expected, stream_name, sensor='null'):
    retrieved = edex_tools.get_from_edex('localhost', stream_name, timestamp_as_string=True, sensor=sensor)
    log.debug('Retrieved %d records from edex:', len(retrieved))
    log.debug(pprint.pformat(retrieved, depth=3))
    log.debug('Retrieved %d records from expected data file:', len(expected))
    log.debug(pprint.pformat(expected, depth=3))
    failures = compare(retrieved, expected)
    return len(retrieved), len(expected), failures


def dump_csv(data):
    """
    Save results in comma separated file and close log file.
    :param data:   table data
    :return:  none
    """
    fh = open(os.path.join(output_dir, 'results.csv'), 'w')
    for row in data:
        row = [str(x) for x in row]
        fh.write(','.join(row) + '\n')
    fh.close()


def purge_edex(logfile=None):
    edex_tools.purge_edex()
    return edex_tools.watch_log_for('Purge Operation: PURGE_ALL_DATA completed', logfile=logfile)


def execute_test(test_queue, expected_queue):
    while True:
        try:
            test_case, index, count = test_queue.get_nowait()
            log.debug('Processing test case: %s index: %d', test_case, index)

            test_file, yaml_file = test_case.pairs[index]
            input_filepath = os.path.join(drivers_dir, test_case.resource, test_file)
            output_filepath = os.path.join(drivers_dir, test_case.resource, yaml_file)

            if os.path.exists(input_filepath) and os.path.exists(output_filepath):

                delivery = test_case.instrument.split('_')[-1]
                sensor = 'VALIDATE-%.1f-%08d' % (time.time(), count)
                queue = 'Ingest.%s' % test_case.instrument

                try:
                    log.info('Sending file (%s) to queue (%s)', test_file, queue)
                    edex_tools.send_file_to_queue(input_filepath, queue, delivery, sensor)
                except NotFound:
                    log.warn('Queue not found: %s', queue)
                    return None

                log.info('Fetching expected results from YML file: %s', yaml_file)
                this_expected = get_expected(output_filepath)
                expected_queue.put((test_case.instrument, sensor, test_file, yaml_file, this_expected))

        except Queue.Empty:
            break


def execute_validate(expected_queue, results_queue):
    while True:
        try:
            instrument, sensor, test_file, yaml_file, stream, expected = expected_queue.get_nowait()

            log.info('Testing instrument: %r, sensor: %r, stream: %r', instrument, sensor, stream)
            results = test_results(expected, stream, sensor=sensor)
            log.debug('Results for instrument: %s test_file: %s yaml_file: %s stream: %s',
                      instrument, test_file, yaml_file, stream)
            log.debug(results)
            results_queue.put_nowait((instrument, test_file, yaml_file, stream, results))

        except Queue.Empty:
            break


def test(my_test_cases):
    try:
        logfile = edex_tools.find_latest_log()
    except OSError as e:
        log.error('Error fetching latest log file - %s', e)
        return {}

    purge_edex(logfile)

    total_timeout = 0
    test_queue = Queue.Queue()
    expected_queue = Queue.Queue()
    results_queue = Queue.Queue()
    threads = []
    sc = {}

    # execute all tests and load expected results
    for count, case in enumerate(my_test_cases):
        total_timeout += case.timeout
        for index, _ in enumerate(case.pairs):
            test_queue.put((case, index, count))

    for _ in xrange(NUM_THREADS):
        t = Thread(target=execute_test, args=[test_queue, expected_queue])
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    # wait for all ingestion to complete
    if not edex_tools.watch_log_for('Ingest: EDEX: Ingest', logfile=logfile,
                                    expected_count=expected_queue.qsize(), timeout=total_timeout):
        log.error('Timed out waiting for ingest complete message')

    # pull expected results from the queue, break them down into individual streams then put them back in
    temp_list = []
    while True:
        try:
            instrument, sensor, test_file, yaml_file, expected = expected_queue.get_nowait()
            for stream in expected:
                temp_list.append((instrument, sensor, test_file, yaml_file, stream, expected[stream]))
        except Queue.Empty:
            break

    for each in temp_list:
        expected_queue.put_nowait(each)

    # retrieve results and compare with expected
    for _ in xrange(NUM_THREADS):
        t = Thread(target=execute_validate, args=[expected_queue, results_queue])
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    while True:
        try:
            instrument, test_file, yaml_file, stream, results = results_queue.get_nowait()
            sc.setdefault(instrument, {}).setdefault(test_file, {}).setdefault(yaml_file, {})[stream] = results
        except Queue.Empty:
            break

    return sc


if __name__ == '__main__':
    options = docopt.docopt(__doc__)

    IGNORE_NULLS = options['--ignore_null']

    test_cases = []
    if not options['<test_case>']:
        test_cases = list(read_test_cases('test_cases'))
    else:
        for each in options['<test_case>']:
            test_cases.extend(list(read_test_cases(each)))

    scorecard = test(test_cases)

    result, table_data = edex_tools.parse_scorecard(scorecard)
    log.info(result)
    dump_csv(table_data)
