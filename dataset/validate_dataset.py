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

import json
import time
import pprint
import ntplib
import random
import docopt
import calendar

from qpid.messaging.exceptions import NotFound
from datetime import datetime
from yaml import load
from common import logger
from common import edex_tools

from multiprocessing.pool import ThreadPool

IGNORE_NULLS = False
VALIDATE_TIMESTAMP = time.strftime('%Y%m%d.%H:%M:%S', time.localtime())

MAX_THREADS = 30

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
        self.sensor = config.get('sensor')
        self.sensor_ids = []
        self.expected = []
        self.count = 0

    def __str__(self):
        return pprint.pformat(self.config)

    def __repr__(self):
        return self.__dict__.__repr__()


def read_test_cases(f):
    log.info('Finding test cases in: %s', f)
    if os.path.isdir(f):
        for filename in os.listdir(f):
            config = load(open(os.path.join(f, filename), 'r'))
            yield TestCase(config)
    elif os.path.isfile(f):
        config = load(open(f, 'r'))
        yield TestCase(config)


def get_expected(filename, cache_dir='.cache'):
    """
    Loads expected results from the supplied YAML file
    :param filename:
    :return: list of records containing the expected results
    """
    cached_path = os.path.join(cache_dir, filename.split('mi-dataset/')[1])
    if os.path.exists(cached_path):
        try:
            return json.load(open(cached_path))
        except:
            log.warn('Exception reading JSON cache, parsing YML')

    try:
        fh = open(filename, 'r')
        data = load(fh)
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

    dirname = os.path.dirname(cached_path)
    log.info('caching yml results for faster testing next run...')
    try:
        if not os.path.exists(dirname):
            log.info('creating dir: %s', dirname)
            os.makedirs(dirname)
        json.dump(expected_dictionary, open(cached_path, 'wb'))
    except OSError:
        pass

    return expected_dictionary


def test_results(expected, stream_name, sensor, method):
    subsite, node, sensor = sensor.split('-', 3)
    start = ntplib.system_to_ntp_time(1)
    stop = 1e10

    stream_code = '%s_%s_%s' % (stream_name, sensor, method)

    log.info('Retrieving data (%s)', stream_code)
    now = time.time()
    metadata = edex_tools.get_edex_metadata('localhost', subsite, node, sensor)
    retrieved = edex_tools.get_from_edex('localhost', subsite, node, sensor, method,
                                         stream_name, start, stop, timestamp_as_string=True)
    elapsed = time.time() - now
    retrieved_count = 0
    for each in retrieved.itervalues():
        retrieved_count += len(each)

    log.info('Retrieved %d records (%s) in %.4f secs', retrieved_count, stream_code, elapsed)

    log.debug(pprint.pformat(retrieved, depth=3))
    log.debug('Retrieved %d records from expected data file:', len(expected))
    log.debug(pprint.pformat(expected, depth=3))
    now = time.time()
    failures = edex_tools.compare(retrieved, expected, metadata, ignore_nulls=IGNORE_NULLS)
    elapsed = time.time() - now
    log.info('Compared %d records (%s) in %.4f secs', retrieved_count, stream_code, elapsed)
    return retrieved_count, len(expected), failures


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


def execute_test(test_case):
    index = random.randint(0, 999)
    log.debug('Processing test case: %s index: %d', test_case, index)
    test_case.count = 0

    for test_file, yaml_file in test_case.pairs:
        input_filepath = os.path.join(drivers_dir, test_case.resource, test_file)
        output_filepath = os.path.join(drivers_dir, test_case.resource, yaml_file)

        if os.path.exists(input_filepath) and os.path.exists(output_filepath):

            delivery = test_case.instrument.split('_')[-1]

            if test_case.sensor is None:
                sensor = 'VALIDATE-%s-%03d' % (VALIDATE_TIMESTAMP, index)
            elif test_case.sensor == 'any':
                sensor = 'null'
            else:
                sensor = test_case.sensor

            queue = 'Ingest.%s' % test_case.instrument

            try:
                log.info('Sending file (%s) to queue (%s)', test_file, queue)
                edex_tools.send_file_to_queue(input_filepath, queue, delivery, sensor, 1)
            except NotFound:
                log.warn('Queue not found: %s', queue)
                return None

            log.info('Fetching expected results from YML file: %s', yaml_file)
            test_case.sensor_ids.append(sensor)
            test_case.expected.append(get_expected(output_filepath))
            test_case.count += 1

        else:
            log.error('Missing test data or results: %s %s', input_filepath, output_filepath)
            test_case.sensor_ids.append(None)
            test_case.expected.append(None)


def evaluate_test_case(tc):
    log.info('Evaluating test case: %s', tc.instrument)
    sc = {}
    method = tc.instrument.split('_')[-1]
    try:
        for index, sensor in enumerate(tc.sensor_ids):
            expected = tc.expected[index]
            if sensor is not None:
                for stream in expected:
                    results = test_results(expected[stream], stream, sensor, method)
                    sc.setdefault(tc.instrument, {}) \
                        .setdefault(tc.pairs[index][0], {}) \
                        .setdefault(tc.pairs[index][1], {})[stream] = results

    except Exception as e:
        import traceback
        traceback.print_exc()
        log.error('Exception processing test case %r: %s', tc, e)
    return sc


def test(my_test_cases):
    try:
        logfile = edex_tools.find_latest_log()
    except OSError as e:
        log.error('Error fetching latest log file - %s', e)
        return {}

    total_timeout = 0
    count = 0
    sc = {}
    pool = ThreadPool(MAX_THREADS)

    pool.map(execute_test, my_test_cases)

    for tc in my_test_cases:
        total_timeout += tc.timeout
        count += tc.count

    # wait for all ingestion to complete
    if not edex_tools.watch_log_for('EDEX - Ingest complete for file', logfile=logfile,
                                    expected_count=count, timeout=total_timeout):
        log.error('Timed out waiting for ingest complete message')

    log.info('All files ingested, testing results')

    for tc in pool.map(evaluate_test_case, test_cases):
        sc.update(tc)

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
