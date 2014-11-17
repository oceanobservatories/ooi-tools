#!/usr/bin/env python

import os
import sys

dataset_dir = os.path.dirname(os.path.realpath('__file__'))
tools_dir = os.path.dirname(dataset_dir)

sys.path.append(tools_dir)

import calendar
from datetime import datetime
import glob
import yaml
import shutil
import time
import pprint
import math
import ntplib

from common import logger
from common import edex_tools

edex_dir = os.getenv('EDEX_HOME')
if edex_dir is None:
    edex_dir = os.path.join(os.getenv('HOME'), 'uframes', 'ooi', 'uframe-1.0', 'edex')
hdf5dir = os.path.join(edex_dir, 'data', 'hdf5', 'sensorreading')
startdir = os.path.join(edex_dir, 'data/utility/edex_static/base/ooi/parsers/mi-dataset/mi')
drivers_dir = os.path.join(startdir, 'dataset/driver')
ingest_dir = os.path.join(edex_dir, 'data', 'ooi')
log_dir = os.path.join(edex_dir, 'logs')

output_dir = os.path.join(dataset_dir, 'output_%s' % time.strftime('%Y%m%d-%H%M%S'))


log = logger.get_logger(file_output=os.path.join(output_dir, 'everything.log'))

DEFAULT_STANDARD_TIMEOUT = 60


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
        self.timeout = config.get('timeout', DEFAULT_STANDARD_TIMEOUT)

    def __str__(self):
        return pprint.pformat(self.config)

    def __repr__(self):
        return self.config.__repr__()


def clear_hdf5():
    for fname in os.listdir(hdf5dir):
        os.remove(os.path.join(hdf5dir, fname))


def read_test_cases(f):
    log.info('Finding test cases in: %s', f)
    if os.path.isdir(f):
        for filename in os.listdir(f):
            config = yaml.load(open(os.path.join(f, filename), 'r'))
            yield TestCase(config)
    elif os.path.isfile(f):
        config = yaml.load(open(f, 'r'))
        yield TestCase(config)


def get_expected(filename):
    """
    Loads expected results from the supplied YAML file
    :param filename:
    :return: list of records containing the expected results
    """
    try:
        fh = open(filename, 'r')
        data = yaml.load(fh)
        log.debug('Raw data from YAML: %s', data)
        header = data.get('header')
        data = data.get('data')
        particle_type = header.get('particle_type')
        if particle_type is not None:
            if particle_type != 'MULTIPLE':
                for record in data:
                    record['particle_type'] = particle_type
    except:
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
            if v is None:
                log.info("%s - Soft failure, None value in expected data for: %s", stream, k)
            else:
                failures.append((edex_tools.FAILURES.MISSING_FIELD, (stream,k)))
                log.error('%s - missing key: %s in retrieved record', stream, k)
            continue
        if type(v) == dict:
            _round = v.get('round')
            value = massage_data(v.get('value'), _round)
            rvalue = massage_data(b[k], _round)
        else:
            value = massage_data(v)
            rvalue = massage_data(b[k])

        if value != rvalue:
            failed = False
            if 'timestamp' in k:
                # try rounding...
                try:
                    value = '%12.3f' % float(value)
                except:
                    pass
                try:
                    rvalue = '%12.3f' % float(value)
                except Exception as e:
                   log.error('Exception massaging timestamp to string: %s', e)
                if value != rvalue: failed = True
            else:
                failed = True
            if failed:
                failures.append((edex_tools.FAILURES.BAD_VALUE, 'stream=%s key=%s expected=%s retrieved=%s' % (stream, k, v, b[k])))
                log.error('%s - non-matching value: key=%r expected=%r retrieved=%r', stream, k, value, rvalue)

    # verify no extra (unexpected) keys present in retrieved data
    for k in b:
        if k in ignore:
            continue
        if k not in a:
            failures.append((edex_tools.FAILURES.UNEXPECTED_VALUE, (stream,k)))
            log.error('%s - item in retrieved data not in expected data: %s', stream, k)

    return failures


def massage_data(value, _round=3):
    if type(value) == str:
        return value.strip()
    elif type(value) == float and math.isnan(value):
        return 'NaN'
    elif type(value) == float:
        return round(value, _round)
    elif type(value) == list:
        return [massage_data(x, _round) for x in value]
    elif type(value) == dict:
        return {massage_data(k, _round):massage_data(v, _round) for k,v in value.items()}
    else:
        return value


def copy_file(resource, endpoint, test_file, rename=False):
    log.info('copy test file %s into endpoint %s from %s', test_file, endpoint, resource)
    source_file = os.path.join(drivers_dir, resource, test_file)
    if rename:
        test_file = '%s.%.2f' % (test_file, time.time())
    destination_file = os.path.join(ingest_dir, endpoint, test_file)
    try:
        shutil.copy(source_file, destination_file)
        return True
    except IOError as e:
        log.error('Exception copying input file to endpoint: %s', e)
        return False


def find_latest_log():
    todayglob = time.strftime('edex-ooi-%Y%m%d.log*', time.localtime())
    files = glob.glob(os.path.join(log_dir, todayglob))
    files = [(os.stat(f).st_mtime, f) for f in files if not f.endswith('lck')]
    files.sort()
    fh = open(files[-1][1], 'r')
    fh.seek(0, 2)
    return fh


def watch_log_for(expected_string, logfile=None, expected_count=1, timeout=DEFAULT_STANDARD_TIMEOUT):
    if logfile is None:
        logfile = find_latest_log()
    log.info('waiting for %s in logfile: %s', expected_string, logfile.name)

    endtime = time.time() + timeout
    count = 0
    while time.time() < endtime:
        data = logfile.read()
        for line in data.split('\n'):
            if expected_string in line:
                count += 1
                log.info('Found expected string %d times of %d', count, expected_count)
                if count == expected_count:
                    return
        time.sleep(.1)
    raise Exception('timeout waiting for log output')


def wait_for_ingest_complete():
    watch_log_for('Ingest: EDEX: Ingest')


def test_results(expected, stream_name):
    retrieved = edex_tools.get_from_edex('localhost', stream_name, timestamp_as_string=True)
    log.debug('Retrieved %d records from edex:', len(retrieved))
    log.debug(pprint.pformat(retrieved, depth=3))
    log.debug('Retrieved %d records from expected data file:', len(expected))
    log.debug(pprint.pformat(expected, depth=3))
    failures = compare(retrieved, expected)
    return len(retrieved), len(expected), failures


def dump_csv(table_data):
    fh = open(os.path.join(output_dir, 'results.csv'), 'w')
    for row in table_data:
        row = [str(x) for x in row]
        fh.write(','.join(row) + '\n')
    fh.close()


def purge_edex(logfile=None):
    edex_tools.purge_edex()
    watch_log_for('Purge Operation: PURGE_ALL_DATA completed', logfile=logfile)


def test(test_cases):
    scorecard = {}
    logfile = find_latest_log()

    last_instrument = None
    for test_case in test_cases:
        logger.remove_handler(last_instrument)
        logger.add_handler(test_case.instrument, dir=output_dir)
        last_instrument = test_case.instrument

        log.debug('Processing test case: %s', test_case)
        for test_file, yaml_file in test_case.pairs:
            purge_edex()
            expected = get_expected(os.path.join(drivers_dir, test_case.resource, yaml_file))

            if copy_file(test_case.resource, test_case.endpoint, test_file):
                try:
                    watch_log_for('Ingest: EDEX: Ingest', logfile=logfile, timeout=test_case.timeout)
                    time.sleep(1)
                except:
                    # didn't see any ingest, proceed, results should be all failed
                    log.error('Timed out waiting for ingest complete message')

            for stream in expected:
                results = test_results(expected[stream], stream)
                log.debug('Results for instrument: %s test_file: %s yaml_file: %s stream: %s',
                          test_case.instrument, test_file, yaml_file, stream)
                log.debug(results)
                scorecard.setdefault(test_case.instrument, {}) \
                         .setdefault(test_file, {}) \
                         .setdefault(yaml_file, {})[stream] = results
    return scorecard


def test_bulk(test_cases):
    expected = {}
    scorecard = {}
    num_files = 0

    purge_edex()
    logfile = find_latest_log()

    for test_case in test_cases:
        log.debug('Processing test case: %s', test_case)
        for test_file, yaml_file in test_case.pairs:
            if copy_file(test_case.resource, test_case.endpoint, test_file, rename=True):
                num_files += 1
            this_expected = get_expected(os.path.join(drivers_dir, test_case.resource, yaml_file))
            for stream in this_expected:
                expected[(test_case.instrument, test_file, yaml_file, stream)] = this_expected[stream]

    try:
        watch_log_for('Ingest: EDEX: Ingest', logfile=logfile, expected_count=num_files, timeout=60)
        # sometimes edex needs to catch its breath after so many files... sleep a bit
        time.sleep(15)
    except:
        log.error('Timed out waiting for ingest complete message')

    last_instrument = None
    for k,v in expected.iteritems():
        instrument, test_file, yaml_file, stream = k
        if instrument != last_instrument:
            logger.remove_handler(last_instrument)
            logger.add_handler(instrument, dir=output_dir)
            last_instrument = instrument

        results = test_results(expected[(instrument,test_file,yaml_file,stream)], stream)
        log.debug('Results for instrument: %s test_file: %s yaml_file: %s stream: %s',
                   instrument, test_file, yaml_file, stream)
        log.debug(results)
        scorecard.setdefault(instrument, {}) \
                 .setdefault(test_file, {}) \
                 .setdefault(yaml_file, {})[stream] = results

    return scorecard


if __name__ == '__main__':
    test_cases = []
    if len(sys.argv) <= 1:
        test_cases = read_test_cases('test_cases')
    else:
        for each in sys.argv[1:]:
            test_cases.extend(list(read_test_cases(each)))
    bulk_test_cases = [tc for tc in test_cases if tc.rename]
    single_test_cases = [tc for tc in test_cases if not tc.rename]

    clear_hdf5()

    scorecard = test_bulk(bulk_test_cases)
    scorecard.update(test(single_test_cases))

    result, table_data = edex_tools.parse_scorecard(scorecard)
    log.info(result)
    dump_csv(table_data)
