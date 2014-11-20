#!/usr/bin/env python
__author__ = 'Pete Cable, Dan Mergens'

import os
import sys

dataset_dir = os.path.dirname(os.path.realpath('__file__'))
tools_dir = os.path.dirname(dataset_dir)

sys.path.append(tools_dir)

import glob
import yaml
import shutil
import time
import pprint

# from common import logger
from common import edex_tools
from common import logger

edex_dir = os.getenv('EDEX_HOME')
if edex_dir is None:
    edex_dir = os.path.join(os.getenv('HOME'), 'uframes', 'ooi', 'uframe-1.0', 'edex')
omc_dir = os.getenv('OMC_HOME')
if omc_dir is None:
    omc_dir = os.path.join(os.getenv('HOME'), 'src', 'omc_data', 'omc_data')
startdir = os.path.join(edex_dir, 'data/utility/edex_static/base/ooi/parsers/mi-dataset/mi')
hdf5dir = os.path.join(edex_dir, 'data', 'hdf5', 'sensorreading')
ingest_dir = os.path.join(edex_dir, 'data', 'ooi')
output_dir = os.path.join(dataset_dir, 'output_%s' % time.strftime('%Y%m%d-%H%M%S'))
log = logger.get_logger(file_output=os.path.join(output_dir, 'everything.log'))
log_dir = os.path.join(edex_dir, 'logs')

DEFAULT_STANDARD_TIMEOUT = 60


class TestCase(object):
    def __init__(self, config):
        self.config = config
        self.resource = os.path.join(omc_dir, config.get('resource'))
        self.endpoint = os.path.join(ingest_dir, config.get('endpoint'))
        self.instrument = config.get('instrument')
        self.source_data = config.get('source_data', [])
        self.rename = config.get('rename', True)
        # Attempt to obtain a timeout value from the test_case yml.  Default it to
        # DEFAULT_STANDARD_TIMEOUT if no yml value was provided.
        self.rename = config.get('rename', True)
        self.timeout = config.get('timeout', DEFAULT_STANDARD_TIMEOUT)

    def __str__(self):
        return pprint.pformat(self.config)

    def __repr__(self):
        return self.config.__repr__()


def read_test_cases(f):
    log.info('Finding test cases in: %s', f)
    if os.path.isdir(f):
        for filename in os.listdir(f):
            config = yaml.load(open(os.path.join(f, filename), 'r'))
            yield TestCase(config)
    elif os.path.isfile(f):
        config = yaml.load(open(f, 'r'))
        yield TestCase(config)


def clear_hdf5():
    for fname in os.listdir(hdf5dir):
        os.remove(os.path.join(hdf5dir, fname))


def find_latest_log():
    """
    Fetch the latest EDEX log file.  Will throw OSError if file not found.
    :return:  file handle to the log file
    """
    todayglob = time.strftime('edex-ooi-%Y%m%d.log*', time.localtime())
    files = glob.glob(os.path.join(log_dir, todayglob))
    files = [(os.stat(f).st_mtime, f) for f in files if not f.endswith('lck')]
    files.sort()
    fh = open(files[-1][1], 'r')
    fh.seek(0, 2)
    return fh


def watch_log_for(expected_string, logfile=None, expected_count=1, timeout=DEFAULT_STANDARD_TIMEOUT):
    """
    Wait for expected string to appear in log file.
    :param expected_string:   string to watch for in log file
    :param logfile:   file to watch
    :param expected_count:  number of occurrences expected
    :param timeout:  maximum time to wait for expected string
    :return:  True if expected string occurs before specified timeout, False otherwise.
    """
    if logfile is None:
        try:
            logfile = find_latest_log()
        except OSError as e:
            log.error('Error fetching latest log file - %s', e)
            return False

    log.info('waiting for %s in logfile: %s', expected_string, logfile.name)

    end_time = time.time() + timeout
    count = 0
    while time.time() < end_time:
        data = logfile.read()
        for line in data.split('\n'):
            if expected_string in line:
                count += 1
                log.info('Found expected string %d times of %d', count, expected_count)
                if count == expected_count:
                    return True
        time.sleep(.1)
    return False


def wait_for_ingest_complete():
    """
    Wait for ingestion to complete.
    :return:  True when the EDEX log file indicates completion, False if message does not appear within expected
              timeout.
    """
    return watch_log_for('Ingest: EDEX: Ingest')


def copy_file(resource, endpoint, test_file, rename=False):
    log.info('copy test file %s into endpoint %s from %s', test_file, endpoint, resource)
    source_file = os.path.join(omc_dir, resource, test_file)
    if rename:
        test_file = '%s.%.2f' % (test_file, time.time())
    destination_file = os.path.join(ingest_dir, endpoint, test_file)
    try:
        shutil.copy(source_file, destination_file)
        return True
    except IOError as e:
        log.error('Exception copying input file to endpoint: %s', e)
        return False


def purge_edex(logfile=None):
    edex_tools.purge_edex()
    return watch_log_for('Purge Operation: PURGE_ALL_DATA completed', logfile=logfile)


def test(test_cases):
    try:
        logfile = find_latest_log()
    except OSError as e:
        log.error('Error fetching latest log file - %s', e)
        return

    last_instrument = None
    for test_case in test_cases:
        logger.remove_handler(last_instrument)
        logger.add_handler(test_case.instrument, dir=output_dir)
        last_instrument = test_case.instrument

        log.debug('Processing test case: %s', test_case)
        purge_edex()

        num_files = 0
        for source in test_case.source_data:
            if copy_file(test_case.resource, test_case.endpoint, source):
                num_files +=1
        if not watch_log_for('Ingest: EDEX: Ingest', logfile=logfile,
                             timeout=test_case.timeout, expected_count=num_files):
            # didn't see any ingest, proceed, results should be all failed
            log.error('Timed out waiting for ingest complete message')
            time.sleep(1)

        mio_analysis(hostname='localhost', dir=output_dir)


def mio_analysis(hostname='localhost', dir='./'):
    """
    Fetch current data from EDEX and perform MIO data analysis.
    :param hostname:  EDEX server hostname (default: localhost)
    :param dir:   Output directory to archive MIO data analysis files.
    :return:          none
    """
    print 'collecting available instruments from %s...' % hostname
    instruments = edex_tools.edex_get_instruments(hostname)

    for stream in instruments:
        for instrument in instruments[stream]:
            print 'calculating results for %s:%s...' % (stream, instrument)
            edex_tools.edex_mio_report(stream, instrument, edex_tools.edex_get_json(hostname, stream, instrument),
                                       dir)
            print 'fetching netcdf file for %s:%s...' % (stream, instrument)
            edex_tools.get_netcdf(hostname, stream, instrument, dir)
    print 'done'


if __name__ == '__main__':
    test_cases = []
    if len(sys.argv) <= 1:
        test_cases = read_test_cases('test_cases')
    else:
        for each in sys.argv[1:]:
            test_cases.extend(list(read_test_cases(each)))

    clear_hdf5()

    test(test_cases)
