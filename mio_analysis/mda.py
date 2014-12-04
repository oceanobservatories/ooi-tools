#!/usr/bin/env python


__author__ = 'Pete Cable, Dan Mergens'

import os
import sys

dataset_dir = os.path.dirname(os.path.realpath('__file__'))
tools_dir = os.path.dirname(dataset_dir)

sys.path.append(tools_dir)

import yaml
import time
import pprint
from common import edex_tools
from common import logger

omc_dir = os.getenv('OMC_HOME')
if omc_dir is None:
    omc_dir = os.path.join(os.getenv('HOME'), 'src', 'omc_data', 'omc_data')
startdir = os.path.join(edex_tools.edex_dir, 'data/utility/edex_static/base/ooi/parsers/mi-dataset/mi')
hdf5dir = os.path.join(edex_tools.edex_dir, 'data', 'hdf5', 'sensorreading')
ingest_dir = os.path.join(edex_tools.edex_dir, 'data', 'ooi')
output_dir = os.path.join(dataset_dir, 'output_%s' % time.strftime('%Y%m%d-%H%M%S'))
log = logger.get_logger(file_output=os.path.join(output_dir, 'everything.log'))
log_dir = os.path.join(edex_tools.edex_dir, 'logs')

DEFAULT_STANDARD_TIMEOUT = 60


class TestCase(object):
    def __init__(self, config):
        self.config = config
        self.resource = os.path.join(omc_dir, config.get('resource'))
        self.endpoint = config.get('endpoint')
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


def wait_for_ingest_complete():
    """
    Wait for ingestion to complete.
    :return:  True when the EDEX log file indicates completion, False if message does not appear within expected
              timeout.
    """
    return edex_tools.watch_log_for('Ingest: EDEX: Ingest')


def load_files(resource, instrument, test_file, sensor):
    queue = 'Ingest.%s' % instrument
    log.info('send test file %s into ingest queue %s from %s', test_file, queue, resource)
    source_file = os.path.join(resource, test_file)

    log.info('source_file: %s', source_file)
    if not os.path.exists(source_file):
        source_file = os.path.join(omc_dir, source_file)
    files = []
    num_files = 0

    if os.path.isdir(source_file):
        files.extend(os.listdir(source_file))
    else:
        files.append(os.path.basename(source_file))
        source_file = os.path.dirname(source_file)

    for f in files:
        try:
            delivery = instrument.split('_')[-1]
            log.debug('sending file to queue: %s', f)
            edex_tools.send_file_to_queue(os.path.join(source_file, f), queue, delivery, sensor)
            num_files += 1
        except IOError as e:
            log.error('Exception copying input file to endpoint: %s', e)

    return num_files


def purge_edex(logfile=None):
    edex_tools.purge_edex()
    return edex_tools.watch_log_for('Purge Operation: PURGE_ALL_DATA completed', logfile=logfile)


def test(test_cases):
    try:
        logfile = edex_tools.find_latest_log()
    except OSError as e:
        log.error('Error fetching latest log file - %s', e)
        return

    purge_edex(logfile)

    last_instrument = None
    num_files = 0
    total_timeout = 0

    for i, test_case in enumerate(test_cases):
        logger.remove_handler(last_instrument)
        logger.add_handler(test_case.instrument, dir=output_dir)
        last_instrument = test_case.instrument
        total_timeout += test_case.timeout

        log.debug('Processing test case: %s', test_case)
        sensor = 'MDA-%.1f-%08d' % (time.time(), i)

        for source in test_case.source_data:
            num_files += load_files(test_case.resource, test_case.endpoint, source, sensor)

    if not edex_tools.watch_log_for('Ingest: EDEX: Ingest', logfile=logfile,
                                    timeout=total_timeout, expected_count=num_files):
        log.error('Timed out waiting for ingest complete message')
        time.sleep(1)

    mio_analysis(hostname='localhost', output_dir=output_dir)


def mio_analysis(hostname='localhost', output_dir='./'):
    """
    Fetch current data from EDEX and perform MIO data analysis.
    :param hostname:  EDEX server hostname (default: localhost)
    :param output_dir:   Output directory to archive MIO data analysis files.
    :return:          none
    """
    log.info('collecting available instruments from %s...', hostname)
    instruments = edex_tools.edex_get_instruments(hostname)

    for stream in instruments:
        for instrument in instruments[stream]:
            log.info('calculating results for %s:%s...' % (stream, instrument))
            edex_tools.edex_mio_report(hostname, stream, instrument, output_dir)
            log.info('fetching netcdf file for %s:%s...' % (stream, instrument))
            edex_tools.get_netcdf(hostname, stream, instrument, output_dir=output_dir)
    log.info('done')


if __name__ == '__main__':
    test_cases = []
    if len(sys.argv) <= 1:
        test_cases = read_test_cases('test_cases')
    else:
        for each in sys.argv[1:]:
            test_cases.extend(list(read_test_cases(each)))

    edex_tools.clear_hdf5()

    test(test_cases)
