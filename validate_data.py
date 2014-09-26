#!/usr/bin/env python

import glob
import logging
import shutil
import pprint
import urllib2
import time
import uuid
import yaml
import json
import sys
import os
import qpid.messaging as qm

all_data_url = 'http://localhost:12570/sensor/user/inv/%s/null'

edex_dir = os.getenv('EDEX_HOME')
if edex_dir is None:
    edex_dir = os.path.join(os.getenv('HOME'), 'uframes', 'ooi', 'uframe-1.0', 'edex')
startdir = os.path.join(edex_dir, 'data/utility/edex_static/base/ooi/parsers/mi-dataset/mi')
drivers_dir = os.path.join(startdir, 'dataset/driver')
ingest_dir = os.path.join(edex_dir, 'data', 'ooi')
log_dir = os.path.join(edex_dir, 'logs')
output_dir = 'output'

USER = 'guest'
HOST = 'localhost'
PORT = 5672
PURGE_MESSAGE = qm.Message(content='PURGE_ALL_DATA', content_type='text/plain', user_id=USER)

# set up the logger
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)-7s %(message)s')
log = logging.getLogger('dataset_test')
log.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)
log.addHandler(ch)
# output file handler
if not os.path.exists(output_dir):
    os.mkdir(output_dir)
fh = logging.FileHandler(os.path.join(output_dir, 'everything.log'))
fh.setFormatter(formatter)
log.addHandler(fh)


class FAILURES:
    MISSING_SAMPLE = 'MISSING_SAMPLE'
    MISSING_FIELD = 'MISSING_FIELD'
    BAD_VALUE = 'BAD_VALUE'
    UNEXPECTED_VALUE = 'UNEXPECTED_VALUE'
    AMBIGUOUS = 'AMBIGUOUS'

    def __init__(self):
        pass


class TestCase(object):
    def __init__(self, config):
        self.config = config
        self.instrument = config.get('instrument')
        self.resource = os.path.join(drivers_dir, config.get('resource'))
        self.endpoint = os.path.join(ingest_dir, config.get('endpoint'))
        self.pairs = config.get('pairs', [])

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


def get_from_edex(stream_name):
    """
    Retrieve all stored sensor data from edex
    :return: list of edex records
    """
    proxy_handler = urllib2.ProxyHandler({})
    opener = urllib2.build_opener(proxy_handler)
    req = urllib2.Request(all_data_url % stream_name)
    r = opener.open(req)
    records = json.loads(r.read())
    log.debug('RETRIEVED:')
    log.debug(pprint.pformat(records, depth=3))
    d = {}
    for record in records:
        timestamp = record.get('internal_timestamp')
        stream_name = record.get('stream_name')
        key = (timestamp, stream_name)
        if key in d:
            log.error('Duplicate record found in retrieved values %s', key)
        else:
            d[key] = record
    return d


def get_expected(filename):
    """
    Loads expected results from the supplied YAML file
    :param filename:
    :return: list of records containing the expected results
    """
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

    for record in data:
        timestamp = record.get('internal_timestamp')
        if type(timestamp) == str:
            timestamp, millis = timestamp.split('.')
            timestamp = time.mktime(time.strptime(timestamp + 'GMT', '%Y-%m-%dT%H:%M:%S%Z'))
            if millis.endswith('Z'):
                millis = millis[:-1]
            divisor = 10 ** len(millis)
            millis = float(millis)
            timestamp = timestamp + millis / divisor + 2208988800l - time.timezone
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
        timestamp = record.get('internal_timestamp')
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
                failures.append((FAILURES.AMBIGUOUS, 'Multiple streams in output, no stream in YML'))
                log.error('Ambiguous stream information in YML file and unable to infer')
                continue
        else:
            key = (timestamp, stream_name)
        if key not in stored:
            failures.append((FAILURES.MISSING_SAMPLE, key))
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
                failures.append((FAILURES.MISSING_FIELD, (stream,k)))
                log.error('%s - missing key: %s in retrieved record', stream, k)
            continue
        if type(v) == dict:
            _round = v.get('round')
            value = v.get('value')
            rvalue = round(b[k], _round)
        else:
            value = v
            rvalue = b[k]
        if value != rvalue:
            failures.append((FAILURES.BAD_VALUE, 'stream=%s key=%s expected=%s retrieved=%s' % (stream, k, v, b[k])))
            log.error('%s - non-matching value: key=%s expected=%s retrieved=%s', stream, k, v, b[k])

    # verify no extra (unexpected) keys present in retrieved data
    for k in b:
        if k in ignore:
            continue
        if k not in a:
            failures.append((FAILURES.UNEXPECTED_VALUE, (stream,k)))
            log.error('%s - item in retrieved data not in expected data: %s', stream, k)

    return failures


def purge_edex(logfile=None):
    log.info('Purging edex')
    conn = qm.Connection(host=HOST, port=PORT, username=USER, password=USER)
    conn.open()
    conn.session().sender('purgeRequest').send(PURGE_MESSAGE)
    conn.close()
    watch_log_for('Purge Operation: PURGE_ALL_DATA completed', logfile=logfile)


def copy_file(resource, endpoint, test_file):
    log.info('copy test file %s into endpoint %s from %s', test_file, endpoint, resource)
    source_file = os.path.join(drivers_dir, resource, test_file)
    destination_file = os.path.join(ingest_dir, endpoint, str(uuid.uuid4()))
    shutil.copy(source_file, destination_file)


def find_latest_log():
    todayglob = time.strftime('edex-ooi-%Y%m%d.log*', time.localtime())
    files = glob.glob(os.path.join(log_dir, todayglob))
    files = [(os.stat(f).st_mtime, f) for f in files if not f.endswith('lck')]
    files.sort()
    fh = open(files[-1][1], 'r')
    fh.seek(0, 2)
    return fh


def watch_log_for(expected_string, logfile=None, expected_count=1, timeout=60):
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
    retrieved = get_from_edex(stream_name)
    log.debug('Retrieved %d records from edex:', len(retrieved))
    log.debug(pprint.pformat(retrieved, depth=3))
    log.debug('Retrieved %d records from expected data file:', len(expected))
    log.debug(pprint.pformat(expected, depth=3))
    failures = compare(retrieved, expected)
    return len(retrieved), len(expected), failures


def parse_scorecard(scorecard):
    result = ['SCORECARD:']
    format_string = "{: <%d}   {: <%d}   {: <%d}   {: <%d} {: >15} {: >15} {: >15} {: >15}"

    total_instrument_count = 0
    total_yaml_count = 0
    total_edex_count = 0
    total_pass_count = 0
    total_fail_count = 0
    table_data = [('Instrument', 'Input File', 'Output File', 'Stream', 'YAML_count', 'EDEX_count', 'Pass', 'Fail')]

    longest = {}
    last_label = {}

    for instrument in sorted(scorecard.keys()):
        for test_file in sorted(scorecard[instrument].keys()):
            for yaml_file in sorted(scorecard[instrument][test_file].keys()):
                for stream in sorted(scorecard[instrument][test_file][yaml_file].keys()):
                    results = scorecard[instrument][test_file][yaml_file][stream]

                    edex_count, yaml_count, fail_count = results
                    pass_count = yaml_count - len(fail_count)

                    labels = {}

                    for name in 'stream instrument test_file yaml_file'.split():
                        val = locals().get(name)
                        if last_label.get(name) == val:
                            labels[name] = '---'
                        else:
                            labels[name] = val
                            last_label[name] = val
                        longest[name] = max((len(val), longest.get(name, 0)))

                    table_data.append((instrument,
                                       test_file,
                                       yaml_file,
                                       stream,
                                       yaml_count,
                                       edex_count,
                                       pass_count,
                                       len(fail_count)))

                    total_yaml_count += yaml_count
                    total_edex_count += edex_count
                    total_pass_count += pass_count
                    total_fail_count += len(fail_count)
                    total_instrument_count += 1

    format_string = format_string % (longest['instrument'], longest['test_file'], longest['yaml_file'], longest['stream'])

    banner = format_string.format(*table_data[0])
    half_banner = len(banner)/2
    result.append('-' * half_banner + 'TEST RESULTS' + '-' * half_banner)
    result.append(banner)

    for row in table_data[1:]:
        result.append(format_string.format(*row))

    result.append('')
    result.append('-' * (len(banner)+12))
    row = ['Total Instrument', '', '', '', 'Total YAML', 'Total EDEX', 'Total Pass', 'Total Fail']
    result.append(format_string.format(*row))

    row = [total_instrument_count, '', '', '', total_yaml_count, total_edex_count, total_pass_count, total_fail_count]
    result.append(format_string.format(*row))
    return '\n'.join(result), table_data


def create_handler(instrument):
    #create filehandler for each instrument
    file_path = 'output/%s.log' % instrument
    fh = logging.FileHandler(file_path)
    fh.setFormatter(formatter)
    return fh


def dump_csv(table_data):
    fh = open(os.path.join(output_dir, 'results.csv'), 'w')
    for row in table_data:
        row = [str(x) for x in row]
        fh.write(','.join(row) + '\n')
    fh.close()


def test(test_cases):
    scorecard = {}
    logfile = find_latest_log()

    handler = None
    for test_case in test_cases:
        if handler is not None: log.removeHandler(handler)
        handler = create_handler(test_case.instrument)
        log.addHandler(handler)


        log.debug('Processing test case: %s', test_case)
        for test_file, yaml_file in test_case.pairs:
            purge_edex()
            copy_file(test_case.resource, test_case.endpoint, test_file)
            expected = get_expected(os.path.join(drivers_dir, test_case.resource, yaml_file))
            watch_log_for('Ingest: EDEX: Ingest', logfile=logfile)
            time.sleep(1)
            for stream in expected:
                scorecard.setdefault(test_case.instrument, {}) \
                         .setdefault(test_file, {}) \
                         .setdefault(yaml_file, {})[stream] = test_results(expected[stream], stream)

    result, table_data = parse_scorecard(scorecard)
    log.info(result)
    dump_csv(table_data)


def test_bulk(test_cases):
    expected = {}
    scorecard = {}
    num_files = 0

    purge_edex()
    logfile = find_latest_log()

    for test_case in test_cases:
        num_files += 1
        log.debug('Processing test case: %s', test_case)

        for test_file, yaml_file in test_case.pairs:
            copy_file(test_case.resource, test_case.endpoint, test_file)
            this_expected = get_expected(os.path.join(drivers_dir, test_case.resource, yaml_file))
            for stream in this_expected:
                expected[(test_case.instrument, test_file, yaml_file, stream)] = this_expected[stream]

    watch_log_for('Ingest: EDEX: Ingest', logfile=logfile, expected_count=num_files, timeout=600)
    # sometimes edex needs to catch its breath after so many files... sleep a bit
    time.sleep(5)

    handler = last_instrument = None
    for k,v in expected.iteritems():
        instrument, test_file, yaml_file, stream = k
        if instrument != last_instrument:
            if handler is not None:
                log.removeHandler(handler)
            log.addHandler(create_handler(instrument))
            last_instrument = instrument

        scorecard.setdefault(instrument, {}) \
                 .setdefault(test_file, {}) \
                 .setdefault(yaml_file, {})[stream] = test_results(expected[(instrument,test_file,yaml_file,stream)], stream)

    result, table_data = parse_scorecard(scorecard)
    log.info(result)
    dump_csv(table_data)


if __name__ == '__main__':
    test_cases = []
    if len(sys.argv) <= 1:
        test_cases = read_test_cases('test_cases')
    else:
        for each in sys.argv[1:]:
            test_cases.extend(list(read_test_cases(each)))

    test_bulk(test_cases)
