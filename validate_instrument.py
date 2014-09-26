#!/usr/bin/env python

import pprint
import time
import sys
import yaml
import edex_tools
import logger


log = logger.get_logger('validate_instrument', file_output='output/instrument_verify.log')


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


def test_results(expected):
    retrieved = edex_tools.get_from_edex()
    log.debug('Retrieved %d records from edex:', len(retrieved))
    log.debug(pprint.pformat(retrieved, depth=3))
    log.debug('Retrieved %d records from expected data file:', len(expected))
    log.debug(pprint.pformat(expected, depth=3))
    failures = compare(retrieved, expected)
    return len(retrieved), len(expected), failures

def get_expected(filename):
    """
    Loads expected results from the supplied YAML file
    :param filename:
    :return: list of records containing the expected results
    """
    return_data = []
    for each in open(filename, 'r').read().split('\n\n'):

        data = yaml.load(each)
        if data is not None:



            #todo - need to parse data into a structure that may be easier to compare with the edex data

            return_data.append(data)

    return return_data


def test(file_name):

    scorecard = {}
    log.debug('Processing test case: %s', file_name)
    expected = get_expected(file_name)
    print expected
    time.sleep(1)
    #TODO HOW TO GET INSTRUMENT NAME??? (PARTICLE NAME)
    test_results(expected)


if __name__ == '__main__':
    test_cases = []
    if len(sys.argv) <= 1:
        print('useage: ./validate_instrument [file1] [file2] ...')
    else:
        for each in sys.argv[1:]:
            test(each)