#!/usr/bin/env python
import glob
import os

import pprint
import math
import ntplib
import numpy

import qpid.messaging as qm
import time
import requests
import struct
from logger import get_logger
import simplejson.scanner


log = get_logger()

edex_dir = os.getenv('EDEX_HOME')
if edex_dir is None:
    edex_dir = os.path.join(os.getenv('HOME'), 'uframes', 'ooi', 'uframe-1.0', 'edex')
hdf5dir = os.path.join(edex_dir, 'data', 'hdf5', 'sensorreading')
log_dir = os.path.join(edex_dir, 'logs')


qpid_session = None
user = 'guest'
host = 'localhost'
port = 5672

DEFAULT_STANDARD_TIMEOUT = 60

EDEX_BASE_URL = 'http://%s:12575/sensor/inv/%s/%s/%s'


def get_qpid():
    global qpid_session
    if qpid_session is None:
        conn = qm.Connection(host=host, port=port, username=user, password=user)
        conn.open()
        qpid_session = conn.session()
    return qpid_session


def purge_edex(table='PURGE_ALL_DATA'):
    purge_message = qm.Message(content=table, content_type='text/plain', user_id=user)
    log.info('Purging edex')
    get_qpid().sender('purgeCass').send(purge_message)


def send_file_to_queue(filename, queue, delivery_type, sensor, deploymentNumber):
    props = {'deliveryType': delivery_type, 'sensor': sensor, 'deploymentNumber': deploymentNumber}
    ingest_message = qm.Message(content=filename, content_type='text/plain', user_id=user, properties=props)
    get_qpid().sender(queue).send(ingest_message)


def ntptime_to_string(t):
    t = ntplib.ntp_to_system_time(t)
    millis = '%f' % (t-int(t))
    millis = millis[1:5]
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(t)) + millis + 'Z'


def get_record_json(record):
    """
    Perform a safe fetch of JSON data from the supplied data record.
    @return  JSON record or empty list if not found or error.
    """
    try:
        return record.json()
    except simplejson.scanner.JSONDecodeError as e:
        log.warn('unable to decode record as JSON - %s - skipping data: %r', e, record.content)
        return []


def get_edex_metadata(hostname, subsite, node, sensor):
    r = requests.get(EDEX_BASE_URL % (hostname, subsite, node, sensor) + '/metadata/parameters')
    try:
        r = r.json()
    except:
        log.error('Unable to decode parameter metadata from edex: %r', r.content)

    d = {}
    for each in r:
        d[each['particleKey']] = each

    return d


def get_from_edex(hostname, subsite, node, sensor, method, stream, start_time, stop_time, timestamp_as_string=False, netcdf=False):
    """
    Retrieve all stored sensor data from edex
    :return: list of edex records
    """
    url = EDEX_BASE_URL % (hostname, subsite, node, sensor) + '/%s/%s' % (method, stream)
    data = {}

    start_time = ntptime_to_string(start_time-.1)
    stop_time = ntptime_to_string(stop_time+.1)
    data['beginDT'] = start_time
    data['endDT'] = stop_time

    r = requests.get(url, params=data)

    if netcdf:
        netcdf_file = os.path.join('%s-%s.nc' % (stream, sensor))
        with open(netcdf_file, 'wb') as fh:
            r = requests.get(url, params={'format': 'application/netcdf'})
            fh.write(r.content)
            return

    records = get_record_json(r)

    log.debug('RETRIEVED:')
    log.debug(pprint.pformat(records, depth=3))
    d = {}
    for record in records:
        timestamp = record.get('pk', {}).get('time')
        restore_lists(record)

        if timestamp is not None and timestamp_as_string:
            timestamp = '%12.3f' % timestamp

        record['timestamp'] = timestamp
        d.setdefault((stream, timestamp), []).append(record)

    return d


def nanize(l):
    rlist = []
    for x in l:
        if x == u'NaN':
            rlist.append(float('nan'))
        else:
            rlist.append(x)
    return rlist


def restore_lists(record):
    shapes = []
    for k in record:
        if k.endswith('_shape'):
            shapes.append(k)

    for k in shapes:
        array_key = k.replace('_shape','')
        shape = record[k]
        array = numpy.array(nanize(record[array_key]))

        if numpy.product(shape) == len(array):
            array = array.reshape(shape)
        else:
            log.error('Shape wrong? %r %d %s', shape, len(array), array)
        record[array_key] = array.tolist()
        del(record[k])


# noinspection PyClassHasNoInit
class FAILURES:
    MISSING_SAMPLE = 'MISSING_SAMPLE'
    MISSING_FIELD = 'MISSING_FIELD'
    BAD_VALUE = 'BAD_VALUE'
    UNEXPECTED_VALUE = 'UNEXPECTED_VALUE'
    AMBIGUOUS = 'AMBIGUOUS'


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
    log.info('timeout value: %s', timeout)

    end_time = time.time() + timeout
    count = 0

    try:
        while time.time() < end_time:
            data = logfile.read()
            for line in data.split('\n'):
                if expected_string in line:
                    count += 1
                    log.info('Found expected string %d times of %d', count, expected_count)
                    if count == expected_count:
                        return True
            time.sleep(.1)
    except KeyboardInterrupt:
        pass
    return False


def parse_scorecard(scorecard):
    result = ['SCORECARD:']
    format_string = "{: <%d}   {: <%d}   {: <%d}   {: <%d} {: >15} {: >15} {: >15} {: >15}"

    total_instrument_count = 0
    total_yaml_count = 0
    total_edex_count = 0
    total_pass_count = 0
    total_fail_count = 0
    table_data = [['Instrument', 'Input File', 'Output File', 'Stream', 'YAML_count', 'EDEX_count', 'Pass', 'Fail']]

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

                    table_data.append([instrument,
                                       test_file,
                                       yaml_file,
                                       stream,
                                       yaml_count,
                                       edex_count,
                                       pass_count,
                                       len(fail_count)])

                    total_yaml_count += yaml_count
                    total_edex_count += edex_count
                    total_pass_count += pass_count
                    total_fail_count += len(fail_count)
                    total_instrument_count += 1

    format_string = format_string % (
        longest.get('instrument', 10),
        longest.get('test_file', 10),
        longest.get('yaml_file', 10),
        longest.get('stream', 10))

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


def edex_mio_report(hostname, stream, instrument, output_dir='.'):
    """
    Calculate statistics for captured data stream and write to CSV file output_dir/<stream>-<instrument>.csv.
    :param stream:      stream name
    :param instrument:  instrument name
    :param output_dir:  location to write mio report
    :return:            none
    """

    stat_file = os.path.join(output_dir, '%s-%s.csv' % (stream, instrument))
    json_file = os.path.join(output_dir, '%s-%s.json' % (stream, instrument))

    data = edex_get_json(hostname, stream, instrument, sample_data_file=json_file, save_sample_data=True)
    d = {}

    # first pass to extract data values
    for record in data:
        for param in record:
            d.setdefault(param, []).append(record[param])

    # second pass to compute statistics
    log.info('saving statistics to %s', stat_file)
    with open(stat_file, 'wb') as f:
        f.write("key,count,min,max,median,mean,sigma\n")
        for param in sorted(d.keys()):
            value = numpy.array(d[param])
            dtype = value.dtype
            if dtype.kind in 'iuf':
                # expect 1 or 2-d arrays - other multi-dimensional arrays are not supported
                if len(value.shape) > 1:
                    value = numpy.transpose(value)
                    for i, row in enumerate(value):
                        v_min = numpy.min(row)
                        v_max = numpy.max(row)
                        median = numpy.median(row)
                        mean = numpy.mean(row)
                        sigma = numpy.std(row)
                        f.write("%s(%d),%d,%f,%f,%f,%f,%f\n" % (param, i, len(row), v_min, v_max, median, mean, sigma))

                v_min = numpy.min(value)
                v_max = numpy.max(value)
                median = numpy.median(value)
                mean = numpy.mean(value)
                sigma = numpy.std(value)
                f.write("%s,%d,%f,%f,%f,%f,%f\n" % (param, len(value), v_min, v_max, median, mean, sigma))
            else:
                s = set()
                s.update(list(value.flatten()))
                log.info(" - skipping non-numeric data for %s", param)


def check_for_sign_error(a, b):
    values = []

    for each in [a, b]:
        if each < 0:
            if each >= -2**7:
                dtype = 'b'
            elif each >= -2**15:
                dtype = 'h'
            elif each >= -2**32:
                dtype = 'i'
            else:
                dtype = 'l'
            values.append(struct.unpack('<%s' % dtype.upper(), struct.pack('<%s' % dtype, each))[0])
        else:
            values.append(each)

    if len(values) == 2 and values[0] == values[1]:
        return True


def same(a, b, errors, float_tolerance=0.001):
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
        return all([same(a[k], b[k], errors, float_tolerance=float_tolerance) for k in a])

    if type(a) is list:
        if len(a) != len(b):
            return False
        return all([same(a[i], b[i], errors, float_tolerance=float_tolerance) for i in xrange(len(a))])

    if type(a) is float or type(b) is float:
        try:
            if type(a) is unicode:
                a = str(a)
            if type(b) is unicode:
                b = str(b)
            a = float(a)
            b = float(b)
            if abs(a-b) < float_tolerance:
                return True
            if math.isnan(a) and math.isnan(b):
                return True
        except:
            pass
        message = 'FAILED floats: %r %r' % (a, b)
        errors.append(message)

    if type(a) in string_types and type(b) in string_types:
        return a.strip() == b.strip()

    if type(a) is int and type(b) is int:
        if check_for_sign_error(a, b):
            errors.append('Detected unsigned/signed issue: %r, %r' % a, b)

    return False


def compare(stored, expected, metadata, ignore_nulls=False, lookup_preferred_timestamp=False):
    """
    Compares a set of expected results against the retrieved values
    :param stored:
    :param expected:
    :return: list of failures
    """
    failures = []
    for record in expected:
        if lookup_preferred_timestamp:
            timestamp = '%12.3f' % record.get(record.get('preferred_timestamp'), 0.0)
        else:
            timestamp = '%12.3f' % record.get('internal_timestamp', 0.0)
        stream_name = record.get('particle_type') or record.get('stream_name')
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

        matches = []
        for each in stored.get((stream_name, timestamp), []):
            if each['stream_name'] == stream_name and each['timestamp'] == timestamp:
                f, errors = diff(stream_name, record, each, metadata, ignore_nulls=ignore_nulls)
                matches.append((len(f), each, f, errors))

        matches.sort()
        if len(matches) == 0:
            m = 'Unable to find a matching sample: %s %s' % (stream_name, timestamp)
            failures.append((FAILURES.MISSING_SAMPLE, m))
            log.error(m)
        if len(matches) > 0:
            failcount, match, f, errors = matches[0]
            if failcount > 0:
                # we had at least one failure, but this record
                # was the closest match; record the failures
                failures.append(f)
                for error in errors:
                    log.error(error)

    return failures


def check_fill(a, b):
    if type(a) == int:
        try:
            b = int(b)
            return a == b
        except:
            return False

    if type(a) == float:
        try:
            b = float(b)
            return a == b
        except:
            return False

    return a == b

def diff(stream, a, b, metadata, ignore=None, rename=None, ignore_nulls=False, float_tolerance=0.001):
    """
    Compare two data records
    :param a:
    :param b:
    :param ignore: fields to ignore
    :param rename: fields to rename before comparison
    :return: list of failures
    """
    if ignore is None:
        ignore = ['particle_object', 'quality_flag', 'driver_timestamp', 'ingestion_timestamp',
                  'stream_name', 'preferred_timestamp', 'port_timestamp', 'pk', 'timestamp', 'provenance']
    if rename is None:
        rename = {'particle_type': 'stream_name'}

    failures = []
    errors = []

    # verify from expected to retrieved
    for k, v in a.iteritems():
        if k in ignore or k.startswith('_'):
            continue
        if k in rename:
            k = rename[k]
        if k not in b:
            message = '%s - missing key: %s in retrieved record' % (stream, k)
            errors.append(message)
            if ignore_nulls and v is None:
                log.info('Ignoring NULL value from expected data')
            else:
                failures.append((FAILURES.MISSING_FIELD, message))
            continue

        if type(v) == dict:
            v = v.get('value')

        if not same(v, b[k], errors, float_tolerance=float_tolerance):
            # check if fill value
            fill = metadata.get(k, {}).get('fillValue')
            if fill is not None and check_fill(b[k], fill):
                log.info('%s - Found fill value: key=%r expected=%r retrieved=%r' % (stream, k, v, b[k]))
            else:
                message = '%s - non-matching value: key=%r expected=%r retrieved=%r' % (stream, k, v, b[k])
                failures.append((FAILURES.BAD_VALUE,
                                 message))
                errors.append(message)

    # verify no extra (unexpected) keys present in retrieved data
    for k in b:
        if k not in a and k not in ignore:
            # check if fill value
            fill = metadata.get(k, {}).get('fillValue')
            if fill is None or not check_fill(b[k], fill):
                failures.append((FAILURES.UNEXPECTED_VALUE, (stream, k)))
                message = '%s - item in retrieved data not in expected data: %s' % (stream, k)
                errors.append(message)

    return failures, errors
