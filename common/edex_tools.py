#!/usr/bin/env python
import glob
import os

import pprint
import ntplib
import numpy

import qpid.messaging as qm
import time
import requests
import shutil
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


def get_qpid():
    global qpid_session
    if qpid_session is None:
        conn = qm.Connection(host=host, port=port, username=user, password=user)
        conn.open()
        qpid_session = conn.session()
    return qpid_session


def purge_edex():
    purge_message = qm.Message(content='PURGE_ALL_DATA', content_type='text/plain', user_id=user)
    log.info('Purging edex')
    get_qpid().sender('purgeRequest').send(purge_message)


def send_file_to_queue(filename, queue, delivery_type, sensor):
    props = {'deliveryType': delivery_type, 'sensor': sensor}
    ingest_message = qm.Message(content=filename, content_type='text/plain', user_id=user, properties=props)
    get_qpid().sender(queue).send(ingest_message)


def clear_hdf5():
    if os.path.exists(hdf5dir):
        shutil.rmtree(hdf5dir)


def ntptime_to_string(t):
    t = ntplib.ntp_to_system_time(t)
    millis = '%f' % (t-int(t))
    millis = millis[1:5]
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(t)) + millis + 'Z'


def get_netcdf(host, stream_name, sensor='null', start_time=None, stop_time=None, output_dir='.'):
    url = 'http://%s:12570/sensor/m2m/inv/%s/%s' % (host, stream_name, sensor)
    netcdf_file = os.path.join(output_dir, '%s-%s.nc' % (stream_name, sensor))
    with open(netcdf_file, 'wb') as fh:
        r = requests.get(url, params={'format': 'application/netcdf'})
        fh.write(r.content)


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


def get_from_edex(hostname, stream_name, sensor='null', start_time=None, stop_time=None, timestamp_as_string=False):
    """
    Retrieve all stored sensor data from edex
    :return: list of edex records
    """
    url = 'http://%s:12570/sensor/m2m/inv/%s/%s' % (hostname, stream_name, sensor)
    if start_time and stop_time:
        start_time = ntptime_to_string(start_time-.1)
        stop_time = ntptime_to_string(stop_time+.1)
        url += '/%s/%s' % (start_time, stop_time)

    r = requests.get(url)
    records = get_record_json(r)

    log.debug('RETRIEVED:')
    log.debug(pprint.pformat(records, depth=3))
    d = {}
    for record in records:
        timestamp = record.get('internal_timestamp')
        if timestamp is None:
            timestamp = record.get('port_timestamp')
        if timestamp is not None and timestamp_as_string:
            timestamp = '%12.3f' % timestamp

        stream_name = record.get('stream_name')
        key = (timestamp, stream_name)
        if key in d:
            log.error('Duplicate record found in retrieved values %s', key)
        else:
            d[key] = record
    return d


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


def isnumber(x):
    """
    :param x: number to be evaluated
    :return: true if x has an __int__ method, false otherwise
    """
    return hasattr(x, '__int__')


def edex_get_streams(hostname):
    """
    Get list of available streams
    :param hostname:  EDEX hostname or qualified IP address
    :return:  list of streams
    """
    url = 'http://%s:12570/sensor/m2m/inv' % hostname
    record = requests.get(url)
    return get_record_json(record)


def edex_get_instruments(hostname):
    """
    Get dictionary of available instruments
    :param hostname:  EDEX hostname or qualified IP address
    :return:  dictionary containing stream name with associated instruments
    """
    instruments = {}
    for stream in edex_get_streams(hostname):
        url = 'http://%s:12570/sensor/m2m/inv/%s' % (hostname, stream)
        # print 'checking: %s' % url
        record = requests.get(url)
        instruments[stream] = get_record_json(record)
    return instruments


def edex_get_json(hostname, stream, sensor, save_sample_data=False, sample_data_file='mda_sample.json'):
    """
    Fetch results from EDEX using URL lookup
    :param hostname:  EDEX host
    :param stream:  stream name
    :param sensor:  sensor name
    :return:  JSON formatted data
    """
    url = 'http://%s:12570/sensor/user/inv/%s/%s' % (hostname, stream, sensor)
    r = requests.get(url)
    if save_sample_data:
        with open(sample_data_file, 'wb') as f:
            f.write(r.content)
    return get_record_json(r)


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
