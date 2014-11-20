#!/usr/bin/env python
import os

import pprint
import urllib2
import json
import ntplib
import numpy

import qpid.messaging as qm
import time
import requests
from logger import get_logger


log = get_logger()


def purge_edex():
    user = 'guest'
    host = 'localhost'
    port = 5672
    purge_message = qm.Message(content='PURGE_ALL_DATA', content_type='text/plain', user_id=user)

    log.info('Purging edex')
    conn = qm.Connection(host=host, port=port, username=user, password=user)
    conn.open()
    conn.session().sender('purgeRequest').send(purge_message)
    conn.close()


def ntptime_to_string(t):
    t = ntplib.ntp_to_system_time(t)
    millis = '%f' % (t-int(t))
    millis = millis[1:5]
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(t)) + millis + 'Z'


def get_netcdf(host, stream_name, sensor='null', start_time=None, stop_time=None, output_dir='.'):
    url = 'http://%s:12570/sensor/m2m/inv/%s/%s' % (host, stream_name, sensor)
    netcdf_file = os.path.join(output_dir, '%s-%s.ncdf' % (stream_name, sensor))
    with open(netcdf_file, 'wb') as fh:
        r = requests.get(url, params={'format': 'application/netcdf'})
        fh.write(r.content)


def get_from_edex(host, stream_name, sensor='null', timestamp_as_string=False, start_time=None, stop_time=None):
    """
    Retrieve all stored sensor data from edex
    :return: list of edex records
    """
    url = 'http://%s:12570/sensor/m2m/inv/%s/%s' % (host, stream_name, sensor)
    r = requests.get(url)
    records = r.json()

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


class FAILURES:
    MISSING_SAMPLE = 'MISSING_SAMPLE'
    MISSING_FIELD = 'MISSING_FIELD'
    BAD_VALUE = 'BAD_VALUE'
    UNEXPECTED_VALUE = 'UNEXPECTED_VALUE'
    AMBIGUOUS = 'AMBIGUOUS'


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


def edex_get_streams(host):
    """
    Get list of available streams
    :param host:  EDEX hostname or qualified IP address
    :return:  list of streams
    """
    url = 'http://%s:12570/sensor/m2m/inv' % host
    return requests.get(url).json()


def edex_get_instruments(host):
    """
    Get dictionary of available instruments
    :param host:  EDEX hostname or qualified IP address
    :return:  dictionary containing stream name with associated instruments
    """
    instruments = {}
    for stream in edex_get_streams(host):
        url = 'http://%s:12570/sensor/m2m/inv/%s' % (host, stream)
        # print 'checking: %s' % url
        instruments[stream] = requests.get(url).json()
    return instruments


def edex_get_json(host, stream, sensor, save_sample_data=False, sample_data_file='mda_sample.json'):
    """
    Fetch results from EDEX using URL lookup
    :param host:  EDEX host
    :param stream:  stream name
    :param sensor:  sensor name
    :return:  JSON formatted data
    """
    url = 'http://%s:12570/sensor/user/inv/%s/%s' % (host, stream, sensor)
    r = requests.get(url)
    if save_sample_data:
        with open(sample_data_file, 'wb') as f:
            f.write(r.content)
    return r.json()


def edex_mio_report(stream, instrument, data, output_dir='.'):
    """
    Calculate statistics for captured data stream and write to CSV file output_dir/<stream>-<instrument>.csv.
    :param stream:      stream name
    :param instrument:  instrument name
    :param data:        JSON formatted stream data
    :param output_dir:  location to write mio report
    :return:            none
    """
    d = {}
    # first pass to extract data values
    for record in data:
        for param in record:
            d.setdefault(param, []).append(record[param])

    # second pass to compute statistics
    stat_file = os.path.join(output_dir, '%s-%s.csv' % (stream, instrument))
    print 'saving statistics to %s' % stat_file
    with open(stat_file, 'wb') as f:
        f.write("key, min, max, median, mean, sigma\n")
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
                        f.write("%s(%d), %f, %f, %f, %f, %f\n" % (param, i, v_min, v_max, median, mean, sigma))

                v_min = numpy.min(value)
                v_max = numpy.max(value)
                median = numpy.median(value)
                mean = numpy.mean(value)
                sigma = numpy.std(value)
                f.write("%s, %f, %f, %f, %f, %f\n" % (param, v_min, v_max, median, mean, sigma))
            else:
                s = set()
                s.update(value)
                print " - skipping non-numeric data for %s" % param

