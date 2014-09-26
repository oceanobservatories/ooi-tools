#!/usr/bin/env python

import pprint
import urllib2
import json

import qpid.messaging as qm
from logger import get_logger


log = get_logger('edex_tools')


def purge_edex():
    USER = 'guest'
    HOST = 'localhost'
    PORT = 5672
    PURGE_MESSAGE = qm.Message(content='PURGE_ALL_DATA', content_type='text/plain', user_id=USER)

    log.info('Purging edex')
    conn = qm.Connection(host=HOST, port=PORT, username=USER, password=USER)
    conn.open()
    conn.session().sender('purgeRequest').send(PURGE_MESSAGE)
    conn.close()


def get_from_edex(host, stream_name):
    """
    Retrieve all stored sensor data from edex
    :return: list of edex records
    """
    all_data_url = 'http://%s:12570/sensor/user/inv/%s/null'
    proxy_handler = urllib2.ProxyHandler({})
    opener = urllib2.build_opener(proxy_handler)
    req = urllib2.Request(all_data_url % (host, stream_name))
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
