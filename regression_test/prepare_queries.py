#!/usr/bin/env python
import datetime
import os
from collections import namedtuple

import pytz
import requests
import sys
import yaml

from dateutil.parser import parse

Query = namedtuple('Query', ['host', 'subsite', 'node', 'sensor', 'method', 'stream', 'start', 'stop'])

earliest_time = datetime.datetime(2015, 1, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
latest_time = datetime.datetime(2017, 2, 1, 0, 0, 0, 0, tzinfo=pytz.utc)
time_format = '%Y-%m-%dT%H:%M:%S.000Z'


def get_toc(hostname):
    return requests.get('http://{}:12576/sensor/inv/toc'.format(hostname)).json()


def parse_toc(hostname):
    toc = get_toc(hostname)
    queries = []
    for each in toc['instruments']:
        subsite = str(each['platform_code'])
        node = str(each['mooring_code'])
        sensor = str(each['instrument_code'])
        for stream in each['streams']:
            start = max(parse(stream['beginTime']), earliest_time)
            stop = min(parse(stream['endTime']), latest_time)

            start = start.strftime(time_format)
            stop = stop.strftime(time_format)

            # skip any streams with equivalent start/stop times
            # this usually indicates only a single data point exists
            if start == stop:
                continue

            method = str(stream['method'])
            name = str(stream['stream'])
            query = Query(hostname, subsite, node, sensor, method, name, start, stop)
            queries.append(query)
    return queries


def write_query(query):
    fname = '{0.subsite}-{0.node}-{0.sensor}-{0.method}-{0.stream}.yml'.format(query)
    yaml.safe_dump(dict(query._asdict()), open(fname, 'w'), default_flow_style=False)


def main():
    hostname = 'localhost'
    if len(sys.argv) > 1:
        hostname = sys.argv[1]
    if not os.path.exists('queries'):
        os.mkdir('queries')

    os.chdir('queries')
    for query in parse_toc(hostname):
        write_query(query)


if __name__ == '__main__':
    main()