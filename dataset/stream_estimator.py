""" Stream Estimator.

Usage:
  stream_estimator.py (-f | --fetch) [--host hostname] [(-i input | --input input)] [<streams> ...]
  stream_estimator.py (-p | --parse) <dir> [(-i input | --input input)] [(-o output | --output output)]
  stream_estimator.py (-i input | --input input)
  stream_estimator.py -h | --help
  stream_estimator.py --version

Options:
  -h --help                   Show help.
  --version                   Show version.
  --fetch                     Fetch NetCDF data for listed streams (or all missing data streams).
  --parse                     Parse NetCDF data files in and below dir and gather size statistics.
  --host hostname             Hostname for OOI CI services [default: ooiufs01.ooi.rutgers.edu].
  -i input, --input input     Read previous size estimate file.  If no other arguments are provided, will list
                              all streams that do not have a size estimate.
  -o output, --output output  Specify new output for updates to the size estimate.

"""
# stream_estimator.py (-f | --fetch) [-i input] [--limit=<count>]
from docopt import docopt

import csv
import fnmatch
import os
from collections import Counter, namedtuple

import ntplib
import requests
import json
import time
from datetime import datetime
from dateutil.parser import parse
import xarray as xr

EDEX_BASE_URL = 'http://%s:12576/sensor/inv/%s/%s/%s'
DEFAULT_HOST = 'ooiufs01.ooi.rutgers.edu'


def timestamp_to_ntp(ts):
    dt = parse(ts).replace(tzinfo=None)
    return (dt-datetime(1900, 1, 1)).total_seconds()


def timestamp_to_datetime(ts):
    """
    :param ts:  formatted time string, e.g. '2014-05-10T19:10:51.794Z'
    :return: datetime object
    """
    return datetime.strptime(ts, '%Y-%m-%dT%H:%M:%S.%fZ')


def ntptime_to_string(t):
    t = ntplib.ntp_to_system_time(t)
    millis = '%f' % (t-int(t))
    millis = millis[1:5]
    return time.strftime('%Y-%m-%dT%H:%M:%S', time.gmtime(t)) + millis + 'Z'


def get_from_edex(hostname, subsite, node, sensor, method, stream, start_time, stop_time, limit=None):
    """
    Retrieve all stored sensor data from edex

    :param hostname:  url for uframe server (e.g. http://ooiufs01.rutgers.edu, localhost)
    :param subsite:  array (e.g. RS03AXPS)
    :param node:  mooring (e.g. RID01)
    :param sensor:  instrument port and sensor (e.g. 05-PARADA101)
    :param method:  method type (e.g. telemetered, streamed, recovered)
    :param stream:  stream name (e.g. )
    :param start_time:  begin time (NTP) for retrieval window
    :param stop_time:  end time (NTP) for retrieval window
    :param limit:  when set, this will be a synchronous request and data will be sub-sampled to specified value
    :return: nothing
    """

    url = EDEX_BASE_URL % (hostname, subsite, node, sensor) + '/%s/%s' % (method, stream)
    data = {}

    start_time = ntptime_to_string(start_time-.1)
    stop_time = ntptime_to_string(stop_time+.1)

    data['beginDT'] = start_time
    data['endDT'] = stop_time
    data['format'] = 'application/netcdf'
    data['user'] = 'estimator'
    if limit:
        data['limit'] = limit

    print 'fetching NetCDF for %s...' % url
    request_file = os.path.join('%s-%s.request' % (stream, sensor))
    with open(request_file, 'wb') as fh:
        r = requests.get(url, params=data)
        fh.write(r.content)
    print '  request complete - %s' % request_file


StreamInfo = namedtuple('StreamInfo', ['count', 'refdes', 'stream', 'method', 'begin', 'end'])


class StreamEstimator:
    def __init__(self, hostname=DEFAULT_HOST):
        """

        """
        self.sizes = {}  # in Bytes per particle for associated NetCDF file
        self.stream_count = Counter()
        self.host = hostname
        self.port = 12576
        self.base_url = 'http://%s:%d/sensor/inv' % (self.host, self.port)
        self.toc_url = 'http://%s:%d/sensor/inv/toc' % (self.host, self.port)
        self.toc = {}

    def _get_toc(self):
        if self.toc:
            return

        print 'Fetching Table of Contents...'
        r = requests.get(self.toc_url)
        self.toc = json.loads(r.content)

        self.stream_map = {}
        for instrument in self.toc['instruments']:
            for stream in instrument['streams']:
                try:
                    begin = timestamp_to_ntp(stream['beginTime'])
                    end = timestamp_to_ntp(stream['endTime'])
                except ValueError as e:
                    print '  %s invalid time range specified (%s-%s): %s' % \
                          (stream['stream'], stream['beginTime'], stream['endTime'], e)
                    continue

                si = StreamInfo(stream['count'],
                                instrument['reference_designator'],
                                stream['stream'],
                                stream['method'],
                                begin,
                                end)
                self.stream_map.setdefault(stream['stream'], []).append(si)

    def _count_particles(self):
        """
        generate full list of all streams and their count in the system
        :return:
        """
        self._get_toc()

        for instrument in self.toc['instruments']:
            for stream in instrument['streams']:
                particles = stream['count']
                self.stream_count[stream['stream']] += particles

    def missing_streams(self):
        """
        get all streams for which we have not already collected data
        :return: sorted list of stream names
        """
        if not self.stream_count:
            self._count_particles()

        return sorted(set(self.stream_count) - set(self.sizes))

    def _fetch_netcdf(self, si):
        """
        fetch NetCDF file for given stream
        :param si:  StreamInfo object defining stream to fetch
        :return:  nothing - return is synchronous if limit is not 0
        """
        subsite, node, sensor = si.refdes.split('-', 2)

        # limit time range to large enough set of particles
        begin = si.begin
        end = si.end
        if si.count > 100000:
            duration = end - begin
            duration /= si.count / 100000
            end = begin + duration
        get_from_edex(self.host, subsite, node, sensor, si.method, si.stream, begin, end)

    def fetch_streams(self, streams):
        """
        get NetCDF files for the given streams
        :param streams:  list of stream names
        :return: nothing - will have to rerun parse_netcdf after asynchronous collection
        """
        self._get_toc()

        for mstream in streams:
            # find the stream in the TOC and make a NetCDF request
            sources = self.stream_map.get(mstream)
            if sources is not None:
                sources.sort()
                si = sources[-1]
                self._fetch_netcdf(si)

    def parse_netcdf(self, directory):
        """
        walk the NetCDF responses and add file sizes returns to the results
        - overwrites existing particle statistics
        :param directory:  directory in which to search for NetCDF files (all subdirectories will be scanned)
        :return:
        """
        for root, dirs, files in os.walk(directory, topdown=False):
            files.sort(key=lambda f: os.stat(os.path.join(root, f)).st_size, reverse=True)
            for f in fnmatch.filter(files, '*.nc'):
                filename = os.path.join(root, f)
                filesize = os.stat(filename).st_size

                ds = xr.open_dataset(filename, decode_times=False)
                if 'obs' in ds:
                    particles = len(ds.obs)
                    stream_name = ds.attrs['stream']
                    if stream_name not in self.sizes:
                        self.sizes[stream_name] = float(filesize) / particles

    def read_config(self, filename):
        """
        populate with pre-generated configuration
        :param filename:  stream estimate configuration file
        :return:  none
        """
        with open(filename, 'rb') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                stream_name = row[0]
                psize = row[1]
                self.sizes[stream_name] = psize

    def write_config(self, filename):
        """
        write (or update) the stream estimate configuration file
        :param filename:
        :return:
        """
        with open(filename, 'wb') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['stream name', 'bytes per particle'])
            for key, value in sorted(self.sizes.iteritems()):
                writer.writerow([key, value])


if __name__ == '__main__':
    arguments = docopt(__doc__, version='Stream Estimator 1.0')

    est = StreamEstimator(arguments['--host'])

    # get configuration file
    if arguments['--input']:
        size_config_file = arguments['--input']
        print 'establishing baseline from %s' % arguments['--input']
        est.read_config(size_config_file)
    else:
        # new configuration file
        size_config_file = 'stream_nc_sizes.cfg'

    # determine mode
    if arguments['--fetch']:
        if arguments['<streams>']:
            print 'fetching NetCDF for %r' % set(arguments['<streams>'])
            est.fetch_streams(set(arguments['<streams>']))
        else:
            if arguments['--input']:
                print 'fetching NetCDF for remaining streams'
            else:
                print 'fetching NetCDF for all streams'
            est.fetch_streams(est.missing_streams())

    elif arguments['--parse']:
        path = arguments['<dir>']
        if not os.path.isdir(path):
            print '%s is not a valid directory' % path
            raise Exception('Invalid command line arguments')

        print 'parsing NetCDF files in %s' % arguments['<dir>']
        est.parse_netcdf(arguments['<dir>'])

        if arguments['--output']:
            print 'archiving size estimates file %s' % arguments['--output']
            size_config_file = arguments['--output']
        est.write_config(size_config_file)

    else:
        est.read_config(size_config_file)
        print est.missing_streams()
