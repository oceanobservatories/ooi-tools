#!/usr/bin/env python
import logging
from collections import Counter, namedtuple

import os
import requests
import grequests
import urllib
import time

from datetime import datetime, timedelta
from functools32 import lru_cache
from requests.exceptions import ConnectTimeout
from simplejson import JSONDecodeError

HOST = 'portland-09.oceanobservatories.org'
BASE_URL = 'http://%s:12576/sensor' % HOST
BASE_AM_URL = 'http://%s:12573' % HOST
StreamInfo = namedtuple('Stream', 'subsite, node, sensor, method, stream start stop')


def get_logger(level):
    logger = logging.getLogger('sensor_inventory')
    logger.setLevel(level)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(level)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


log = get_logger(logging.DEBUG)


def datetime_from_msecs(msecs, fill=0):
    if msecs is None or msecs < 0:
        dt = datetime.utcfromtimestamp(fill) + timedelta(microseconds=1)
    else:
        dt = datetime.utcfromtimestamp(msecs / 1000.0)
    return dt


def make_isoformat(dt):
    # microsecond hack necessary because uframe won't accept time input without msecs
    if dt.microsecond == 0:
        dt += timedelta(microseconds=1)
    iso = dt.isoformat()
    if not iso.endswith('Z'):
        iso += 'Z'

    return iso


class AssetManagement(object):
    def __init__(self):
        self.base_url = BASE_AM_URL

    @lru_cache(20)
    def _get_events(self, subsite, node, sensor):
        qurl = os.path.join(self.base_url, 'assets', 'byReferenceDesignator', subsite, node, sensor, 'events')
        log.debug('AM query: %r', qurl)
        return requests.get(qurl).json()

    def get_deployments(self, subsite, node, sensor):
        deps = {}
        for event in self._get_events(subsite, node, sensor):
            if event.get('@class') == '.DeploymentEvent':
                start = make_isoformat(datetime_from_msecs(event.get('startDate')))
                stop = make_isoformat(datetime_from_msecs(event.get('endDate'), time.time()))
                number = event.get('deploymentNumber', 0)
                deps[number] = (start, stop)
        return deps

    def get_calibrations(self, subsite, node, sensor):
        cals = []
        for event in self._get_events(subsite, node, sensor):
            if event.get('@class') == '.CalibrationEvent':
                start = make_isoformat(datetime_from_msecs(event.get('startDate')))
                stop = make_isoformat(datetime_from_msecs(event.get('endDate'), time.time()))
                cc = event.get('calibrationCoefficient', 0)
                cals.append((start, stop, cc))
        return cals


class SensorInventory(object):
    def __init__(self, concurrency=20):
        self.base_url = os.path.join(BASE_URL, 'inv')
        self._concurrency = concurrency

    def _fetch(self, items, filter_list=None):
        items = self._filter_last(items, filter_list)
        queries = []
        for each in items:
            url = os.path.join(self.base_url, *each)
            log.debug('Fetching %r', url)
            queries.append(grequests.get(url))

        responses = self._map_responses(queries)
        return [x + (r,) for x, response in zip(items, responses) for r in response]

    def _map_responses(self, responses):
        response_list = []
        for response in grequests.map(responses, size=self._concurrency):
            try:
                response_list.append(response.json())
            except JSONDecodeError:
                response_list.append([])
        return response_list

    @staticmethod
    def _filter_last(items, filter_items):
        if not filter_items:
            return items
        if not isinstance(filter_items, (list, tuple)):
            filter_items = (filter_items,)

        return [i for i in items if i[-1] in filter_items]

    def get_subsites(self):
        return [(x,) for x in requests.get(self.base_url).json()]

    def get_nodes(self, subsites=None):
        if not subsites:
            subsites = self.get_subsites()
        elif not (isinstance(subsites, (tuple, list))):
            subsites = [(subsites,)]
        else:
            temp = []
            for subsite in subsites:
                if isinstance(subsite, basestring):
                    temp.append((subsite,))
                elif isinstance(subsite, (tuple, list)):
                    temp.append(subsite)
            subsites = temp

        return self._fetch(subsites)

    def get_sensors(self, subsites=None, nodes=None):
        return self._fetch(self.get_nodes(subsites=subsites), nodes)

    def get_methods(self, subsites=None, nodes=None, sensors=None):
        return self._fetch(self.get_sensors(subsites, nodes), sensors)

    def get_streams(self, subsites=None, nodes=None, sensors=None, methods=None):
        streams = self._fetch(self.get_methods(subsites, nodes, sensors), methods)
        rval = []
        for subsite, node, sensor, method, stream in streams:
            start, stop = self.get_times(subsite, node, sensor, method, stream)
            if start and stop:
                rval.append(StreamInfo(subsite, node, sensor, method, stream, start, stop))
        return rval

    @lru_cache(20)
    def _get_metadata_times_dict(self, subsite, node, sensor):
        d = {}
        for each in requests.get(os.path.join(self.base_url, subsite, node, sensor, 'metadata', 'times')).json():
            d[(each.get('method'), each.get('stream'))] = each.get('beginTime'), each.get('endTime')
        return d

    def get_times(self, subsite, node, sensor, method, stream):
        time_dict = self._get_metadata_times_dict(subsite, node, sensor)
        rval = time_dict.get((method, stream), (0, 0))
        return rval


class Fetcher(object):
    QSTART = 'query_start'
    QFIN = 'query_finished'

    def __init__(self, stream, deployment=None, start=None, stop=None, limit=10):
        self.base_url = BASE_URL
        self.stream = stream
        self.request_id = None
        self.deployment = deployment
        self.start = start
        self.stop = stop
        self.limit = limit
        self.stats = {'stream': stream}

    def __hash__(self):
        return hash(self.stream)

    def make_query_url(self):
        url = os.path.join(self.base_url, 'inv', self.stream.subsite,
                           self.stream.node, self.stream.sensor,
                           self.stream.method, self.stream.stream)
        params = self._get_query_params()
        return '?'.join((url, params))

    def _get_query_params(self):
        start, stop = self.stream.start, self.stream.stop
        am = AssetManagement()
        if self.deployment is not None:
            deps = am.get_deployments(self.stream.subsite, self.stream.node, self.stream.sensor)
            log.debug('Found deployment data: %r', deps)
            if self.deployment in deps:
                depstart, depstop = deps[self.deployment]
                start = max(start, depstart)
                stop = min(stop, depstop)
                if depstart == start and depstop == stop:
                    log.info('Using deployment times %r %r', start, stop)
                else:
                    log.warn('Deployment times %r %r differ from available times, using %r %r',
                             depstart, depstop, start, stop)
        if self.start:
            start = max(start, self.start)
        if self.stop:
            stop = min(stop, self.stop)
        # log.error(am.get_calibrations(self.stream.subsite, self.stream.node, self.stream.sensor))

        return urllib.urlencode({'endDT': stop,
                                 'beginDT': start,
                                 'limit': self.limit})

    def query(self):
        qurl = self.make_query_url()
        log.debug('Starting query: %r', qurl)
        self.stats[self.QSTART] = time.time()
        response = requests.get(qurl)
        self.stats[self.QFIN] = time.time()
        try:
            response = response.json()
        except JSONDecodeError:
            log.error('Unable to decode response: %r', response.content)
            return []
        return response

    def __str__(self):
        query_time = self.stats.get(self.QFIN, time.time()) - self.stats.get(self.QSTART, 0)
        return '%-40s QT: %6.2f' % (self.stream.stream, query_time)

    def validate(self, parameters, fill):
        missing = None
        parameters = set(parameters)
        filled = set()
        particles = self.query()
        log.debug('Fetched %d particles', len(particles))
        if not isinstance(particles, list):
            log.error('Received non-particle result: %r', particles)
            return self.stream, 'FAIL', 'FAIL'
        for particle in particles:
            log.debug('particle: %r', particle)
            if isinstance(particle, dict):
                for p in parameters:
                    val = particle.get(p, None)
                    # log.debug('Checking parameter: %r %r %r', p, val, fill[p])
                    if val == fill[p]:
                        # log.debug('FILL VALUE DETECTED (%r) parameter: %r %r', self.stream, p, val)
                        filled.add((p, val))
                keys = set(particle.keys())
                missing = parameters - keys
                if missing:
                    break
            else:
                log.error('Received non-particle value: %r', particle)
        return self.stream, missing, filled


class AsyncFetcher(Fetcher):
    QREC = 'query_received'
    STATUS = 'job_status'

    def _get_query_params(self):
        return urllib.urlencode({'format': 'application/netcdf',
                                 'beginDT': self.stream.start,
                                 'endDT': self.stream.stop})

    def make_status_url(self):
        return os.path.join(self.base_url, 'async', 'status', 'm2m', self.request_id)

    def query(self):
        log.debug('Starting query: %r', self.stream)
        self.stats[self.QSTART] = time.time()
        response = requests.get(self.make_query_url()).json()
        self.stats[self.QREC] = time.time()
        self.request_id = response['requestUUID']

    def _get_status(self):
        try:
            response = requests.get(self.make_status_url())
            return response.json()
        except ConnectTimeout as e:
            log.exception('Exception getting status: %r', e)
            return {}

    def check_status(self):
        pending_keys = ['SCHEDULED', 'STARTED', 'NEW', 'PROCESSED']
        if self.request_id:
            status = self._get_status()
            if status:
                status = [s.get('status', {}) for s in status]
                counter = Counter(status)
                self.stats[self.STATUS] = counter
                if sum([counter.get(k, 0) for k in pending_keys]) == 0:
                    self.stats[self.QFIN] = time.time()
                    return True
        return False

    def __str__(self):
        response_time = self.stats.get(self.QREC, time.time()) - self.stats.get(self.QSTART, 0)
        query_time = self.stats.get(self.QFIN, time.time()) - self.stats.get(self.QSTART, 0)

        if self.QFIN in self.stats:
            state = 'COMPLETE'
        elif self.QREC in self.stats:
            state = 'IN PROGRESS'
        else:
            state = 'PENDING'
            response_time = query_time = 0
        return '%-40s %-12s RT: %6.2f QT: %6.2f STATUS: %s' % (self.stream.stream, state, response_time,
                                                               query_time, self.stats.get(self.STATUS))
