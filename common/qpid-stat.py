#!/usr/bin/env python
import math
from collections import deque

import time
import datetime
import os
import pickle
import requests


suffixes = (' ', 'k', 'm', 'g', 't', 'p', 'e', 'z', 'y')
queues = {}


def human_readable(number):
    if number <= 1000:
        return '%0.2f ' % number
    power = min(len(suffixes)-1, int(math.floor(math.log(number, 1000))))
    suffix = suffixes[power]
    reduced = float(number) / 1000**power
    return '%0.2f%s' % (reduced, suffix)


class QpidQueueStats(object):
    """
    [u'bindingCount', u'consumerCount', u'consumerCountWithCredit', u'persistentDequeuedBytes',
    u'persistentDequeuedMessages', u'persistentEnqueuedBytes', u'persistentEnqueuedMessages',
    u'queueDepthBytes', u'queueDepthMessages', u'totalDequeuedBytes', u'totalDequeuedMessages',
    u'totalEnqueuedBytes', u'totalEnqueuedMessages', u'unacknowledgedBytes', u'unacknowledgedMessages']
    """
    cols = {
        'bound': 'bindingCount',
        'consumers': 'consumerCount',
        'bytes': 'unacknowledgedBytes',
        'bytes_in': 'totalEnqueuedBytes',
        'bytes_out': 'totalDequeuedBytes',
        'msgs': 'unacknowledgedMessages',
        'msgs_in': 'totalEnqueuedMessages',
        'msgs_out': 'totalDequeuedMessages',
    }

    def __init__(self, deque_size=20):
        self.deques = {name: deque([], deque_size) for name in self.cols}
        self.times = deque([], deque_size)

    def update(self, stats_dict, data_time=None):
        if data_time is None:
            data_time = datetime.datetime.now()

        self.times.append(data_time)
        for col, field in self.cols.iteritems():
            self.deques[col].append(stats_dict[field])

    def _get_last_value(self, deque_name):
        return list(self.deques[deque_name])[-1]

    def _get_diff(self, deque_name):
        vals = list(self.deques[deque_name])
        if len(vals) < 2:
            return 0
        return vals[-1] - vals[0]

    def get_rate(self, deque_name):
        elapsed = self.elapsed
        if elapsed == 0:
            return 0.0
 
        diff = self._get_diff(deque_name)
        return float(diff) / elapsed

    @property
    def bound(self):
        return self._get_last_value('bound')

    @property
    def consumers(self):
        return self._get_last_value('consumers')

    @property
    def bytes(self):
        return self._get_last_value('bytes')

    @property
    def bytes_in(self):
        return self._get_last_value('bytes_in')

    @property
    def bytes_out(self):
        return self._get_last_value('bytes_out')

    @property
    def msgs(self):
        return self._get_last_value('msgs')

    @property
    def msgs_in(self):
        return self._get_last_value('msgs_in')

    @property
    def msgs_out(self):
        return self._get_last_value('msgs_out')

    @property
    def elapsed(self):
        times = list(self.times)
        if len(times) < 2:
            return 0
        return (times[-1] - times[0]).total_seconds()

    @property
    def bytes_in_rate(self):
        return self.get_rate('bytes_in')

    @property
    def msgs_in_rate(self):
        return self.get_rate('msgs_in')

    def __repr__(self):
        return str(self.__dict__)


class QpidQueue(object):
    # queueFlowResumeSizeBytes
    # exclusive
    # alertThresholdMessageAge
    # alertThresholdQueueDepthBytes
    # bindings
    # maximumDeliveryAttempts
    # statistics
    # queueFlowControlSizeBytes
    # consumers
    # type
    # durable
    # alertRepeatGap
    # alertThresholdMessageSize
    # state
    # alertThresholdQueueDepthMessages
    # lifetimePolicy
    # queueFlowStopped
    def __init__(self, rest_response):
        self.id = rest_response['id']
        self.name = rest_response['name']
        self.type = rest_response['type']
        self.exclusive = rest_response['exclusive']
        self.durable = rest_response['durable']
        self.stats = QpidQueueStats()

    def update(self, rest_response, data_time):
        self.stats.update(rest_response['statistics'], data_time)

    def _display_dict(self):
        return {
            'name': self.name,
            'durable': 'Y' if self.durable else 'N',
            'exclusive': 'Y' if self.exclusive else 'N',
            'msgs': human_readable(self.stats.msgs),
            'msgs_in': human_readable(self.stats.msgs_in),
            'msgs_out': human_readable(self.stats.msgs_out),
            'msgs_in_rate': human_readable(self.stats.msgs_in_rate),
            'bytes': human_readable(self.stats.bytes),
            'bytes_in': human_readable(self.stats.bytes_in),
            'bytes_out': human_readable(self.stats.bytes_out),
            'bytes_in_rate': human_readable(self.stats.bytes_in_rate),
            'consumers': self.stats.consumers,
            'bound': self.stats.bound,
        }

    def __repr__(self):
        return ('{name:40s} {durable:s} {exclusive:s} '
                '{msgs:>12s}{msgs_in:>12s}{msgs_out:>12s}{msgs_in_rate:>12s}'
                '{bytes:>12s}{bytes_in:>12s}{bytes_out:>12s}{bytes_in_rate:>12s}'
                '{consumers:4d} {bound:4d}'
                ).format(**self._display_dict())

    @staticmethod
    def header():
        return ('{name:40s} {durable:s} {exclusive:s} '
                '{msgs:>12s}{msgs_in:>12s}{msgs_out:>12s}{msgs_in_rate:>12s}'
                '{bytes:>12s}{bytes_in:>12s}{bytes_out:>12s}{bytes_in_rate:>12s}'
                '{consumers:>4s} {bound:>4s}'
                ).format(name='name', durable='d', exclusive='e', msgs='msgs', msgs_in='msgs_in', msgs_out='msgs_out',
                         msgs_in_rate='msg_rate', bytes='bytes', bytes_in='bytes_in', bytes_out='bytes_out',
                         bytes_in_rate='byte_rate', consumers='con', bound='bnd')


def parse_queue(queue_dict, data_time):
    name = queue_dict['name']
    if name not in queues:
        queues[name] = QpidQueue(queue_dict)

    queues[name].update(queue_dict, data_time)


def get_queues(base_url):
    while True:
        data = requests.get(os.path.join(base_url, 'queue')).json()
        # pickle.dump(data, open('qpid.data', 'w'))
        data_time = datetime.datetime.now()
        # data = pickle.load(open('qpid.data'))
        for each in data:
            parse_queue(each, data_time)

        print (chr(27) + "[2J")
        print data_time
        print QpidQueue.header()
        for each in sorted(queues):
            qq = queues[each]
            if qq.stats.bytes_in > 0:
                print qq

        print
        time.sleep(2)


#get_queues('http://ooiufs03.ooi.rutgers.edu:8180/rest')
get_queues('http://localhost:8180/rest')
