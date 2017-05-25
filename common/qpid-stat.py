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
        if len(vals) < 2:                                                                                                                                                                                                                                                                                     1,1           Top
