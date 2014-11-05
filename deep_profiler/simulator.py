#!/usr/bin/env python
import json
import time
import zmq

__author__ = 'pcable'

TEST_DATA = [
    {'name': 'optode', 'data': {'doconcs': 37.921, 't': 7.818}},
    {'name': 'flntu', 'data': {'chlaflo': 85, 'ntuflo': 247}},
    {'name': 'flcd', 'data': {'cdomflo': 168}},
    # {'name': 'mmp', 'data':
    #     {'current': 0.0,
    #      'mode': 'down',
    #      'pnum': 6400,
    #      'pressure': 14.3,
    #      'vbatt': 12.1}},
    {'name': 'acm',
     'data': {'hx': 0.3263,
              'hy': 0.1313,
              'hz': -0.9361,
              'tx': -1.52,
              'ty': 0.31,
              'va': 4.11,
              'vb': -3.28,
              'vc': -0.79,
              'vd': -0.89}},
    {'name': 'ctd', 'data': {'condwat': 32.2808, 'preswat': 14.28, 'tempwat': 7.8077}}]

context = zmq.Context()
sock = context.socket(zmq.PUB)
sock.bind('tcp://0.0.0.0:5510')

while True:
    now = time.time()
    secs = int(now)
    usecs = int((now - secs) * 1e6)
    for record in TEST_DATA:
        record['t'] = [secs, usecs]
        sock.send_multipart(['DATA', json.dumps(record)])
        time.sleep(1)