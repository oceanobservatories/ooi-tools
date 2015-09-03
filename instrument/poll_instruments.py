#!/usr/bin/env python
"""
Usage:
    poll_instruments.py <host>
    poll_instruments.py <host> <instrument>...
"""
from simplejson import JSONDecodeError

__author__ = 'petercable'

import requests
import docopt
from multiprocessing.pool import ThreadPool

pool = ThreadPool(16)

def get_base_url(host):
    return 'http://%s:12572/instrument/api' % host

def get_instruments(host):
    url = get_base_url(host)
    response = requests.get(url, timeout=2)
    return response.json()

def get_state(host, instrument):

    try:
        url = get_base_url(host) + '/' + instrument
        response = requests.get(url, timeout=4)
        return response.json()
    except Exception:
        return {}

def poll(host, instruments):
    if not instruments:
        instruments = get_instruments(host)
    futures = []
    results = {}

    for instrument in instruments:
        futures.append((instrument, pool.apply_async(get_state, (host, instrument))))

    for instrument, future in futures:
        subsite, node, sensor = instrument.split('-', 2)
        result = future.get().get('value', {}).get('state', 'NO RESPONSE')
        results.setdefault(subsite, {}).setdefault(node, {})[sensor] = result

    for subsite in sorted(results.keys()):
        print
        for node in sorted(results[subsite].keys()):
            for sensor in sorted(results[subsite][node].keys()):
                print '\t' + '-'.join((subsite, node, sensor)), results[subsite][node][sensor]

def main():
    options = docopt.docopt(__doc__)
    host = options['<host>']
    instruments = options['<instrument>']
    poll(host, instruments)

if __name__ == '__main__':
    main()
