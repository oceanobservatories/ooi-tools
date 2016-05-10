#!/user/bin/env python
"""
Instrument Lock Utility

Usage:
  ilock.py
  ilock.py <host>
  ilock.py <host> <instruments>...
  ilock.py <host> (-u | --unlock)
  ilock.py <host> (-l | --lock)
  ilock.py <host> (-u | --unlock) <instruments>...
  ilock.py <host> (-l | --lock) <instruments>...
  ilock.py <host> (-i | --instruments)

Options:
  -h --help         Show this screen.
  --version         Show version.
  -u --unlock       Unlock the instrument(s)
  -l --locks        List current locks
  -i --instruments  List all instrument reference designators
"""

import docopt
import requests

BASE_INSTRUMENT_URL = 'http://%s:12572/instrument/api'
TIMEOUT = 60
user = 'ilock.py'

__author__ = 'Dan Mergens'

def get_refdes_list(hostname):
    url = BASE_INSTRUMENT_URL % hostname
    print 'url: %s' % url
    r = requests.get(url)
    return r.json()

def locks(hostname):
    url = BASE_INSTRUMENT_URL % hostname + '/locks'
    print 'url: %s' % url
    r = requests.get(url)
    return r.json()

def status(hostname, instruments):
    r = []
    for instrument in instruments:
        url = BASE_INSTRUMENT_URL % hostname + '/%s/lock' % instrument
        result = requests.get(url).json()
        print '%s: %s' % (instrument, result)
        r.append(result)
    return r

def lock(hostname, instruments, unlock=False):
    r = []
    endpoint = 'lock'
    if unlock:
        endpoint = 'unlock'
    for instrument in instruments:
        url = BASE_INSTRUMENT_URL % hostname + '/%s/%s' % (instrument, endpoint)
        result = requests.post(url, data={'key': user}).json()
        if result:
            print '%s %sed - %s' % (instrument, endpoint, result)
        r.append(result)
    return r

if __name__ == '__main__':
    options = docopt.docopt(__doc__, version='Instrument Lock Utility 1.0.0')
    hostname = options.get('<host>')
    instruments = options.get('<instruments>')

    if not hostname:
        hostname = 'localhost'

    if options.get('--locks'):
        print locks(hostname)
    elif options.get('--instruments'):
        print get_refdes_list(hostname)
    else:
        all = False
        if not instruments:
            instruments = get_refdes_list(hostname)
            all = True

        if options.get('--unlock') or options.get('--lock'):
            unlock = False
            state = 'Locking'
            if options.get('--unlock'):
                unlock = True
                state = 'Unlocking'
            if all:
                print '%s all instruments:\n' % state
            else:
               print '%s instrument(s):\n%s\n' % (state, instruments)
            lock(hostname, instruments, unlock)
        else:
            print 'Lock status:\n'
            status(hostname, instruments)
