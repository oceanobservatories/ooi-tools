#!/usr/bin/env python
"""
Instrument Lock Utility

Usage:
  ilock.py [--host <host>] [--instruments] [--help] [--version]
  ilock.py [--host <host>] <instruments>...
  ilock.py [--host <host>] --lock [<instruments>...]
  ilock.py [--host <host>] --unlock [<instruments>...]

Options:
  --help            Show this screen.
  --version         Show version.
  -h --host         Hostname (default is localhost).
  -u --unlock       Unlock the instrument(s) (default is all instruments).
  -l --lock         Lock the instrument(s) (default is all instruments).
  -i --instruments  List all instrument reference designators.
"""

import signal
import requests

__author__ = 'Dan Mergens'


class LockClient(object):
    def __init__(self, lock_hostname='localhost', user='ilock.py'):
        self._hostname = lock_hostname
        self._user = user
        self._base_url = 'http://%s:12572/instrument/api'
        self._user_interrupt = False

    def get_refdes_list(self):
        url = self._base_url % self._hostname
        print 'url: %s' % url
        r = requests.get(url)
        return r.json()

    def locks(self):
        url = self._base_url % self._hostname + '/locks'
        print 'url: %s' % url
        r = requests.get(url)
        return r.json()

    def status(self, instrument_list, select_all_flag=False):
        r = []
        if select_all_flag:
            r = self.locks()
            d = r.get('locks')
            for key in sorted(d):
                print "%s: {u'locked-by': u'%s'}" % (key, d[key])
        else:
            for instrument in instrument_list:
                url = self._base_url % self._hostname + '/%s/lock' % instrument
                result = requests.get(url).json()
                print '%s: %s' % (instrument, result)
                r.append(result)
                if self._user_interrupt:
                    break
        return r

    def lock(self, instrument_list, unlock_flag=False):
        r = []
        endpoint = 'lock'
        if unlock_flag:
            endpoint = 'unlock'
        for instrument in instrument_list:
            url = self._base_url % hostname + '/%s/%s' % (instrument, endpoint)
            result = requests.post(url, data={'key': self._user}).json()
            if result:
                print '%s %sed - %s' % (instrument, endpoint, result)
            r.append(result)
            if self._user_interrupt:
                break
        return r

    def sigint_handler(self, signum, stack):
        print '<Interrupt>'
        self._user_interrupt = True


if __name__ == '__main__':
    import docopt

    options = docopt.docopt(__doc__, version='Instrument Lock Utility 1.0.0')
    hostname = options.get('<host>')
    instruments = options.get('<instruments>')

    if not hostname:
        hostname = 'localhost'

    client = LockClient(hostname)

    signal.signal(signal.SIGUSR1, client.sigint_handler)
    signal.signal(signal.SIGUSR2, client.sigint_handler)
    signal.signal(signal.SIGINT, client.sigint_handler)

    try:
        if options.get('--instruments'):
            print client.get_refdes_list()
        else:
            select_all = False
            if not instruments:
                instruments = client.get_refdes_list()
                select_all = True

            if options.get('--unlock') or options.get('--lock'):
                unlock = False
                state = 'Locking'
                if options.get('--unlock'):
                    unlock = True
                    state = 'Unlocking'
                if select_all:
                    print '%s all instruments:\n' % state
                else:
                   print '%s instrument(s):\n%s\n' % (state, instruments)
                client.lock(instruments, unlock)
            else:  # status only
                print 'Lock status:\n'
                client.status(instruments, select_all)

    except requests.ConnectionError as e:
        print e
        pass
