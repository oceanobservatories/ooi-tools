#!/usr/bin/env python
"""
Parameter Extraction Utility

Usage:
  iparam.py [--host <host>] [--instruments] [--help] [--version]
  iparam.py [--host <host>] <instruments>...
  iparam.py [--host <host>] [--default-params] [--init-params] [--startup]
  iparam.py [--host <host>] [--default-params] [--init-params] [--startup] <instruments>...

Options:
  --help            Show this screen.
  --version         Show version.
  -h --host         Hostname (default is localhost).
  -i --instruments  List all instrument reference designators.
  --init-params     List the startup parameters from associate instruments
  --default-params  List the default parameters from associate instruments
"""
import codecs
from multiprocessing.pool import ThreadPool

import requests
import datetime
import yaml

__author__ = 'Dan Mergens'


class InstrumentAPIClient(object):
    def __init__(self, lock_hostname='localhost', user='ilock.py'):
        self._hostname = lock_hostname
        self._user = user
        self._base_url = 'http://%s:12572/instrument/api'

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
        return r

    def list_params(self, instrument_list, show_startup_params=True, show_default_params=True):
        for instrument in instrument_list:
            url = self._base_url % hostname + '/%s' % instrument
            print '%s - (%s)' % (instrument, url)
            result = requests.get(url).json()
            if result:
                if show_startup_params:
                    init_params = result.get('value').get('init_params')
                    print '%r' % init_params
                if show_default_params:
                    parameters = None
                    value = result.get('value')
                    if value:
                        metadata = value.get('metadata')
                        if metadata:
                            parameters = metadata.get('parameters')
                    if not parameters:
                        continue
                    for key in parameters:
                        startup = parameters[key].get('startup')
                        if startup:
                            startup = 'Y'
                        else:
                            startup = 'N'
                        default = parameters[key].get('value').get('default')
                        print '%s %s %s' % (key, startup, default)

    def get_startup_params(self, instrument):
        params = {}
        url = self._base_url % self._hostname + '/%s' % instrument
        result = requests.get(url).json()
        if result:
            init_params = result.get('value').get('init_params')
            parameters = None
            value = result.get('value')
            if value:
                metadata = value.get('metadata')
                if metadata:
                    parameters = metadata.get('parameters')
            if parameters:
                for key in parameters:

                    startup = parameters[key].get('startup')
                    visibility = parameters[key].get('visibility')
                    if visibility == 'READ_ONLY':
                        continue
                    if visibility == 'READ_WRITE' and startup:  # we only care about ones that need to be set
                        visibility = 'IMMUTABLE'

                    default = parameters[key].get('value').get('default')
                    display_name = parameters[key].get('display_name')
                    range = parameters[key].get('range', '')
                    units = parameters[key].get('value').get('units', '')
                    params.setdefault(visibility, {})[key] = {
                        'default': default, 'display_name': display_name, 'range': range, 'units': units}
        return params

    def write_startup_configs(self, instrument_list):
        pool = ThreadPool(10)
        futures = []

        for instrument in instrument_list:
            futures.append(pool.apply_async(self.write_startup_config, (instrument,)))

        for future in futures:
            future.get()

    @staticmethod
    def param_to_string(param, param_dict):
        key = str(param)
        value = param_dict['default']
        if isinstance(value, unicode):
            value = str(value)
        param_string = yaml.dump({key: value}, default_flow_style=False).rstrip()
        units = param_dict['units']
        units = units if units is not None else ''
        return '  %-40s # %s (%s [%s]) %s\n' % (param_string,
                                               param_dict['display_name'],
                                               param_dict['range'],
                                               param_dict['default'],
                                               param_dict['units'])

    def write_startup_config(self, instrument):
        file = '%s.yaml' % instrument
        date = datetime.date.today()
        header = \
"""
# %s startup configuration file
#
# Version   Date         User    Description
# ------------------------------------------------------------------------------
#    0-01   2016-05-16   djm     Default parameters query
#    0-02   %10s   djm     Added additional parameter information
#

parameters:
""" % (instrument, date)
        params = self.get_startup_params(instrument)
        with codecs.open(file, 'w', 'utf-8') as f:
            f.write(header)
            startup = params.get('IMMUTABLE')
            if startup:
                f.write('\n### Startup parameters ###\n\n')
                for param in sorted(startup):
                    f.write(self.param_to_string(param, startup[param]))
            read_write = params.get('READ_WRITE')
            if read_write:  # comment out by default - user can override by uncommenting
                f.write('\n### Read/Write parameters ###\n\n')
                for param in sorted(read_write):
                    f.write('#' + self.param_to_string(param, read_write[param]))

        print 'wrote %s' % file

if __name__ == '__main__':
    import docopt

    options = docopt.docopt(__doc__, version='Instrument Lock Utility 1.0.0')
    hostname = options.get('<host>')
    select_all = False
    instruments = options.get('<instruments>')
    init_params = options.get('--init-params')
    default_params = options.get('--default-params')

    if not hostname:
        hostname = 'localhost'

    client = InstrumentAPIClient(hostname)

    if not instruments:
        instruments = client.get_refdes_list()
        select_all = True

    try:
        if options.get('--instruments'):
            print instruments
        elif init_params or default_params:
            client.list_params(instruments, show_startup_params=init_params, show_default_params=default_params)
        elif options.get('--startup'):
            client.write_startup_configs(instruments)
        else:
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
    except (KeyboardInterrupt, SystemExit):
        pass
