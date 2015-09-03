#!/usr/bin/env python
"""Instrument Control

Usage:
  instrument_control.py <host>
  instrument_control.py <host> <name> start [options]
  instrument_control.py <host> <name> stop
  instrument_control.py <host> <name> connect
  instrument_control.py <host> <name> discover
  instrument_control.py <host> <name> state
  instrument_control.py <host> <name> configure <config_file>
  instrument_control.py <host> <name> execute <capability>

Options:
  --driver_host=<driver_host>     The host running the driver              [default: localhost]
  --module=<module>               The module containing the driver class   [default: mi.instrument.virtual.driver]
  --klass=<klass>                 The name of the class to be instantiated [default: InstrumentDriver]
  --command_port=<command_port>   The port to host the command interface   [default: 10000]
  --event_port=<event_port>       The port to host the event interface     [default: 10001]

"""
import os
import pprint
import sys

instrument_dir = os.path.dirname(os.path.realpath('__file__'))
tools_dir = os.path.dirname(instrument_dir)

sys.path.append(tools_dir)

import threading
import requests
import docopt
import json
import time
import yaml
import zmq

from common import logger

instrument_agent_port = 12572
base_api_url = 'instrument/api'

log_dir = os.path.join(instrument_dir, 'output_%s' % time.strftime('%Y%m%d-%H%M%S'))
log = logger.get_logger(file_output=os.path.join(log_dir, 'instrument_control.log'))


def flatten(particle):
    try:
        for each in particle.get('values'):
            id = each.get('value_id')
            val = each.get('value')
            particle[id] = val
        del (particle['values'])
    except:
        log.error('Exception flattening particle: %s', particle)
    return particle


def get_running(host):
    r = requests.get('http://%s:%d/%s' % (host, instrument_agent_port, base_api_url))
    return r.json()


class Controller(object):
    def __init__(self, host, name):
        self.host = host
        self.name = name
        self.base_url = 'http://%s:%d/%s/%s' % (self.host, instrument_agent_port, base_api_url, self.name)
        self.state = None
        self.keeprunning = True
        self.samples = {}

    def start_state_thread(self):
        def loop():
            self.get_state()
            while self.keeprunning:
                try:
                    r = self.get_state()
                    log.info('State updated: %s', r['value']['state'])
                except Exception as e:
                    log.info('Exception: %s', e)
                time.sleep(2)

        t = threading.Thread(target=loop)
        t.setDaemon(True)
        t.start()

    def stop_driver(self):
        self.keeprunning = False
        return requests.delete(self.base_url)

    def configure(self, config=None):
        return requests.post(self.base_url + '/configure', data={'config': json.dumps(config)})

    def set_init_params(self, config=None):
        return requests.post(self.base_url + '/initparams', data={'config': json.dumps(config)})

    def connect(self):
        return requests.post(self.base_url + '/connect')

    def discover(self):
        return requests.post(self.base_url + '/discover', data={'timeout': 300000})

    def set_resource(self, **kwargs):
        return requests.post(self.base_url + '/resource', data={'resource': json.dumps(kwargs)})

    def get_state(self):
        r = requests.get(self.base_url)
        reply = r.json()
        self.state = reply['value']
        return reply

    def execute(self, command):
        return requests.post(self.base_url + '/execute', data={'command': json.dumps(command)})

    def initialize_driver(self, target_state, init_config, timeout=300):
        self.get_state()
        end_time = time.time() + timeout

        while self.state['state'] != target_state:
            if self.state['state'] == 'DRIVER_STATE_UNCONFIGURED':
                log.info('Configuring driver: %r', init_config)
                self.set_init_params(init_config)
            elif self.state['state'] == 'DRIVER_STATE_DISCONNECTED':
                log.info('Connecting to instrument')
                self.connect()
            elif self.state['state'] == 'DRIVER_STATE_UNKNOWN':
                log.info('Calling discover')
                self.set_init_params(init_config)
                self.discover()
            elif self.state['state'] == 'DRIVER_STATE_COMMAND':
                if target_state == 'DRIVER_STATE_AUTOSAMPLE':
                    self.execute('DRIVER_EVENT_START_AUTOSAMPLE')
            elif self.state['state'] == 'DRIVER_STATE_AUTOSAMPLE':
                self.execute('DRIVER_EVENT_STOP_AUTOSAMPLE')
            self.get_state()
            time.sleep(1)

            if time.time() > end_time:
                raise Exception('Timed out transitioning to target state: %s' % target_state)

    def wait_state(self, state, length):
        log.info('Enter wait_state: %s, %s', state, length)
        end_time = time.time() + length
        while time.time() < end_time:
            if self.state['state'] == state:
                log.info('Target state found')
                return
        raise Exception('Timed out waiting for state: %s' % state)

    def run_script(self, script, terminate=True):
        self.samples = {}
        try:
            for command, args in script:
                if command == 'sleep':
                    log.info('Sleep %d seconds', args)
                    time.sleep(args)
                elif command == 'set_resource':
                    log.info('sending set_resource with args: %r', args)
                    self.set_resource(**args)
                elif command == 'wait_state':
                    state, length = args
                    self.wait_state(state, length)
                elif hasattr(self, command):
                    log.info('Sending command: %r with args: %r', command, args)
                    reply = getattr(self, command)(args)
                    try:
                        reply = json.loads(reply)
                    except:
                        pass
                    if isinstance(reply, dict):
                        if reply.get('type') == 'DRIVER_EXCEPTION_EVENT':
                            raise Exception('Exception from driver: %s' % reply['value'])

        finally:
            if terminate:
                self.stop_driver()

def main():
    options = docopt.docopt(__doc__)

    if options['<name>'] is None:
        # return a list of running agents
        print '\n'.join(get_running(options['<host>']))
        sys.exit()

    c = Controller(options['<host>'],
                   options['<name>'])

    if options['stop']:
        c.stop_driver()
    elif options['configure']:
        config = yaml.load(open(options['<config_file>']))
        c.configure(config['port_agent_config'])
        c.set_init_params(config['startup_config'])
    elif options['connect']:
        c.connect()
    elif options['discover']:
        c.discover()
    elif options['execute']:
        c.execute(options['<capability>'])
    elif options['state']:
        pprint.pprint(c.get_state())

if __name__ == '__main__':
    c = main()
