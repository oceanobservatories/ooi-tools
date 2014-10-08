#!/usr/bin/env python
"""Instrument Control

Usage:
  instrument_control.py <host> <name> [options]

Options:
  --module=<module>               The module containing the driver class   [default: mi.instrument.virtual.driver]
  --klass=<klass>                 The name of the class to be instantiated [default: InstrumentDriver]
  --command_port=<command_port>   The port to host the command interface   [default: 10000]
  --event_port=<event_port>       The port to host the event interface     [default: 10001]

"""
import threading
import requests
import docopt
import json
import time
import zmq

instrument_agent_port = 12572
base_api_url = 'instrument/api'


class Controller(object):
    def __init__(self, host, name, module, klass, command_port, event_port):
        self.host = host
        self.name = name
        self.module = module
        self.klass = klass
        self.command_port = int(command_port)
        self.event_port = int(event_port)
        self.base_url = 'http://%s:%d/%s/%s' % (self.host, instrument_agent_port, base_api_url, self.name)
        self.event_url = 'tcp://%s:%d' % (self.host, self.event_port)
        self.state = None
        self.keeprunning = True
        self.samples = []

    def start_driver(self):
        payload = {
            'host': self.host,
            'module': self.module,
            'class': self.klass,
            'commandPort': self.command_port,
            'eventPort': self.event_port
        }
        return requests.post(self.base_url, data=payload)

    def start_event_thread(self):
        context = zmq.Context()
        context.setsockopt(zmq.LINGER, 0)
        evt_socket = context.socket(zmq.SUB)

        evt_socket.connect(self.event_url)
        evt_socket.setsockopt(zmq.LINGER, 0)
        evt_socket.setsockopt(zmq.SUBSCRIBE, '')

        def loop():

            while self.keeprunning:
                try:
                    evt = evt_socket.recv_json(flags=zmq.NOBLOCK)
                    if evt.get('type') == 'DRIVER_ASYNC_EVENT_SAMPLE':
                        sample = json.loads(evt.get('value'))
                        if sample.get('stream_name') != 'raw':
                            self.samples.append(sample)
                except zmq.ZMQError as e:
                    time.sleep(.1)

        threading.Thread(target=loop).start()

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
        return requests.post(self.base_url + '/discover')

    def set_resource(self, **kwargs):
        return requests.post(self.base_url + '/resource', data={'resource': json.dumps(kwargs)})

    def get_state(self):
        r = requests.get(self.base_url)
        self.state = r.json()['value']
        return r

    def execute(self, command):
        return requests.post(self.base_url + '/execute', data={'command': json.dumps(command)})

    def initialize_driver(self, target_state, port_config, init_config, timeout=60):
        self.start_driver()
        self.start_event_thread()
        self.get_state()
        end_time = time.time() + timeout

        while self.state['state'] != target_state:
            if self.state['state'] == 'DRIVER_STATE_UNCONFIGURED':
                self.configure(port_config)
                self.set_init_params(init_config)
            elif self.state['state'] == 'DRIVER_STATE_DISCONNECTED':
                self.connect()
            elif self.state['state'] == 'DRIVER_STATE_UNKNOWN':
                self.discover()
            elif self.state['state'] == 'DRIVER_STATE_COMMAND':
                if target_state == 'DRIVER_STATE_AUTOSAMPLE':
                    self.execute('DRIVER_EVENT_START_AUTOSAMPLE')
            elif self.state['state'] == 'DRIVER_STATE_AUTOSAMPLE':
                self.execute('DRIVER_EVENT_STOP_AUTOSAMPLE')
            self.get_state()

            if time.time() > end_time:
                raise Exception('Timed out transitioning to target state: %s' % target_state)

    def run_script(self, script):
        self.samples = []
        try:
            for command, args in script:
                if command == 'sleep':
                    time.sleep(args)
                elif hasattr(self, command):
                    reply = getattr(self, command)(args)
                    try:
                        reply = json.loads(reply)
                    except:
                        pass
                    if isinstance(reply, dict):
                        if reply.get('type') == 'DRIVER_EXCEPTION_EVENT':
                            raise Exception('Exception from driver: %s' % reply['value'])

            self.stop_driver()
        finally:
            self.keeprunning = False

def main():
    options = docopt.docopt(__doc__)
    c = Controller(options['<host>'],
                   options['<name>'],
                   options['--module'],
                   options['--klass'],
                   options['--command_port'],
                   options['--event_port'])

if __name__ == '__main__':
    main()
