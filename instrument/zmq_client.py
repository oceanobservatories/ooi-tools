#!/usr/bin/env python

"""
lifted from ion.agent.instrument.driver_client, modified to remove ion dependencies
"""

import time
import zmq
import logging


__author__ = 'Edward Hunter'

DEFAULT_TIMEOUT = 10


def get_logger():
    logger = logging.getLogger('zmq_client')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger

log = get_logger()

class ZmqDriverClient(object):
    """
    A class for communicating with a ZMQ-based driver process using python
    thread for catching asynchronous driver events.
    """

    def __init__(self, host, cmd_port):
        """
        Initialize members.
        @param host Host string address of the driver process.
        @param cmd_port Port number for the driver process command port.
        @param event_port Port number for the driver process event port.
        """
        self.host = host
        self.cmd_port = cmd_port

        self.context = zmq.Context()
        self.zmq_cmd_socket = None

        self._stopped = False

    def _connect_driver(self):
        cmd_host_string = 'tcp://%s:%i' % (self.host, self.cmd_port)

        self.zmq_cmd_socket = self.context.socket(zmq.REQ)
        self.zmq_evt_socket = self.context.socket(zmq.SUB)

        self.zmq_cmd_socket.connect(cmd_host_string)
        log.info('Driver client cmd socket connected to %s.', cmd_host_string)

    def start_messaging(self, callback=None):
        """
        Initialize and start messaging resources for the driver process client.
        Initializes command socket for sending requests,
        and starts event thread that listens for events from the driver
        process independently of command request-reply.
        """
        self._connect_driver()

    def stop_messaging(self):
        self._stopped = True
        log.info('Driver client messaging closed.')

    def _send(self, message, timeout=DEFAULT_TIMEOUT):
        start_send = time.time()
        while time.time() < start_send+timeout:
            try:
                # Attempt command send. Retry if necessary.
                return self.zmq_cmd_socket.send_json(message, flags=zmq.NOBLOCK)

            except zmq.ZMQError:
                # Socket not ready to accept send. Sleep and retry later.
                time.sleep(.0001)

            except Exception,e:
                log.error('Driver client error writing to zmq socket: ' + str(e))
                raise SystemError('exception writing to zmq socket: ' + str(e))
        raise Exception('Unable to send command within timeout')

    def _receive(self, timeout):
        start_reply = time.time()
        while time.time() < start_reply+timeout:
            try:
                # Attempt reply recv. Retry if necessary.
                return self.zmq_cmd_socket.recv_json(flags=zmq.NOBLOCK)

            except zmq.ZMQError:
                # Socket not ready with the reply. Sleep and retry later.
                time.sleep(.0001)

            except Exception,e:
                log.error('Driver client error reading from zmq socket: ' + str(e))
                raise SystemError('exception reading from zmq socket: ' + str(e))
        raise Exception('Unable to receive reply within timeout')

    def _command(self, cmd, *args, **kwargs):
        try:
            # Package command dictionary.
            timeout = kwargs.pop('driver_timeout', 5)
            msg = {'cmd':cmd,'args':args,'kwargs':kwargs}

            log.debug('Sending command %s.' % str(msg))
            self._send(msg, timeout)

            log.debug('Awaiting reply.')

            reply = self._receive(timeout)
            log.debug('Reply: %r', reply)

            ## exception information is returned as a tuple (code, message, stacks)
            if isinstance(reply, tuple) and len(reply)==3:
                log.error('Proceeding to raise exception with these args: ' + str(reply))
            else:
                return reply
        except:
            self._connect_driver()

    def ping(self, *args, **kwargs):
        return self._command('process_echo', *args, **kwargs)

    def configure(self, *args, **kwargs):
        return self._command('configure', *args, **kwargs)

    def connect(self, *args, **kwargs):
        return self._command('connect', *args, **kwargs)

    def discover(self, *args, **kwargs):
        return self._command('discover_state', *args, **kwargs)

    def execute(self, *args, **kwargs):
        return self._command('execute_resource', *args, **kwargs)

    def shutdown(self, *args, **kwargs):
        return self._command('stop_driver_process', *args, **kwargs)

    def get_state(self, *args, **kwargs):
        return self._command('get_resource_state', *args, **kwargs)

    def set_init_params(self, *args, **kwargs):
        return self._command('get_resource_state', *args, **kwargs)

