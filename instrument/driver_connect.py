#!/usr/bin/env python
"""
Usage:
    driver_connect.py <refdes> <config>
"""

import time
import logging
import yaml
from zmq_client import ZmqDriverClient
import docopt

SLEEPTIME = .5

def get_logger():
    logger = logging.getLogger('driver_connect')
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

def sleep(seconds):
    time.sleep(seconds)

def main():
    options = docopt.docopt(__doc__)
    refdes = options['<refdes>']
    config = options['<config>']
    config = yaml.load(open(config))

    z = ZmqDriverClient(refdes)
    if z is None:
        print 'Error: Instrument Not Found'
    else:
        z.start_messaging(callback)
        z.ping()

        z.configure(config['port_agent_config'])
        sleep(SLEEPTIME)
        z.connect()

def callback(data):
    log.debug('DATA: %s', data)

if __name__ == '__main__':
    main()
