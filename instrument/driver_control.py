#!/usr/bin/env python
"""
Usage:
    driver_control.py <config>
    driver_control.py <refdes>
"""

import time
import logging
import yaml
from zmq_client import ZmqDriverClient
from IPython import embed
import docopt
import consulate

SLEEPTIME = .5


def get_logger():
    logger = logging.getLogger('driver_control')
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

def s():
    time.sleep(.001)

def main():
    options = docopt.docopt(__doc__)
    refdes = options['<refdes>']
    config = options['<config>']
    if config is not None:
        config = yaml.load(open(config))
        refdes = config['refdes']

    z = ZmqDriverClient(refdes)
    z.start_messaging(callback)
    z.ping()
    embed()

    if z is None:
        print 'not found'


def callback(data):
    log.debug('DATA: %s', data)

if __name__ == '__main__':
    main()
