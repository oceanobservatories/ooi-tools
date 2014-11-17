#!/usr/bin/env python

__author__ = 'Pete Cable, Dan Mergens'

import os
import sys

dataset_dir = os.path.dirname(os.path.realpath('__file__'))
tools_dir = os.path.dirname(dataset_dir)

sys.path.append(tools_dir)

import hotshot
import hotshot.stats
import json

# from common import logger
from common import edex_tools


def main():
    hostname = 'uf2.local'  # hostname = '10.5.48.180'
    print 'collecting available instruments from %s...' % hostname
    instruments = edex_tools.edex_get_instruments(hostname)

    for stream in instruments:
        for instrument in instruments[stream]:
            print 'calculating results for %s:%s...' % (stream, instrument)
            edex_tools.edex_mio_report(stream, instrument, edex_tools.edex_get_json(hostname, stream, instrument))
    print 'done'


def main_test():
    stream = 'cg_dcl_eng_dcl_superv_recovered'
    instrument = 'XX00XXXX-XX00X-00-CGDCLE100'
    edex_tools.edex_mio_report(stream, instrument, unit_test())


def unit_test(sample_data_file='mda_sample.json'):
    """
    Unit test statistics generator with saved sample file
    :return:  none
    """
    with open(sample_data_file, 'r') as f:
        read_data = json.load(f)
        return read_data


def profile():
    """
    Check performance by function to assess bottlenecks
    :return:  none
    """
    profile = hotshot.Profile('stats.profile')
    profile.runcall(main)
    stats = hotshot.stats.load('stats.profile')
    stats.strip_dirs()
    stats.sort_stats('time', 'calls')
    stats.print_stats()


# profile()
main()
