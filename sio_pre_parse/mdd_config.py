# mdd_config.py:
# system configuration for mdd system

import os

data_path = os.path.join(os.path.dirname(__file__), 'data')
dockserver_path = '/var/opt/gmc/gliders/'
host_name = 'test-dockserver.webbresearch.com/default'

def datafile(fn):
    return os.path.join(data_path, fn)

def from_glider(glider):
    return os.path.join(dockserver_path, glider, 'from-glider')

def to_glider(glider):
    return os.path.join(dockserver_path, glider, 'to-glider')
