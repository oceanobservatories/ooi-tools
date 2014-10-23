#!/usr/bin/env python

import os
import sqlite3
import logging
import sys
from parse_preload import create_db, load_paramdicts, load_paramdefs
from xml.etree.ElementTree import Element, SubElement, tostring
from xml.dom import minidom

dbfile = 'preload.db'

streams_template = '''<?xml version="1.0" encoding="UTF-8"?>
<streamDefinitions>
%s
</streamDefinitions>'''

stream_template = '''  <streamDefinition streamName="%s">
%s
  </streamDefinition>'''

stream_param_template = '''    <parameterId>%s</parameterId> <!-- %s -->'''


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


def massage_value(x):
    if x is None:
        return ''
    return unicode(x)


def streams_to_xml(stream_dict, param_dict, outputfile):
    rendered_streams = []
    for stream in stream_dict.itervalues():
        rendered_params = []
        params = stream.parameter_ids
        if params is None: continue
        params = params.split(',')
        for param_id in params:
            param = param_dict.get(param_id.strip())
            if param is None:
                rendered_params.append(stream_param_template % (param_id, "NOT FOUND"))
            else:
                rendered_params.append(stream_param_template % (param.id, param.name))
        rendered_streams.append(stream_template % (stream.name, '\n'.join(rendered_params)))
    output = streams_template % '\n'.join(rendered_streams)
    outputfile.write(output)


# 'id, scenario, hid, parameter_type, value_encoding, units, display_name, precision, '
# 'parameter_function_id, parameter_function_map, dpi'
def params_to_xml(param_dict, outputfile):
    root = Element('parameterContainer')
    for param in param_dict.itervalues():
        SubElement(root, 'parameter',
                   attrib={'pd_id': massage_value(param.id),
                           'name': massage_value(param.name),
                           'type': massage_value(param.parameter_type),
                           'unit': massage_value(param.units),
                           'fill': massage_value(param.fill_value),
                           'encoding': massage_value(param.value_encoding),
                           'precision': massage_value(param.precision)}
        )
    outputfile.write(
        minidom.parseString(tostring(root, encoding='UTF-8')).toprettyxml(encoding='UTF-8'))

def main():
    if not os.path.exists(dbfile):
        conn = sqlite3.connect(dbfile)
        create_db(conn)

    conn = sqlite3.connect(dbfile)
    stream_dict = load_paramdicts(conn)[1]
    param_dict = load_paramdefs(conn)
    streams_to_xml(stream_dict, param_dict, open('streams.xml', 'w'))
    params_to_xml(param_dict, open('params.xml', 'w'))


main()

