#!/usr/bin/env python
"""
    Usage:
        ./parse_preload.py [--rebuild] [--key=key]
"""
from collections import namedtuple, Counter
import json

import os
import sqlite3
import logging
import gdata.spreadsheet.service
import docopt

__author__ = 'pcable'

key = '1jIiBKpVRBMU5Hb1DJqyR16XCkiX-CuTsn1Z1VnlRV4I'

IA_SELECT = """
SELECT id, scenario, iadriveruri, iadrivermodule, iadriverclass, streamconfigurations, agentdefaultconfig
FROM instrumentagent
WHERE id like 'IA%%'
"""

STREAM_SELECT = """
SELECT id, scenario, cfgstreamtype, cfgstreamname, cfgparameterdictionaryname
FROM streamconfiguration
WHERE id like 'SC%'
"""

PARAMDICT_SELECT = """
SELECT id, scenario, name, parameterids, temporalparameter
FROM parameterdictionary
WHERE id like 'DICT%'
"""

PARAMDEF_SELECT = """
SELECT id, scenario, name, hid, parametertype, valueencoding, unitofmeasure, fillvalue, displayname,
        precision, parameterfunctionid, parameterfunctionmap, dataproductidentifier
FROM parameterdefs
WHERE id like 'PD%'
"""

temp = 'temp.xlsx'
dbfile = 'preload.db'

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

def sheet_generator():
    client = gdata.spreadsheet.service.SpreadsheetsService()
    sheets = {}
    for sheet in client.GetWorksheetsFeed(key, visibility='public', projection='basic').entry:
        title = sheet.title.text
        id = sheet.id.text.split('/')[-1]

        log.debug('Fetching sheet %s from googles', title)
        rows = []
        for x in client.GetListFeed(key, id, visibility='public', projection='values').entry:
           rows.append({k:v.text for k,v in x.custom.items()})
        yield title, rows

def get_parameters(param_list, param_dict):
    params = {}
    for param in param_list:
        param = param_dict.get(param)
        if param is None: return
        params[param['Name']] = param
    return params

def sanitize_for_sql(row):
    subs = {
        ' ': '_',
        '-': '_',
        '/': '_',
        '(': '',
        ')': '',
    }
    new_row = []
    for val in row:
        for x,y in subs.iteritems():
            val = val.replace(x,y)
        new_row.append(val)
    return new_row

def sanitize_names(name):
    subs = {
        'Constraint': 'Constraints',
    }
    return subs.get(name, name)

def create_table(conn, name, row):
    row = sanitize_for_sql(row)
    log.debug('CREATE TABLE: %s %r', name, row)
    c = conn.cursor()
    try:
        c.execute('DROP TABLE %s' % name)
    except:
        pass
    c.execute('CREATE TABLE %s (%s)' % (name, ', '.join(row)))
    conn.commit()

def populate_table(conn, name, rows):
    log.debug('POPULATE TABLE: %s NUM ROWS: %d', name, len(rows))
    keys = rows[0].keys()
    values = [[r[k] for k in keys] for r in rows]
    c = conn.cursor()
    c.executemany('INSERT INTO %s (%s) VALUES (%s)' % (name, ','.join(keys), ','.join(['?']*len(keys))), values)
    conn.commit()

def create_db(conn):
    for name, sheet in sheet_generator():
        log.debug('Creating table: %s', name)
        name = sanitize_names(name)
        create_table(conn, name, sheet[0].keys())
        populate_table(conn, name, sheet[1:])

def test_param_function_map(conn):
    c = conn.cursor()
    c.execute("select id,parameterfunctionmap from parameterdefs where parameterfunctionmap not like ''")
    for row in c:
        try:
            if row[0].startswith('PD'):
                obj = eval(row[1])
                json_string = json.dumps(obj)
                # log.error('PARSED %s %s %r %r %r', str(row[1]) == json_string, row[0], row[1], obj, json_string)
                # c.execute("update parameterdefs set parameter_function_map=%s where id='%r'" % (json_string, row[0]))
        except Exception as e:
            log.error('ERROR PARSING %s %r %s', row[0], row[1], e)

ParameterDef = namedtuple('ParameterDef',
                          'id, scenario, name, hid, parameter_type, value_encoding, units, fill_value, '
                          'display_name, precision, parameter_function_id, parameter_function_map, dpi')
# CREATE TABLE ParameterDefs (Scenario, confluence, Name, ID, HID, HID_Conflict, Parameter_Type, Value_Encoding,
# Code_Set, Unit_of_Measure, Fill_Value, Display_Name, Precision, visible, Parameter_Function_ID,
# Parameter_Function_Map, Lookup_Value, QC_Functions, Standard_Name, Data_Product_Identifier, Reference_URLS,
# Description, Review_Status, Review_Comment, Long_Name, SKIP);
def load_paramdefs(conn):
    log.debug('Loading Parameter Definitions')
    c = conn.cursor()
    c.execute(PARAMDEF_SELECT)
    params = map(ParameterDef._make, c.fetchall())
    param_dict = {x.id:x for x in params}
    check_for_dupes(params, "id")
    check_for_dupes(params, "hid")
    return param_dict

ParameterDictionary = namedtuple('ParameterDictionary', 'id, scenario, name, parameter_ids, temporal_parameter')
# CREATE TABLE ParameterDictionary (Scenario, ID, confluence, name, parameter_ids,
# temporal_parameter, parameters, Review_Status, SKIP);
def load_paramdicts(conn):
    log.debug('Loading Parameter Dictionary')
    c = conn.cursor()
    c.execute(PARAMDICT_SELECT)
    params = map(ParameterDictionary._make, c.fetchall())
    param_dicts_by_id = {p.id:p for p in params}
    param_dicts_by_name = {p.name:p for p in params}
    check_for_dupes(params, 'id')
    check_for_dupes(params, 'name')

    return param_dicts_by_id, param_dicts_by_name

StreamConfig = namedtuple('StreamConfig', 'id, scenario, stream_type, stream_name, dict_name')
# CREATE TABLE StreamConfiguration (Scenario, COMMENT, ID, cfg_stream_type,
# cfg_stream_name, cfg_parameter_dictionary_name, attr_display_name, comment2);
def load_streams(conn):
    log.debug('Loading Stream Configurations')
    c = conn.cursor()
    c.execute(STREAM_SELECT)
    streams = map(StreamConfig._make, c.fetchall())
    stream_dict = {stream.id:stream for stream in streams}
    check_for_dupes(streams, 'id')
    return stream_dict

def check_streams(agent, streams, dicts, defs):
    stream_names = []
    for stream in agent.streams.split(','):
        stream = streams.get(stream)
        if stream is None:
            log.error('UNDEFINED STREAM: %s', stream)
            continue
        if not agent.scenario in stream.scenario.split(','):
            if not 'BETA' in stream.scenario.split(','):
                log.error('Scenario %s missing from %s', agent.scenario, stream)
        stream_names.append(stream.stream_name)
        paramdict = dicts.get(stream.stream_name)
        if paramdict is None:
            log.error('Unable to find stream %s in ParameterDictionary', stream.stream_name)
        for param in paramdict.parameter_ids.split(','):
            paramdef = defs.get(param)
            if paramdef is None:
                log.error('Unable to find param: %s from stream: %s', param, stream.stream_name)
                continue
            check_for_missing_values(paramdef,
                                     ['dpi', 'parameter_function_id', 'parameter_function_map','units', 'precision'])
    return stream_names

def check_agent_config(agent, stream_names):
    config = agent.config.split(',')
    my_stream_names = []
    try:
        config_dict = {each.split(':')[0]:each.split(':')[1] for each in config}
        for k,v in config_dict.iteritems():
            if k != k.strip():
                # log.warn('Whitespace in agent_default_config [%s] entry could break naive parsing! %s',
                #          agent.id, config_dict)
                pass
            try:
                v = int(v)
            except:
                log.error('Non-numeric value on right-hand-side of agent [%s] config entry %s',
                          agent.id, config_dict)
            my_stream_names.append(k.strip().split('.')[-1])

    except Exception as e:
        log.error(e)
        log.error('Unparseable agent_default_config: %s', agent)

    stream_names.sort()
    my_stream_names.sort()
    if my_stream_names != stream_names:
        log.error('Mismatch in streams for agent [%s] %s %s', agent.id, my_stream_names, stream_names)

def check_for_missing_values(data, optional=None):
    if optional is None:
        optional = []
    for k, v in data._asdict().iteritems():
        if k in optional: continue
        if v is None:
            log.warn('Missing value (%s) from %s %s', k, type(data).__name__, data.id)

def check_for_dupes(data, field):
    name = type(data[0]).__name__
    counter = Counter([getattr(each, field) for each in data])
    for k, v in counter.iteritems():
        if v > 1:
            log.warn('Duplicate record found [%s][%s] ID: %s COUNT: %d', name, field, k, v)

def main():
    global key
    options = docopt.docopt(__doc__)
    if options['--key'] is not None:
        key = options['--key']
    log.debug('Opening database...')

    if options['--rebuild'] or not os.path.exists(dbfile):
        conn = sqlite3.connect(dbfile)
        create_db(conn)

    conn = sqlite3.connect(dbfile)
    test_param_function_map(conn)

if __name__ == '__main__':
    main()


