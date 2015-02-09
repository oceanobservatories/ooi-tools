#!/usr/bin/env python
import codecs

import os
import sqlite3
import logging
import jinja2
from parse_preload import create_db, load_paramdicts, load_paramdefs


dbfile = 'preload.db'


def get_logger():
    logger = logging.getLogger('generate_cql')
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


cql_parameter_map = {
    'int8': 'int',
    'int16': 'int',
    'int32': 'int',
    'int64': 'bigint',
    'uint8': 'int',
    'uint16': 'int',
    'uint32': 'bigint',
    'uint64': 'bigint',
    'string': 'text',
    'float32': 'double',
    'float64': 'double',
}

java_parameter_map = {
    'int': 'int',
    'bigint': 'long',
    'varint': 'BigInteger',
    'text': 'String',
    'double': 'double'
}

java_promoted_map = {
    'int': 'Integer',
    'bigint': 'Long',
    'varint': 'BigInteger',
    'text': 'String',
    'double': 'Double'
}

def camelize(s, all=False):
    parts = s.split('_')
    if all:
        parts = [x.capitalize() for x in parts]
    else:
        parts = parts[:1] + [x.capitalize() for x in parts[1:]]
    return ''.join(parts)

class Column(object):
    def __init__(self):
        # flags
        self.valid = True
        self.islist = False
        self.sparse = False
        self.numeric = False
        self.fillable = True

        self.cqltype = self.javatype = self.cqlanno = None
        self.name = self.javaname = self.setter = self.getter = None
        self.fillvalue = self.fillvar = None

    def parse(self, param):
        self.set_name(param.name)
        # preferred timestamp is enum in preload, string in practice
        if self.name == 'preferred_timestamp':
            value_encoding = 'text'
        else:
            value_encoding = cql_parameter_map.get(param.value_encoding)

        if value_encoding is not None:
            self.cqlanno = value_encoding.upper()

        # unknown encoding - log and mark this column as invalid
        if value_encoding is None:
            log.error('unknown encoding type for parameter: %s', param)
            self.valid = False

        if 'array' in param.parameter_type:
            self.islist = True
            self.cqltype = 'list<%s>' % value_encoding
            self.javatype = 'List<%s>' % java_promoted_map.get(value_encoding)
            self.filltype = java_parameter_map.get(value_encoding)
            self.java_object = java_promoted_map.get(value_encoding)
        else:
            self.cqltype = value_encoding
            self.javatype = self.filltype = java_parameter_map.get(value_encoding)
            self.java_object = java_promoted_map.get(value_encoding)

        if 'sparse' in param.parameter_type:
            self.sparse = True

        if self.javatype in ['int', 'long', 'double', 'BigInteger']:
            self.numeric = True

        self.fillvalue = param.fill_value

        if self.java_object == 'Double':
            # ignore preload, this will always be NaN
            self.fillvalue = 'Double.NaN'
        elif self.java_object == 'Integer':
            try:
                fv = int(self.fillvalue)
                if fv > 2**31-1 or fv < -2**31:
                    log.error('BAD FILL VALUE for %s %d', self.name, fv)
                    self.fillvalue = -999999999
                else:
                    self.fillvalue = fv
            except:
                log.error('BAD FILL VALUE for %s %s', self.name, self.fillvalue)
                self.fillvalue = -999999999
        elif self.java_object == 'Long':
            try:
                fv = int(self.fillvalue)
                if fv > 2**63-1 or fv < -2**63:
                    log.error('BAD FILL VALUE for %s %d', self.name, fv)
                    self.fillvalue = -999999999999999999
                else:
                    self.fillvalue = fv
            except:
                log.error('BAD FILL VALUE for %s %s', self.name, self.fillvalue)
                self.fillvalue = -999999999999999999
        elif self.java_object == 'BigInteger':
            try:
                fv = int(self.fillvalue)
                self.fillvalue = fv
            except:
                log.error('BAD FILL VALUE for %s %s', self.name, self.fillvalue)
                self.fillvalue = -9999999999999999999999

    def set_name(self, name):
        self.name = name.strip()
        self.javaname = self.name
        self.fillvar = self.name + "Fill"
        self.getter = "get" + self.name[0].capitalize() + self.name[1:]
        self.setter = "set" + self.name[0].capitalize() + self.name[1:]
        self.filler = "fill" + self.name[0].capitalize() + self.name[1:]



class Table(object):
    def __init__(self, name, params):
        self.name = name.strip()
        self.classname = camelize(self.name, all=True)
        self.params = params
        self.basecolumns = ['driver_timestamp', 'ingestion_timestamp', 'internal_timestamp',
                            'preferred_timestamp', 'time', 'port_timestamp']
        self.valid = True
        self.columns = []
        self.column_names = []
        self.build_columns()

    def build_columns(self):
        params = [(p.name, p) for p in self.params]
        params.sort()
        params = [p[1] for p in params]
        for param in params:
            # function? skip
            if param.name in self.basecolumns or param.parameter_type == 'function':
                continue
            column = Column()
            column.parse(param)
            if column.valid:
                if column.name in self.column_names:
                    log.error('DUPLICATE COLUMN: %s', self.name)
                    continue
                self.columns.append(column)
                self.column_names.append(column.name)
                if column.islist:
                    shape = Column()

                    shape.set_name(column.name + "_shape")
                    shape.cqltype = 'list<int>'
                    shape.cqlanno = 'INT'
                    shape.javatype = 'List<Integer>'
                    shape.fillable = False
                    shape.islist = True

                    self.columns.append(shape)
            else:
                self.valid = False
                break


def massage_value(x):
    if x is None:
        return ''
    return unicode(x.strip())


def generate(stream_dict, param_dict, java_template, cql_template, mapper_template):
    for d in ['cql', 'java/tables']:
        if not os.path.exists(d):
            os.makedirs(d)

    tables = []
    with codecs.open('cql/all.cql', 'wb', 'utf-8') as all_cql_fh:
        for stream in stream_dict.itervalues():
            if stream.parameter_ids is not None:
                params = []
                for param in stream.parameter_ids.split(','):
                    param = param_dict.get(param)
                    if param is None:
                        continue
                    params.append(param)
                t = Table(stream.name, params)
                tables.append(t)
                all_cql_fh.write(cql_template.render(table=t))
                all_cql_fh.write('\n\n')
                with codecs.open('java/tables/%s.java' % t.classname, 'wb', 'utf-8') as fh:
                    fh.write(java_template.render(table=t))
                with codecs.open('cql/%s.cql' % t.name, 'wb', 'utf-8') as fh:
                    fh.write(cql_template.render(table=t))

    # sort the list of tables by name for the mapper class
    tables = [(table.name, table) for table in tables]
    tables.sort()
    tables = [table[1] for table in tables]
    with codecs.open('java/ParticleMapper.java', 'wb', 'utf-8') as mapper_fh:
        mapper_fh.write(mapper_template.render(tables=tables))


def main():
    templateLoader = jinja2.FileSystemLoader(searchpath="templates")
    templateEnv = jinja2.Environment(loader=templateLoader, trim_blocks=True, lstrip_blocks=True)
    java_template = templateEnv.get_template('java.jinja')
    cql_template = templateEnv.get_template('cql.jinja')
    mapper_template = templateEnv.get_template('mapper.jinja')

    if not os.path.exists(dbfile):
        conn = sqlite3.connect(dbfile)
        create_db(conn)

    conn = sqlite3.connect(dbfile)
    stream_dict = load_paramdicts(conn)[1]
    param_dict = load_paramdefs(conn)
    generate(stream_dict, param_dict, java_template, cql_template, mapper_template)


main()

