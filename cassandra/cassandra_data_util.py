#!/usr/bin/env python
"""
Usage:
  cassandra_data_util.py <dir> (--load|--dump) [--filter=<regex>] [--keyspace=<name>] [--contact=<ip_address>] [--upgrade=<upgrade_id>] [--preload=<preload>]
  cassandra_data_util.py --direct --remote_contact=<ip_address> --remote_keyspace=<name> [--keyspace=<name>] [--contact=<contact>]

Options:
  --keyspace=<name>       Source Keyspace [default: ooi]
  --contact=<ip_address>  Source Contact Point [default: 127.0.0.1]
"""
import glob
import os
import uuid
from cassandra.query import dict_factory, _clean_column_name
from cassandra.cluster import Cluster
import re
import msgpack
from docopt import docopt
import sys
import numpy
import struct


def dump_data(directory, filter_string, contact_point, keyspace):
    cluster = Cluster([contact_point], control_connection_timeout=60)
    session = cluster.connect(keyspace)
    session.row_factory = dict_factory
    tables = []

    if filter_string is not None:
        filter_re = re.compile(filter_string)

    if not os.path.exists(directory):
        os.makedirs(directory)

    os.chdir(directory)

    with open('stream_metadata.mpk', 'wb') as fh:
        for row in session.execute('select * from stream_metadata', timeout=None):
            if filter_string is None or any((filter_re.search(x) for x in row.values() if isinstance(x, basestring))):
                tables.append(row['stream'])
                fh.write(msgpack.packb(row))

    for table in sorted(set(tables)):
        print 'dumping table: %s' % table
        with open('%s.mpk' % table, 'wb') as fh:
            for row in session.execute('select * from %s' % table, timeout=None):
                for k in row:
                    if type(row[k]) == uuid.UUID:
                        row[k] = str(row[k])
                fh.write(msgpack.packb(row))
    session.shutdown()
    cluster.shutdown()


def insert_data(directory, contact_point, keyspace, upgrade_id=None):
    os.chdir(directory)

    cluster = Cluster([contact_point], control_connection_timeout=60)
    session = cluster.connect(keyspace)

    for mpk in glob.glob('*.mpk'):
        tablename = mpk.replace('.mpk', '')
        print 'inserting records into table: %s' % tablename
        with open(mpk) as fh:
            unpacker = msgpack.Unpacker(fh)
            first = True
            ps = None
            for index, record in enumerate(unpacker):
                if first:
                    cols = cluster.metadata.keyspaces[keyspace].tables[tablename].columns
                    keys = map(_clean_column_name, cols.keys())
                    uuids = [k for k in keys if cols[k].typestring == 'uuid']
                    ps = session.prepare('insert into %s (%s) values (%s)'
                                         % (tablename, ','.join(keys), ','.join('?' for _ in keys)))
                    first = False

                for k in uuids:
                    if record[k] is not None:
                        record[k] = uuid.UUID(record[k])

                if upgrade_id is not None:
                    record = upgrade(record, upgrade_id, tablename)

                session.execute(ps, record)

                if (index + 1) % 100 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()
            print
    session.shutdown()
    cluster.shutdown()


def upgrade(record, upgrade, tablename):
    if upgrade == '5.1-to-5.2' and tablename != 'stream_metadata':
        record['bin'] = int(record['time'] / (24 * 60 * 60))
        convert_to_msgpack(record)
    return record


pname_map = {}
def convert_to_msgpack(r):
    for pname in r:
        data = r[pname]
        shape_name = pname + '_shape'
        if shape_name in r:
            shape = r[shape_name]
            value_encoding = pname_map.get(pname);
            if value_encoding is None:
                from preload_database.model.preload import Parameter
                p = Parameter.query.filter(Parameter.name == pname).first()
                value_encoding = p.value_encoding.value
                pname_map[pname] = value_encoding
            if value_encoding != 'string':
                data = handle_byte_buffer(data, value_encoding, shape)
                r[pname] = msgpack.packb(data.tolist())


def handle_byte_buffer(data, encoding, shape):
    if encoding in ['int8', 'int16', 'int32', 'uint8', 'uint16']:
        format_string = 'i'
        count = len(data) / 4
    elif encoding in ['uint32', 'int64']:
        format_string = 'q'
        count = len(data) / 8
    elif encoding in ['uint64']:
        format_string = 'Q'
        count = len(data) / 8
    elif 'float' in encoding:
        format_string = 'd'
        count = len(data) / 8
    else:
        raise Exception('Unknown encoding %s' % (encoding))

    data = numpy.array(struct.unpack('>%d%s' % (count, format_string), data))
    data = data.reshape(shape)
    return data


def main():
    options = docopt(__doc__)

    if options['--preload']:
        import sys
        sys.path.append(options['--preload'])
        import preload_database.database
        preload_database.database.initialize_connection(preload_database.database.PreloadDatabaseMode.POPULATED_FILE)
        preload_database.database.open_connection()

    if options['--dump']:
        dump_data(options['<dir>'], options['--filter'], options['--contact'], options['--keyspace'])
    elif options['--load']:
        insert_data(options['<dir>'], options['--contact'], options['--keyspace'], options['--upgrade'])
    elif options['--direct']:
        print 'not yet implemented'


if __name__ == '__main__':
    main()
