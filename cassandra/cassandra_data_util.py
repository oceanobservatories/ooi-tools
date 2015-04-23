#!/usr/bin/env python
"""
Usage:
  cassandra_data_util.py <dir> (--load|--dump) [--filter=<regex>] [--keyspace=<name>] [--contact=<ip_address>]
  cassandra_data_util.py --direct --remote_contact=<ip_address> --remote_keyspace=<name> [--keyspace=<name>] [--contact=<contact>]

Options:
  --keyspace=<name>       Source Keyspace [default: ooi]
  --contact=<ip_address>  Source Contact Point [default: 127.0.0.1]
"""
import glob
import os
import uuid
from cassandra.query import dict_factory
from cassandra.cluster import Cluster
import re
import msgpack
from docopt import docopt
import sys


def dump_data(directory, filter_string, contact_point, keyspace):
    cluster = Cluster([contact_point])
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
                row['id'] = str(row['id'])
                fh.write(msgpack.packb(row))


def insert_data(directory, contact_point, keyspace):
    os.chdir(directory)

    cluster = Cluster([contact_point])
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
                    keys = record.keys()
                    ps = session.prepare('insert into %s (%s) values (%s)'
                                         % (tablename, ','.join(keys), ','.join('?' for _ in keys)))
                    first = False

                if 'id' in record:
                    record['id'] = uuid.UUID(record['id'])

                session.execute(ps, record)

                if (index + 1) % 100 == 0:
                    sys.stdout.write('.')
                    sys.stdout.flush()
            print


def main():
    options = docopt(__doc__)

    if options['--dump']:
        dump_data(options['<dir>'], options['--filter'], options['--contact'], options['--keyspace'])
    elif options['--load']:
        insert_data(options['<dir>'], options['--contact'], options['--keyspace'])
    elif options['--direct']:
        print 'not yet implemented'


if __name__ == '__main__':
    main()
