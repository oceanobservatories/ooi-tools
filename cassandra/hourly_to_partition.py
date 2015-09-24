#!/usr/bin/env python

from cassandra.cluster import Cluster
import sys

session = None


class Partition(object):
    def __init__(self, refdes, stream, method, bin, store):
        self.refdes = refdes
        self.stream = stream
        self.method = method
        self.bin = bin
        self.store = store
        self.first = None
        self.last = None
        self.count = 0

    def add_row(self, row):
        if self.first is None or row.first < self.first:
            self.first = row.first

        if self.last is None or row.last > self.last:
            self.last = row.last

        self.count += row.count

    @staticmethod
    def key(row, store='cass'):
        refdes = '-'.join((row.subsite, row.node, row.sensor))
        _bin = row.hour / 24
        return refdes, row.stream, row.method, _bin, store


def fetch_hourly():
    return session.execute('select * from stream_metadata_hourly')


def insert_partition_row(partition):
    session.execute('insert into partition_metadata (stream, refdes, method, bin, store, count, first, last) '
                    'values (%s, %s, %s, %s, %s, %s, %s, %s)',
                    (partition.stream, partition.refdes, partition.method, partition.bin, partition.store,
                     partition.count, partition.first, partition.last))


def main():
    global session

    if len(sys.argv) == 1:
        print 'Please supply one or more contact point'
        sys.exit(1)

    contact_points = sys.argv[1:]

    cluster = Cluster(contact_points)
    session = cluster.connect('ooi')

    d = {}
    # fetch all the hourly data
    for row in fetch_hourly():
        # bin by the new partition
        key = Partition.key(row)
        d.setdefault(key, Partition(*key)).add_row(row)

    # create a new metadata record for each partition found
    for partition in d.itervalues():
        insert_partition_row(partition)


if __name__ == '__main__':
    main()
