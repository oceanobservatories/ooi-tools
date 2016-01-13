#!/usr/bin/env python
"""
Delete a specific reference designator from cassandra

Usage:
    delete_stream.py <refdes> <contact_ip>...

"""

from cassandra.cluster import Cluster


class Deleter(object):
    def __init__(self, contacts, refdes):
        self.contacts = contacts
        self.refdes = refdes
        self.subsite, self.node, self.sensor = self.parse_refdes(refdes)
        self.cluster = Cluster(self.contacts, control_connection_timeout=60)
        self.session = self.cluster.connect('ooi')

        self.lookup_stream_method = self.session.prepare('select stream, method, count from stream_metadata where subsite=? and node=? and sensor=?')
        self.del_metadata = self.session.prepare('delete from stream_metadata where subsite=? and node=? and sensor=? and method=? and stream=?')
        self.del_p_metadata = self.session.prepare('delete from partition_metadata where stream=? and refdes=?')
        self.select_p_metadata = self.session.prepare('select * from partition_metadata where stream=? and refdes=?')

    def delete_metadata(self, method, stream):
        self.session.execute(self.del_metadata, (self.subsite, self.node, self.sensor, method, stream))
        self.session.execute(self.del_p_metadata, (stream, self.refdes))

    def delete_stream(self, stream):
        delete_q = self.session.prepare('delete from %s where subsite=? and node=? and sensor=? and method=? and bin=?' % stream)
        for row in self.session.execute(self.select_p_metadata, (stream, self.refdes)):
            print '  Deleting: ', row
            self.session.execute(delete_q, (self.subsite, self.node, self.sensor, row.method, row.bin))

    @staticmethod
    def parse_refdes(refdes):
        return refdes.split('-', 2)

    def find_stream_methods(self):
        return self.session.execute(self.lookup_stream_method, (self.subsite, self.node, self.sensor))

    def delete(self):
        for row in self.find_stream_methods():
            print 'Deleting:', row
            self.delete_stream(row.stream)
            self.delete_metadata(row.method, row.stream)


def main():
    import docopt
    options = docopt.docopt(__doc__)
    deleter = Deleter(options['<contact_ip>'], options['<refdes>'])
    deleter.delete()


if __name__ == '__main__':
    main()
