#!/usr/bin/env python
"""
Delete data for a specific reference designator from cassandra.

Usage:
    delete_data.py <refdes> <contact_ip>...

"""

from cassandra.cluster import Cluster


class Deleter(object):
    def __init__(self, contacts, refdes):
        self.contacts = contacts
        self.refdes = refdes
        self.subsite, self.node, self.sensor = self.parse_refdes(refdes)
        # For now use version=3 against the current cassandra.
        self.cluster = Cluster(self.contacts, control_connection_timeout=60, protocol_version=3)

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

    def delete_provenance(self, methods):
        for method in methods:
            print 'Deleting provenance:%s:%s' % (self.refdes, method)
            # decide which table to delete from - note different refdes specification.
            if 'streaming' == method:
                del_provenance = self.session.prepare('delete from streaming_l0_provenance where refdes=? and method=?')
                self.session.execute(del_provenance, (self.refdes, method))
            else:
                del_provenance = self.session.prepare('delete from dataset_l0_provenance where subsite=? and node=? and sensor=? and method=?')
                self.session.execute(del_provenance, (self.subsite, self.node, self.sensor, method))

    @staticmethod
    def parse_refdes(refdes):
        return refdes.split('-', 2)

    def find_stream_methods(self):
        return self.session.execute(self.lookup_stream_method, (self.subsite, self.node, self.sensor))

    def delete(self):
        # collect the methods that will need to be deleted from provenance.
        methods = set()
        for row in self.find_stream_methods():
            print 'Deleting:', row
            self.delete_stream(row.stream)
            self.delete_metadata(row.method, row.stream)
            methods.add(row.method)
        self.delete_provenance(methods)

def main():
    import docopt
    options = docopt.docopt(__doc__)
    deleter = Deleter(options['<contact_ip>'], options['<refdes>'])
    deleter.delete()


if __name__ == '__main__':
    main()
