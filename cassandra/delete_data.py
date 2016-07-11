#!/usr/bin/env python
"""
Delete data for a specific reference designator from cassandra.

Usage:
    delete_data.py <refdes> <uframe_ip> <cassandra_ip_list>...
"""

import cassandra.cluster
import docopt
import os
import sys

# Add parent directory to python path to locate the metadata_service_api package
sys.path.insert(0, '/'.join((os.path.dirname(os.path.realpath(__file__)), '..')))
from metadata_service_api import MetadataServiceAPI

STREAM_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/streamMetadata'
PARTITION_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/partitionMetadata'


class Deleter(object):

    def __init__(self, refdes, uframe_ip, cassandra_ip_list):
        self.refdes = refdes
        self.subsite, self.node, self.sensor = self.parse_refdes(refdes)
        # For now use version=3 against the current cassandra.
        cluster = cassandra.cluster.Cluster(cassandra_ip_list, control_connection_timeout=60, protocol_version=3)
        self.session = cluster.connect('ooi')
        stream_url = STREAM_METADATA_SERVICE_URL_TEMPLATE.format(uframe_ip)
        partition_url = PARTITION_METADATA_SERVICE_URL_TEMPLATE.format(uframe_ip)
        self.metadata_service_api = MetadataServiceAPI(stream_url, partition_url)

    @staticmethod
    def parse_refdes(refdes):
        return refdes.split('-', 2)

    def get_stream_info(self):
        result = {}
        partition_metadata_record_list = self.metadata_service_api.get_partition_metadata_records(
            self.subsite, self.node, self.sensor
        )
        for partition_metadata_record in partition_metadata_record_list:
            result.setdefault(partition_metadata_record['stream'], []).append(partition_metadata_record['bin'])
        return result

    def delete_stream(self, stream, bins):
        query = self.session.prepare('delete from %s where subsite=? and node=? and sensor=? and bin=?' % stream)
        for bin in bins:
            self.session.execute(query, (self.subsite, self.node, self.sensor, bin))

    def delete_metadata(self):
        self.metadata_service_api.delete_stream_metadata_records(self.subsite, self.node, self.sensor)
        self.metadata_service_api.delete_partition_metadata_records(self.subsite, self.node, self.sensor)

    def delete_provenance(self):
        query = self.session.prepare('delete from dataset_l0_provenance where subsite=? and node=? and sensor=?')
        self.session.execute(query, (self.subsite, self.node, self.sensor))

    def delete(self):
        for stream, bins in self.get_stream_info().iteritems():
            self.delete_stream(stream, bins)
        self.delete_metadata()
        self.delete_provenance()


def main():
    # Process command line arguments
    options = docopt.docopt(__doc__)
    refdes = options['<refdes>']
    uframe_ip = options['<uframe_ip>']
    cassandra_ip_list = options['<cassandra_ip_list>']

    # Execute deletion code
    deleter = Deleter(refdes, uframe_ip, cassandra_ip_list)
    deleter.delete()


if __name__ == '__main__':
    main()
