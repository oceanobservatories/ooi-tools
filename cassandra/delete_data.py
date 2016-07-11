#!/usr/bin/env python
"""
Delete data for a specific reference designator from cassandra.

Usage:
    delete_data.py <refdes> <uframe_ip> <cassandra_ip_list>...
"""

from cassandra.cluster import Cluster
from metadata_service_api import MetadataServiceAPI

STREAM_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/streamMetadata'
PARTITION_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/partitionMetadata'


class Deleter(object):

    def __init__(self, refdes, uframe_ip, cassandra_ip_list):
        self.refdes = refdes
        self.subsite, self.node, self.sensor = self.parse_refdes(refdes)
        # For now use version=3 against the current cassandra.
        cluster = Cluster(cassandra_ip_list, control_connection_timeout=60, protocol_version=3)
        self.session = cluster.connect('ooi')
        stream_url = STREAM_METADATA_SERVICE_URL_TEMPLATE.format(uframe_ip)
        partition_url = PARTITION_METADATA_SERVICE_URL_TEMPLATE.format(uframe_ip)
        self.metadata_service_api = MetadataServiceAPI(stream_url, partition_url)

    @staticmethod
    def parse_refdes(refdes):
        return refdes.split('-', 2)

    def get_stream_info(self):
        result = {}
        for partitionMetadataRecord in self.metadata_service_api.get_partition_metadata_records(self.subsite, self.node, self.sensor):
            result.setdefault(partitionMetadataRecord['stream'], []).append(partitionMetadataRecord['bin'])
        return result

    def delete_stream(self, stream, bins):
        delete_stream_data = self.session.prepare('delete from %s where subsite=? and node=? and sensor=? and bin=?' % stream)
        for bin in bins:
            self.session.execute(delete_stream_data, (self.subsite, self.node, self.sensor, bin))

    def delete_metadata(self):
        self.metadata_service_api.delete_stream_metadata_records(self.subsite, self.node, self.sensor)
        self.metadata_service_api.delete_partition_metadata_records(self.subsite, self.node, self.sensor)

    def delete_provenance(self):
        # Delete non-streaming provenance
        delete_non_streaming_provenance = self.session.prepare('delete from dataset_l0_provenance where subsite=? and node=? and sensor=?')
        self.session.execute(delete_non_streaming_provenance, (self.subsite, self.node, self.sensor))
        # Delete streaming provenance
        delete_streaming_provenance = self.session.prepare('delete from streaming_l0_provenance where refdes=? and method=?')
        self.session.execute(delete_streaming_provenance, (self.refdes, 'streaming'))

    def delete(self):
        for (stream, bins) in self.get_stream_info().items():
            self.delete_stream(stream, bins)
        self.delete_metadata()
        self.delete_provenance()


def main():
    import docopt
    options = docopt.docopt(__doc__)
    deleter = Deleter(options['<refdes>'], options['<uframe_ip>'], options['<cassandra_ip_list>'])
    deleter.delete()


if __name__ == '__main__':
    main()

