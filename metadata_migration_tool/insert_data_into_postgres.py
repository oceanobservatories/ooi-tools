#!/usr/bin/env python
"""
Read Stream & Partition Metadata from CSV files and insert it into Postgres.

Usage:
    insert_data_into_postgres.py <uframe_ip>
"""

import csv
import docopt
import os
import sys

# Add parent directory to python path to locate the metadata_service_api package
sys.path.insert(0, '/'.join((os.path.dirname(os.path.realpath(__file__)), '..')))
from metadata_service_api import MetadataServiceAPI

DATA_DIR = 'data'
STREAM_METADATA_FILEPATH = '/'.join((DATA_DIR, 'stream_metadata.csv'))
PARTITION_METADATA_FILEPATH = '/'.join((DATA_DIR, 'partition_metadata.csv'))

STREAM_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/streamMetadata'
PARTITION_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/partitionMetadata'


def read_csv_file(filepath):
    '''Read specified CSV file into list of dictionaries.'''
    with open(filepath) as csvfile:
        return [row for row in csv.DictReader(csvfile)]


def parse_refdes(refdes):
    '''Parse a Reference Designator string into a dictonary.'''
    parts = refdes.split('-', 2)
    return {'subsite': parts[0], 'node': parts[1], 'sensor': parts[2]}


def main():
    # Process command line arguments
    options = docopt.docopt(__doc__)
    uframe_ip = options['<uframe_ip>']

    # Read Stream Metadata from CSV file
    stream_metadata = read_csv_file(STREAM_METADATA_FILEPATH)

    # Read Partition Metadata from CSV file
    partition_metadata = read_csv_file(PARTITION_METADATA_FILEPATH)

    # Instantiate Metadata Service API
    stream_url = STREAM_METADATA_SERVICE_URL_TEMPLATE.format(uframe_ip)
    partition_url = PARTITION_METADATA_SERVICE_URL_TEMPLATE.format(uframe_ip)
    metadata_service_api = MetadataServiceAPI(stream_url, partition_url)

    # Insert Stream Metadata into Postgres
    for row in stream_metadata:
        rec = metadata_service_api.build_stream_metadata_record(**row)
        metadata_service_api.create_stream_metadata_record(rec)

    # Insert Partition Metadata into Postgres
    for row in partition_metadata:
        temp = row.copy()
        del temp['refdes']
        temp.update(parse_refdes(row['refdes']))
        rec = metadata_service_api.build_partition_metadata_record(**temp)
        metadata_service_api.create_partition_metadata_record(rec)


if __name__ == '__main__':
    main()
