#!/usr/bin/env python
"""
Extract Stream & Partition Metadata from Cassandra and save it to CSV files.

Usage:
    extract_data_from_cassandra.py <cassandra_ip_list>...
"""

import cassandra.cluster
import csv
import docopt
import operator

DATA_DIR = 'data'
STREAM_METADATA_FILEPATH = '/'.join((DATA_DIR, 'stream_metadata.csv'))
PARTITION_METADATA_FILEPATH = '/'.join((DATA_DIR, 'partition_metadata.csv'))

STREAM_METADATA_COLUMNS = ['subsite', 'node', 'sensor', 'method', 'stream', 'count', 'first', 'last']
ALL_STREAM_METADATA = 'SELECT {0} FROM stream_metadata'.format(','.join(STREAM_METADATA_COLUMNS))

PARTITION_METADATA_COLUMNS = ['stream', 'refdes', 'method', 'bin', 'store', 'first', 'last', 'count']
ALL_PARTITION_METADATA = 'SELECT {0} FROM partition_metadata'.format(','.join(PARTITION_METADATA_COLUMNS))


def execute_query(session, query, columns):
    '''Execute specified query and return sorted list of dictionaries.'''
    rows = [dict(zip(columns, row)) for row in session.execute(query)]
    rows.sort(key=operator.itemgetter(*columns))
    return rows


def write_csv_file(filepath, data, columns):
    '''Write data to specified CSV file.'''
    with open(filepath, 'wb') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def main():
    # Process command line arguments
    options = docopt.docopt(__doc__)
    cassandra_ip_list = options['<cassandra_ip_list>']

    # Open connection to Cassandra
    cluster = cassandra.cluster.Cluster(cassandra_ip_list, control_connection_timeout=60, protocol_version=3)
    session = cluster.connect('ooi')

    # Extract Stream Metadata from Cassandra
    stream_metadata = execute_query(session, ALL_STREAM_METADATA, STREAM_METADATA_COLUMNS)

    # Extract Partition Metadata from Cassandra
    partition_metadata = execute_query(session, ALL_PARTITION_METADATA, PARTITION_METADATA_COLUMNS)

    # Close connection to Cassandra
    cluster.shutdown()

    # Write Stream Metadata to CSV file
    write_csv_file(STREAM_METADATA_FILEPATH, stream_metadata, STREAM_METADATA_COLUMNS)

    # Write Partition Metadata to CSV file
    write_csv_file(PARTITION_METADATA_FILEPATH, partition_metadata, PARTITION_METADATA_COLUMNS)


if __name__ == '__main__':
    main()
