#!/usr/bin/env python
"""
Retroactively generate DOIs for raw and parsed data represented in uFrame.
This script creates Raw File Set, Parsed Data Set, Parser, and Sensor DOIs.
DOIDaemon logic will handle creating Array, Subsite, Node, and Cruise DOIs.

Usage:
    retro_doi_builder.py <uframe_ip> <cassandra_ip_list>...
"""

import re
import csv
import sqlite3
import json
import os
import sys
import requests
import cassandra.cluster
import operator
from datetime import datetime
import docopt
import multiprocessing

# Add parent directory to python path to locate the
# metadata_service_api package
sys.path.insert(0, '/'.join(
    (os.path.dirname(os.path.realpath(__file__)), '..')))
from common import time_util
from metadata_service_api import MetadataServiceAPI
from xasset_service_api.api import XAssetServiceAPI
from doi_service_api.api import DOIServiceAPI, DOIServiceException

RUTGERS_FILE_SERVER = 'https://rawdata.oceanobservatories.org:13580/'
STREAMING_SENSOR_INVENTORY_SERVICE_TEMPLATE = 'http://{0}:12576/sensor/inv'

DATASET_L0_PROVENANCE_COLUMNS = [
    'subsite', 'node', 'sensor', 'method', 'deployment', 'id', 'fileName',
    'parserName', 'parserVersion']
ALL_DATASET_L0_PROVENANCE = 'SELECT {0} FROM dataset_l0_provenance'.format(
    ','.join(DATASET_L0_PROVENANCE_COLUMNS))

STREAM_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/streamMetadata'
PARTITION_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/partitionMetadata'
XASSET_SERVICE_URL_TEMPLATE = 'http://{0}:12587/'
DOI_SERVICE_URL_TEMPLATE = 'http://{0}:12588/doi'
PARSED_URL_TEMPLATE = """%s/sensor/inv/%s/%s/%s/%s/%s?beginDT=%s&endDT=%s&
                      execDPA=false&user=doilookup"""
DATE_REGEX = re.compile("""\\d{4}[_-]?(0[1-9]|1[0-2])[-_]?(0[1-9]|[12][0-9]|
                        3[01])""")


def streamed(method):
    return 'streamed' in method


def create_doi_records_helper(params):
    (builder, row) = params
    create_doi_records(builder, row)


def create_doi_records(builder, row):
    (subsite, node, sensor, method, deployment_number, parsername,
     parserversion, version, start, stop, latitude, longitude, depth,
     manufacturer, model, serialnumber, mio, name, uid, raw_streams) = row
    if not mio:
        mio = "OOI - Ocean Observatories Initiative"
    streams = json.loads(raw_streams)
    refdes = "-".join((subsite, node, sensor))

    # make parser DOI
    parser_doi = {}
    parser_doi["@class"] = ".ParserDOIRecord"
    parser_doi["doi"] = None
    parser_doi["obsolete"] = False
    parser_doi["error"] = None
    parser_doi["doiType"] = "PARSER"
    parser_doi["name"] = parsername
    parser_doi["version"] = parserversion
    parser_doi["language"] = "Python"
    parser_doi["predecessor"] = None
    # try to create parser_doi and store the JSON response
    try:
        response = builder.doi_service_api.create_doi_record(parser_doi)
        parser_doi["id"] = response["id"]
    except DOIServiceException as e:
        pass
    # if the creation succeeded or not, log the result
    builder.log_result(json.dumps(parser_doi), response["statusCode"])

    # make raw DOI
    raw_file_set_doi = {}
    raw_file_set_doi["@class"] = ".RawFileSetDOIRecord"
    raw_file_set_doi["doi"] = None
    raw_file_set_doi["obsolete"] = False
    raw_file_set_doi["error"] = None
    raw_file_set_doi["doiType"] = "RAW_FILE_SET"
    raw_file_set_doi["sensorDOI"] = None
    raw_file_set_doi["referenceDesignator"] = refdes
    raw_file_set_doi["deployment"] = deployment_number
    raw_file_set_doi["method"] = method

    # TODO update URL structure once it is finalized
    if streamed(method):
        date = time_util.java_time_to_iso8601_basic_date(start)
        url = RUTGERS_FILE_SERVER + "_".join([refdes, method, date])
    else:
        url = RUTGERS_FILE_SERVER + "_".join(
            (refdes, str(deployment_number), str(version), method))

    # in the current implementation, the file mask and url are the same
    raw_file_set_doi["uriMask"] = url
    raw_file_set_doi["url"] = url
    raw_file_set_doi["startTime"] = start
    raw_file_set_doi["stopTime"] = stop
    raw_file_set_doi["predecessor"] = None
    # try to create raw_file_set_doi and store the JSON response
    try:
        response = builder.doi_service_api.create_doi_record(raw_file_set_doi)
        raw_file_set_doi["id"] = response["id"]
    except DOIServiceException as e:
        pass
    # if the creation succeeded or not, log the result
    builder.log_result(json.dumps(raw_file_set_doi), response["statusCode"])

    # make parsed DOI
    parsed_data_set_doi = {}
    parsed_data_set_doi["@class"] = ".ParsedDataSetDOIRecord"
    parsed_data_set_doi["doi"] = None
    parsed_data_set_doi["obsolete"] = False
    parsed_data_set_doi["error"] = None
    parsed_data_set_doi["doiType"] = "PARSED_DATA_SET"
    parsed_data_set_doi["raw_file_set_doi"] = raw_file_set_doi
    parsed_data_set_doi["parser_doi"] = parser_doi
    parsed_data_set_doi["referenceDesignator"] = refdes
    parsed_data_set_doi["streams"] = streams
    parsed_data_set_doi["startTime"] = start
    parsed_data_set_doi["stopTime"] = stop
    parsed_data_set_doi["urls"] = builder.generate_stream_urls(parsed_data_set_doi,
                                                               method)
    parsed_data_set_doi["predecessor"] = None
    # try to create parsed_data_set_doi and store the JSON response
    try:
        response = builder.doi_service_api.create_doi_record(parsed_data_set_doi)
        parsed_data_set_doi["id"] = response["id"]
    except DOIServiceException as e:
        pass
    # if the creation succeeded or not, log the result
    builder.log_result(json.dumps(parsed_data_set_doi), response["statusCode"])


class RetroactiveDOIBuilder(object):
    def __init__(self, uframe_ip, cassandra_ip_list, database="retrodoi.db"):
        self.database = database
        self.uframe_ip = uframe_ip
        self.cassandra_ip_list = cassandra_ip_list

        stream_url = STREAM_METADATA_SERVICE_URL_TEMPLATE.format(uframe_ip)
        partition_url = PARTITION_METADATA_SERVICE_URL_TEMPLATE.format(
            uframe_ip)
        self.metadata_service_api = MetadataServiceAPI(
            stream_url, partition_url)

        xasset_url = XASSET_SERVICE_URL_TEMPLATE.format(uframe_ip)
        self.xasset_service_api = XAssetServiceAPI(
            xasset_url, requests.Session())

        doi_url = DOI_SERVICE_URL_TEMPLATE.format(uframe_ip)
        self.doi_service_api = DOIServiceAPI(doi_url, requests.Session())

        inventory_url = STREAMING_SENSOR_INVENTORY_SERVICE_TEMPLATE.format(
            uframe_ip)
        self.streaming_sensor_inventory_service = (inventory_url)

    def log_result(self, json, status_code):
        now = datetime.now()
        success_log_file = (
            "./retro_doi_gen_pass_" + now.strftime("%Y%m%d") + ".log")
        failure_log_file = (
            "./retro_doi_gen_fail_" + now.strftime("%Y%m%d") + ".log")
        if status_code in ["OK", "CREATED"]:
            log_file = success_log_file
        else:
            log_file = failure_log_file
        with open(log_file, "a") as log:
            log.write(
                "TIME: " + now.strftime("%H:%M:%S.%f")[:-3] +
                "  JSON: " + json + ", STATUS CODE: " + status_code + "\n")

    def execute_query(self, session, query, columns):
        '''Execute specified query and return sorted list of dictionaries.'''
        rows = [dict(zip(columns, row)) for row in session.execute(query)]
        rows.sort(key=operator.itemgetter(*columns))
        return rows

    def load_provenance(self):
        cluster = cassandra.cluster.Cluster(
            self.cassandra_ip_list, control_connection_timeout=60,
            protocol_version=3)
        session = cluster.connect('ooi')
        provenance = self.execute_query(
            session, ALL_DATASET_L0_PROVENANCE,
            DATASET_L0_PROVENANCE_COLUMNS)
        cluster.shutdown()

        with sqlite3.connect(self.database) as conn:

            def get_streamed_start(method, filename):
                if not streamed(method):
                    return None
                date = time_util.parse_basic_iso8601_date(filename)
                if not date:
                    return None
                return time_util.javatime_from_basic_iso8601_date(date)

            def get_streamed_stop(start):
                if not start:
                    return None
                return start + time_util.MILLIS_PER_DAY - 1

            conn.create_function("get_streamed_start", 2, get_streamed_start)
            conn.create_function("get_streamed_stop", 1, get_streamed_stop)

            conn.execute("DROP TABLE IF EXISTS PROVENANCE")
            conn.execute(
                """CREATE TABLE PROVENANCE (subsite TEXT, node TEXT,
                 sensor TEXT, method TEXT, deployment INTEGER,
                 rowid TEXT, filename TEXT, parsername TEXT,
                 parserversion TEXT)""")

            for row in provenance:
                subsite = row['subsite']
                node = row['node']
                sensor = row['sensor']
                method = row['method']
                deployment = row['deployment']
                rowid = str(row['id'])
                filename = row['fileName']
                parsername = row['parserName']
                parserversion = row['parserVersion']

                record = [subsite, node, sensor, method, deployment, rowid,
                          filename, parsername, parserversion]

                conn.execute(
                    """INSERT INTO PROVENANCE (subsite, node, sensor,
                    method, deployment, rowid, filename, parsername,
                    parserversion) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);""",
                    record)

            conn.execute("""ALTER TABLE PROVENANCE ADD COLUMN
                         start INTEGER""")
            conn.execute("""UPDATE PROVENANCE SET
                         start=get_streamed_start(method, filename)""")
            conn.execute("""ALTER TABLE PROVENANCE ADD COLUMN
                         stop INTEGER""")
            conn.execute("""UPDATE PROVENANCE SET
                         stop=get_streamed_stop(start)""")

            conn.commit()

    def load_stream_metadata(self):
        with sqlite3.connect(self.database) as conn:
            metadata = self.metadata_service_api.get_stream_metadata_records()
            conn.execute("DROP TABLE IF EXISTS METADATA")
            conn.execute(
                """CREATE TABLE METADATA (subsite TEXT, node TEXT,
                 sensor TEXT, method TEXT, start INTEGER,
                 stop INTEGER, stream TEXT)""")
            for row in metadata:
                subsite = row['referenceDesignator']['subsite']
                node = row['referenceDesignator']['node']
                sensor = row['referenceDesignator']['sensor']
                method = row['method']
                start = time_util.ntp_to_java_time(row['first'])
                stop = time_util.ntp_to_java_time(row['last'])
                stream = row['stream']
                record = [subsite, node, sensor, method, start, stop, stream]
                conn.execute(
                    """INSERT INTO METADATA
                     (subsite, node, sensor, method, start, stop, stream)
                     VALUES (?, ?, ?, ?, ?, ?, ?);""", record)
            conn.commit()

    def load_deployments(self):
        with sqlite3.connect(self.database) as conn:
            conn.execute("DROP TABLE IF EXISTS DEPLOYMENTS")
            conn.execute(
                """CREATE TABLE DEPLOYMENTS (subsite TEXT, node TEXT,
                 sensor TEXT, deployment INTEGER, version INTEGER,
                 start INTEGER, stop INTEGER, latitude REAL,
                 longitude REAL, depth REAL, manufacturer TEXT, model TEXT,
                 serialnumber TEXT, mio TEXT, name TEXT, uid TEXT)""")

            cursor = conn.cursor()
            cursor.execute("""SELECT DISTINCT subsite, node, sensor, deployment
                           FROM PROVENANCE;""")
            for subsite, node, sensor, deployment in cursor:
                if deployment == 0:
                    # use -1 to get all available deployment numbers
                    deployment = -1
                deployments = self.xasset_service_api.get_records(
                    subsite, node, sensor, deployment)

                # WARNING: This approach can potentially create DOIRecords
                # whose deployment, version, refdes, and method do not
                # correspond to existing data

                for deployment_event in deployments:
                    deployment = deployment_event["deploymentNumber"]
                    version = deployment_event["versionNumber"]
                    start = deployment_event["eventStartTime"]
                    stop = deployment_event["eventStopTime"]
                    latitude = deployment_event["location"]["latitude"]
                    longitude = deployment_event["location"]["longitude"]
                    depth = deployment_event["location"]["depth"]
                    manufacturer = deployment_event["sensor"]["manufacturer"]
                    model = deployment_event["sensor"]["modelNumber"]
                    serialnumber = deployment_event["sensor"]["serialNumber"]
                    mio = deployment_event["sensor"]["owner"]
                    name = deployment_event["sensor"]["name"]
                    uid = deployment_event["sensor"]["uid"]

                    record = [subsite, node, sensor, deployment, version,
                              start, stop, latitude, longitude, depth,
                              manufacturer, model, serialnumber, mio, name,
                              uid]

                    conn.execute(
                        """INSERT INTO DEPLOYMENTS (subsite, node,
                        sensor, deployment, version, start, stop,
                        latitude, longitude, depth, manufacturer, model,
                        serialnumber, mio, name, uid) VALUES
                        (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?, ?);""", record)
        conn.commit()

    def augment_provenance(self):
        self.load_provenance()
        self.load_stream_metadata()
        self.load_deployments()
        with sqlite3.connect(self.database) as conn:
            def get_matching_streams(subsite, node, sensor, method, start, stop):
                cursor = conn.cursor()
                cursor.execute(
                    '''SELECT stream FROM METADATA WHERE subsite = ? AND
                    node = ? AND sensor = ? AND method = ? AND
                    start < ? AND stop > ?''',
                    (subsite, node, sensor, method, stop, start))
                streams = cursor.fetchall()
                # convert from list of tuples to JSON string representing
                # list of strings
                return json.dumps([streamTuple[0] for streamTuple in streams])

            conn.create_function("get_matching_streams", 6, get_matching_streams)

            conn.execute("DROP TABLE IF EXISTS AUGMENTED_PROVENANCE")

            conn.execute(
                """CREATE TABLE AUGMENTED_PROVENANCE AS
                SELECT PROVENANCE.subsite, PROVENANCE.node,
                PROVENANCE.sensor, PROVENANCE.method,
                PROVENANCE.deployment, PROVENANCE.parsername,
                PROVENANCE.parserversion, DEPLOYMENTS.version,
                DEPLOYMENTS.start, DEPLOYMENTS.stop, DEPLOYMENTS.latitude,
                DEPLOYMENTS.longitude, DEPLOYMENTS.depth,
                DEPLOYMENTS.manufacturer, DEPLOYMENTS.model,
                DEPLOYMENTS.serialnumber, DEPLOYMENTS.mio,
                DEPLOYMENTS.name, DEPLOYMENTS.uid
                FROM PROVENANCE JOIN DEPLOYMENTS
                ON (PROVENANCE.subsite = DEPLOYMENTS.subsite
                AND PROVENANCE.node = DEPLOYMENTS.node
                AND PROVENANCE.sensor = DEPLOYMENTS.sensor
                AND PROVENANCE.deployment = DEPLOYMENTS.deployment)

                UNION

                SELECT PROVENANCE.subsite, PROVENANCE.node,
                PROVENANCE.sensor, PROVENANCE.method,
                DEPLOYMENTS.deployment, PROVENANCE.parsername,
                PROVENANCE.parserversion, DEPLOYMENTS.version,
                PROVENANCE.start, PROVENANCE.stop, DEPLOYMENTS.latitude,
                DEPLOYMENTS.longitude, DEPLOYMENTS.depth,
                DEPLOYMENTS.manufacturer, DEPLOYMENTS.model,
                DEPLOYMENTS.serialnumber, DEPLOYMENTS.mio,
                DEPLOYMENTS.name, DEPLOYMENTS.uid
                FROM PROVENANCE JOIN DEPLOYMENTS
                ON (PROVENANCE.subsite = DEPLOYMENTS.subsite
                AND PROVENANCE.node = DEPLOYMENTS.node
                AND PROVENANCE.sensor = DEPLOYMENTS.sensor
                AND PROVENANCE.method = 'streamed'
                AND (PROVENANCE.start < DEPLOYMENTS.stop OR
                DEPLOYMENTS.stop IS NULL)
                AND (PROVENANCE.stop > DEPLOYMENTS.start OR
                PROVENANCE.stop IS NULL));""")

            conn.execute("""ALTER TABLE AUGMENTED_PROVENANCE ADD COLUMN
                         streams TEXT""")
            conn.execute("""UPDATE AUGMENTED_PROVENANCE SET
                         streams=getMatchingStreams(subsite, node, sensor,
                         method, start, stop)""")
            conn.commit()

    def generate_stream_urls(self, parsed_data_set_doi, method):
        streams = parsed_data_set_doi['streams']
        refdes = parsed_data_set_doi['referenceDesignator'].split('-', 2)
        start = time_util.java_time_to_iso8601(parsed_data_set_doi['startTime'])
        stop = time_util.java_time_to_iso8601(parsed_data_set_doi['stopTime'])

        urls = []
        for stream in streams:
            urls.append(
                PARSED_URL_TEMPLATE %
                (self.streaming_sensor_inventory_service, refdes[0],
                 refdes[1], refdes[2], method, stream, start, stop))

        return urls

    def generate_dois(self):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT * FROM AUGMENTED_PROVENANCE;")

            pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
            pool.map(create_doi_records_helper, [(self, row) for row in cursor])
            pool.close()
            pool.join()

    def write_table(self, table):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM " + table + ";")
            with open(table.lower() + ".csv", 'wb') as outputFile:
                output_writer = csv.writer(outputFile)
                output_writer.writerow([i[0] for i in cursor.description])
                output_writer.writerows(cursor)


def main():
    # Process command line arguments
    options = docopt.docopt(__doc__)
    uframe_ip = options['<uframe_ip>']
    cassandra_ip_list = options['<cassandra_ip_list>']

    # Execute retroactive DOI generation code
    retro_doi_generator = RetroactiveDOIBuilder(uframe_ip, cassandra_ip_list)
    retro_doi_generator.augmentProvenance()
    retro_doi_generator.generate_dois()


if __name__ == '__main__':
    main()
