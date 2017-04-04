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
import traceback
import time
import multiprocessing

# Add parent directory to python path to locate the
# metadata_service_api package
sys.path.insert(0, '/'.join(
        (os.path.dirname(os.path.realpath(__file__)), '..')))
from metadata_service_api import MetadataServiceAPI
from xasset_service_api.api import XAssetServiceAPI
from doi_service_api.api import DOIServiceAPI, DOIServiceException

RUTGERS_FILE_SERVER = 'https://rawdata.oceanobservatories.org:13580/'
STREAMING_SENSOR_INVENTORY_SERVICE_TEMPLATE = 'http://{0}:12576/sensor/inv'

NTP_UNIX_DELTA_SECONDS = 2208988800
MILLIS_PER_DAY = 24 * 60 * 60 * 1000

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


# Take a timestamp representing milliseconds since the UNIX epoch and return
# an ISO8601 string representation of that time. For example, an integer input
# of 1494524856000 would result in a string output of "2017-05-11T17:47:36".
def convertMillisToIso8601(timestamp):
        return datetime.utcfromtimestamp(timestamp/1000).isoformat()


def streamed(method):
    return 'streamed' in method


def createDOIRecordsHelper(params):
    (builder, row) = params
    createDOIRecords(builder, row)


def createDOIRecords(builder, row):
    (subsite, node, sensor, method, deploymentNumber, parsername,
     parserversion, version, start, stop, latitude, longitude, depth,
     manufacturer, model, serialnumber, mio, name, uid, rawStreams) = row
    if mio is None or mio == "":
        mio = "OOI - Ocean Observatories Initiative"
    streams = json.loads(rawStreams)
    refdes = "-".join((subsite, node, sensor))

    # make parser DOI
    parserDOI = {}
    parserDOI["@class"] = ".ParserDOIRecord"
    parserDOI["doi"] = None
    parserDOI["obsolete"] = False
    parserDOI["error"] = None
    parserDOI["doiType"] = "PARSER"
    parserDOI["name"] = parsername
    parserDOI["version"] = parserversion
    parserDOI["language"] = "Python"
    parserDOI["predecessor"] = None
    # try to create parserDOI and store the JSON response
    try:
        response = builder.doi_service_api.create_doi_record(parserDOI)
        parserDOI["id"] = response["id"]
    except DOIServiceException as e:
        pass
    # if the creation succeeded or not, log the result
    builder.log_result(json.dumps(parserDOI), response["statusCode"])

    # make raw DOI
    rawFileSetDOI = {}
    rawFileSetDOI["@class"] = ".RawFileSetDOIRecord"
    rawFileSetDOI["doi"] = None
    rawFileSetDOI["obsolete"] = False
    rawFileSetDOI["error"] = None
    rawFileSetDOI["doiType"] = "RAW_FILE_SET"
    rawFileSetDOI["sensorDOI"] = None
    rawFileSetDOI["referenceDesignator"] = refdes
    rawFileSetDOI["deployment"] = deploymentNumber
    rawFileSetDOI["method"] = method

    # TODO update URL structure once it is finalized
    if streamed(method):
        date = (datetime.utcfromtimestamp(start/1000.0)
                .date().strftime('%Y%m%d'))
        url = RUTGERS_FILE_SERVER + "_".join([refdes, method, date])
    else:
        url = RUTGERS_FILE_SERVER + "_".join(
                (refdes, str(deploymentNumber), str(version), method))

    # in the current implementation, the file mask and url are the same
    rawFileSetDOI["uriMask"] = url
    rawFileSetDOI["url"] = url
    rawFileSetDOI["startTime"] = start
    rawFileSetDOI["stopTime"] = stop
    rawFileSetDOI["predecessor"] = None
    # try to create rawFileSetDOI and store the JSON response
    try:
        response = builder.doi_service_api.create_doi_record(rawFileSetDOI)
        rawFileSetDOI["id"] = response["id"]
    except DOIServiceException as e:
        pass
    # if the creation succeeded or not, log the result
    builder.log_result(json.dumps(rawFileSetDOI), response["statusCode"])

    # make parsed DOI
    parsedDataSetDOI = {}
    parsedDataSetDOI["@class"] = ".ParsedDataSetDOIRecord"
    parsedDataSetDOI["doi"] = None
    parsedDataSetDOI["obsolete"] = False
    parsedDataSetDOI["error"] = None
    parsedDataSetDOI["doiType"] = "PARSED_DATA_SET"
    parsedDataSetDOI["rawFileSetDOI"] = rawFileSetDOI
    parsedDataSetDOI["parserDOI"] = parserDOI
    parsedDataSetDOI["referenceDesignator"] = refdes
    parsedDataSetDOI["streams"] = streams
    parsedDataSetDOI["startTime"] = start
    parsedDataSetDOI["stopTime"] = stop
    parsedDataSetDOI["urls"] = builder.generateStreamUrls(parsedDataSetDOI,
                                                          method)
    parsedDataSetDOI["predecessor"] = None
    # try to create parsedDataSetDOI and store the JSON response
    try:
        response = builder.doi_service_api.create_doi_record(parsedDataSetDOI)
        parsedDataSetDOI["id"] = response["id"]
    except DOIServiceException as e:
        pass
    # if the creation succeeded or not, log the result
    builder.log_result(json.dumps(parsedDataSetDOI), response["statusCode"])


class retroactiveDOIBuilder(object):
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

    def log_result(self, json, statusCode):
        now = datetime.now()
        successLogFile = (
                "./retro_doi_gen_pass_" + now.strftime("%Y%m%d") + ".log")
        failureLogFile = (
                "./retro_doi_gen_fail_" + now.strftime("%Y%m%d") + ".log")
        if statusCode in ["OK", "CREATED"]:
            logFile = successLogFile
        else:
            logFile = failureLogFile
        with open(logFile, "a") as log:
            log.write(
                    "TIME: " + now.strftime("%H:%M:%S.%f")[:-3] +
                    "  JSON: " + json + ", STATUS CODE: " + statusCode + "\n")

    def execute_query(self, session, query, columns):
        '''Execute specified query and return sorted list of dictionaries.'''
        rows = [dict(zip(columns, row)) for row in session.execute(query)]
        rows.sort(key=operator.itemgetter(*columns))
        return rows

    def loadProvenance(self):
        cluster = cassandra.cluster.Cluster(
                self.cassandra_ip_list, control_connection_timeout=60,
                protocol_version=3)
        session = cluster.connect('ooi')
        provenance = self.execute_query(
                session, ALL_DATASET_L0_PROVENANCE,
                DATASET_L0_PROVENANCE_COLUMNS)
        cluster.shutdown()

        with sqlite3.connect(self.database) as conn:

            def getStreamedStart(method, filename):
                if not streamed(method):
                    return None
                match = re.search(DATE_REGEX, filename)
                if match is None:
                    return None
                dateString = match.group().replace("-", "")
                date = datetime.strptime(dateString, "%Y%m%d")
                epoch = datetime(1970, 1, 1)
                return (date - epoch).total_seconds() * 1000

            def getStreamedStop(start):
                if start is None:
                    return None
                return start + MILLIS_PER_DAY - 1

            conn.create_function("getStreamedStart", 2, getStreamedStart)
            conn.create_function("getStreamedStop", 1, getStreamedStop)

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

            conn.execute("""ALTER TABLE PROVENANCE add column
                         start INTEGER""")
            conn.execute("""UPDATE PROVENANCE SET
                         start=getStreamedStart(method, filename)""")
            conn.execute("""ALTER TABLE PROVENANCE add column
                         stop INTEGER""")
            conn.execute("""UPDATE PROVENANCE SET
                         stop=getStreamedStop(start)""")

            conn.commit()

    def loadStreamMetadata(self):
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
                start = long(
                        round(row['first'] - NTP_UNIX_DELTA_SECONDS) * 1000)
                stop = long(round(row['last'] - NTP_UNIX_DELTA_SECONDS) * 1000)
                stream = row['stream']
                record = [subsite, node, sensor, method, start, stop, stream]
                conn.execute(
                        """INSERT INTO METADATA
                         (subsite, node, sensor, method, start, stop, stream)
                         VALUES (?, ?, ?, ?, ?, ?, ?);""", record)
            conn.commit()

    def loadDeployments(self):
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
            for row in cursor:
                subsite, node, sensor, deployment = row
                if (deployment == 0):
                    # use -1 to get all available deployment numbers
                    deployment = -1
                deployments = self.xasset_service_api.get_records(
                              subsite, node, sensor, deployment)

                # WARNING: This approach can potentially create DOIRecords
                # whose deployment, version, refdes, and method do not
                # correspond to existing data

                for deploymentEvent in deployments:
                    deployment = deploymentEvent["deploymentNumber"]
                    version = deploymentEvent["versionNumber"]
                    start = deploymentEvent["eventStartTime"]
                    stop = deploymentEvent["eventStopTime"]
                    latitude = deploymentEvent["location"]["latitude"]
                    longitude = deploymentEvent["location"]["longitude"]
                    depth = deploymentEvent["location"]["depth"]
                    manufacturer = deploymentEvent["sensor"]["manufacturer"]
                    model = deploymentEvent["sensor"]["modelNumber"]
                    serialnumber = deploymentEvent["sensor"]["serialNumber"]
                    mio = deploymentEvent["sensor"]["owner"]
                    name = deploymentEvent["sensor"]["name"]
                    uid = deploymentEvent["sensor"]["uid"]

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

    def augmentProvenance(self):
        self.loadProvenance()
        self.loadStreamMetadata()
        self.loadDeployments()
        with sqlite3.connect(self.database) as conn:
            def getMatchingStreams(subsite, node, sensor, method, start, stop):
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

            conn.create_function("getMatchingStreams", 6, getMatchingStreams)

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

            conn.execute("""ALTER TABLE AUGMENTED_PROVENANCE add column
                         streams TEXT""")
            conn.execute("""UPDATE AUGMENTED_PROVENANCE SET
                         streams=getMatchingStreams(subsite, node, sensor,
                         method, start, stop)""")
            conn.commit()

    def generateStreamUrls(self, parsedDataSetDOI, method):
        streams = parsedDataSetDOI['streams']
        refdes = parsedDataSetDOI['referenceDesignator'].split('-', 2)
        start = convertMillisToIso8601(parsedDataSetDOI['startTime'])
        stop = convertMillisToIso8601(parsedDataSetDOI['stopTime'])

        urls = []
        for stream in streams:
            urls.append(
                        PARSED_URL_TEMPLATE %
                        (self.streaming_sensor_inventory_service, refdes[0],
                         refdes[1], refdes[2], method, stream, start, stop))

        return urls

    def generateDOIs(self):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT * FROM AUGMENTED_PROVENANCE;")

            pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())
            pool.map(createDOIRecordsHelper, [(self, row) for row in cursor])
            pool.close()
            pool.join()

    def writeTable(self, table):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM " + table + ";")
            with open(table.lower() + ".csv", 'wb') as outputFile:
                outputWriter = csv.writer(outputFile)
                outputWriter.writerow([i[0] for i in cursor.description])
                outputWriter.writerows(cursor)


def main():
    # Process command line arguments
    options = docopt.docopt(__doc__)
    uframe_ip = options['<uframe_ip>']
    cassandra_ip_list = options['<cassandra_ip_list>']

    # Execute retroactive DOI generation code
    retroDOIGen = retroactiveDOIBuilder(uframe_ip, cassandra_ip_list)
    retroDOIGen.augmentProvenance()
    retroDOIGen.generateDOIs()

if __name__ == '__main__':
    main()
