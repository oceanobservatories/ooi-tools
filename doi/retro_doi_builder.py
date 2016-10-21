#!/usr/bin/env python
"""
Retroactively generate DOIs for raw and parsed data represented in uFrame.
This script creates Raw File Set, Parsed Data Set, Parser, and Sensor DOIs.
DOIDaemon logic will handle creating Array, Subsite, Node, and Cruise DOIs.

Usage:
    retro_doi_builder.py <uframe_ip> <cassandra_ip_list>...
"""

import csv
import sqlite3
import json
import os
import sys
import requests
import cassandra.cluster
import operator
import datetime
import docopt
import traceback

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

DATASET_L0_PROVENANCE_COLUMNS = [
        'subsite', 'node', 'sensor', 'method', 'deployment', 'id', 'fileName',
        'parserName', 'parserVersion']
ALL_DATASET_L0_PROVENANCE = 'SELECT {0} FROM dataset_l0_provenance'.format(
        ','.join(DATASET_L0_PROVENANCE_COLUMNS))

STREAM_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/streamMetadata'
PARTITION_METADATA_SERVICE_URL_TEMPLATE = 'http://{0}:12571/partitionMetadata'
XASSET_SERVICE_URL_TEMPLATE = 'http://{0}:12587/'
DOI_SERVICE_URL_TEMPLATE = 'http://{0}:12588/doi'


class retroactiveDOIBuilder(object):
    def __init__(self, uframe_ip, cassandra_ip_list, database="retrodoi.db"):
        self.database = database
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
        self.streaming_sensor_inventory_service = (
                STREAMING_SENSOR_INVENTORY_SERVICE_TEMPLATE.format(uframe_ip))

    def log_result(self, json, statusCode):
        now = datetime.datetime.now()
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

    # get the column names for a table
    def getColumns(self, table):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM " + table + ";")
            return [x[0] for x in cursor.description]

    def write_csv_file(self, filepath, data, columns):
        '''Write data to specified CSV file.'''
        with open(filepath, 'wb') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=columns)
            writer.writeheader()
            for row in data:
                writer.writerow(row)

    def loadProvenance(self):
        cluster = cassandra.cluster.Cluster(
                self.cassandra_ip_list, control_connection_timeout=60,
                protocol_version=3)
        session = cluster.connect('ooi')
        provenance = self.execute_query(
                session, ALL_DATASET_L0_PROVENANCE,
                DATASET_L0_PROVENANCE_COLUMNS)
        cluster.shutdown()
        return provenance

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

    def getMatchingStreams(self, subsite, node, sensor, method, start, stop):
        with sqlite3.connect(self.database) as conn:
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

    def augmentProvenance(self):
        self.loadStreamMetadata()
        provenance = self.loadProvenance()
        with sqlite3.connect(self.database) as conn:
            conn.execute("DROP TABLE IF EXISTS AUGMENTED_PROVENANCE")
            conn.execute("""CREATE TABLE IF NOT EXISTS AUGMENTED_PROVENANCE
                (subsite TEXT, node TEXT, sensor TEXT, method TEXT,
                deployment INTEGER, filename TEXT, parsername TEXT,
                parserversion TEXT, version INTEGER, start INTEGER,
                stop INTEGER, latitude REAL, longitude REAL,
                depth REAL, manufacturer TEXT, model TEXT,
                serialnumber TEXT, mio TEXT, name TEXT, uid TEXT,
                streams TEXT)""")
            for row in provenance:
                subsite = row['subsite']
                node = row['node']
                sensor = row['sensor']
                method = row['method']
                deploymentNumber = row['deployment']
                filename = row['fileName']
                parsername = row['parserName']
                parserversion = row['parserVersion']

                # WARNING: This approach can potentially create DOIRecords
                # whose deployment, version, refdes, and method do not
                # correspond to existing data
                deployments = self.xasset_service_api.get_records(
                        subsite, node, sensor, deploymentNumber)
                for deployment in deployments:
                    if deployment["sensor"] is not None:
                        version = deployment["versionNumber"]
                        start = deployment["eventStartTime"]
                        stop = deployment["eventStopTime"]
                        ingestInfo = deployment["ingestInfo"]
                        latitude = deployment["location"]["latitude"]
                        longitude = deployment["location"]["longitude"]
                        depth = deployment["location"]["depth"]
                        manufacturer = deployment["sensor"]["manufacturer"]
                        model = deployment["sensor"]["modelNumber"]
                        serialnumber = deployment["sensor"]["serialNumber"]
                        mio = deployment["sensor"]["owner"]
                        name = deployment["sensor"]["name"]
                        uid = deployment["sensor"]["uid"]
                        streams = self.getMatchingStreams(
                                subsite, node, sensor, method, start, stop)

                        record = [subsite, node, sensor, method,
                                  deploymentNumber, filename, parsername,
                                  parserversion, version, start, stop,
                                  latitude, longitude, depth, manufacturer,
                                  model, serialnumber, mio, name, uid, streams]
                        conn.execute("""INSERT INTO AUGMENTED_PROVENANCE
                            (subsite, node, sensor, method, deployment,
                            filename, parsername, parserversion, version,
                            start, stop, latitude, longitude, depth,
                            manufacturer, model, serialnumber, mio, name,
                            uid, streams) VALUES (?, ?, ?, ?, ?, ?, ?, ?,
                            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);""", record)

                conn.commit()

    def createDOIRecords(self, row):
        subsite = row[0]
        node = row[1]
        sensor = row[2]
        method = row[3]
        deploymentNumber = row[4]
        filename = row[5]
        parsername = row[6]
        parserversion = row[7]
        version = row[8]
        start = row[9]
        stop = row[10]
        latitude = row[11]
        longitude = row[12]
        depth = row[13]
        manufacturer = row[14]
        model = row[15]
        serialnumber = row[16]
        mio = row[17]
        name = row[18]
        uid = row[19]
        streams = json.loads(row[20])

        # make parser DOI
        parserDOI = {}
        parserDOI["@class"] = ".ParserDOIRecord"
        parserDOI["doi"] = None
        parserDOI["dirty"] = False
        parserDOI["obsolete"] = False
        parserDOI["error"] = None
        parserDOI["doiType"] = "PARSER"
        parserDOI["name"] = parsername
        parserDOI["version"] = parserversion
        parserDOI["language"] = "Python"
        parserDOI["predecessor"] = None
        # try to create parserDOI and store the JSON response
        try:
            response = self.doi_service_api.create_doi_record(parserDOI)
            parserDOI["id"] = response["id"]
        except DOIServiceException as e:
            pass
        # if the creation succeeded or not, log the result
        self.log_result(json.dumps(parserDOI), response["statusCode"])

        # make sensor DOI
        sensorDOI = {}
        sensorDOI["@class"] = ".SensorDOIRecord"
        sensorDOI["doi"] = None
        sensorDOI["dirty"] = False
        sensorDOI["obsolete"] = False
        sensorDOI["error"] = None
        sensorDOI["doiType"] = "SENSOR"
        sensorDOI["uid"] = uid
        sensorDOI["referenceDesignator"] = "-".join((subsite, node, sensor))
        sensorDOI["deployment"] = deploymentNumber
        sensorDOI["version"] = version
        sensorDOI["deployed"] = start
        sensorDOI["recovered"] = stop
        sensorDOI["location"] = (
                "POINT (" + str(latitude) + " " + str(longitude) + ")")
        sensorDOI["latitude"] = latitude
        sensorDOI["longitude"] = longitude
        sensorDOI["depth"] = depth
        sensorDOI["manufacturer"] = manufacturer
        sensorDOI["model"] = model
        sensorDOI["serialNumber"] = serialnumber
        sensorDOI["mio"] = mio
        sensorDOI["name"] = name
        sensorDOI["displayName"] = name
        sensorDOI["nodeDOI"] = None
        # try to create sensorDOI and store the JSON response
        try:
            response = self.doi_service_api.create_doi_record(sensorDOI)
            sensorDOI["id"] = response["id"]
        except DOIServiceException as e:
            pass
        # if the creation succeeded or not, log the result
        self.log_result(json.dumps(sensorDOI), response["statusCode"])

        # make raw DOI
        rawFileSetDOI = {}
        rawFileSetDOI["@class"] = ".RawFileSetDOIRecord"
        rawFileSetDOI["doi"] = None
        rawFileSetDOI["dirty"] = False
        rawFileSetDOI["obsolete"] = False
        rawFileSetDOI["error"] = None
        rawFileSetDOI["doiType"] = "RAW_FILE_SET"
        rawFileSetDOI["sensorDOI"] = sensorDOI
        rawFileSetDOI["referenceDesignator"] = "-".join(
                (subsite, node, sensor))
        rawFileSetDOI["deployment"] = deploymentNumber
        rawFileSetDOI["method"] = method
        # in the current implementation, the file mask and url are the same
        refdes = "-".join((subsite, node, sensor))
        url = RUTGERS_FILE_SERVER + "_".join(
                (refdes, str(deploymentNumber), str(version), method))
        rawFileSetDOI["uriMask"] = url
        rawFileSetDOI["url"] = url
        rawFileSetDOI["startTime"] = start
        rawFileSetDOI["stopTime"] = stop
        rawFileSetDOI["predecessor"] = None
        # try to create rawFileSetDOI and store the JSON response
        try:
            response = self.doi_service_api.create_doi_record(rawFileSetDOI)
            rawFileSetDOI["id"] = response["id"]
        except DOIServiceException as e:
            pass
        # if the creation succeeded or not, log the result
        self.log_result(json.dumps(rawFileSetDOI), response["statusCode"])

        # make parsed DOI
        parsedDataSetDOI = {}
        parsedDataSetDOI["@class"] = ".ParsedDataSetDOIRecord"
        parsedDataSetDOI["doi"] = None
        parsedDataSetDOI["dirty"] = False
        parsedDataSetDOI["obsolete"] = False
        parsedDataSetDOI["error"] = None
        parsedDataSetDOI["doiType"] = "PARSED_DATA_SET"
        parsedDataSetDOI["rawFileSetDOI"] = rawFileSetDOI
        parsedDataSetDOI["parserDOI"] = parserDOI
        parsedDataSetDOI["referenceDesignator"] = "-".join(
                (subsite, node, sensor))
        parsedDataSetDOI["streams"] = streams
        parsedDataSetDOI["urls"] = self.generateStreamUrls(
                subsite, node, sensor, method, streams)
        parsedDataSetDOI["startTime"] = start
        parsedDataSetDOI["stopTime"] = stop
        parsedDataSetDOI["predecessor"] = None
        # try to create parsedDataSetDOI and store the JSON response
        try:
            response = self.doi_service_api.create_doi_record(parsedDataSetDOI)
            parsedDataSetDOI["id"] = response["id"]
        except DOIServiceException as e:
            pass
        # if the creation succeeded or not, log the result
        self.log_result(json.dumps(parsedDataSetDOI), response["statusCode"])

    def generateStreamUrls(self, subsite, node, sensor, method, streams):
        urls = []
        for stream in streams:
            urls.append("/".join((self.streaming_sensor_inventory_service,
                                  subsite, node, sensor, method,
                                  stream + "?user=doilookup&execDPA=false")))
        return urls

    def generateDOIs(self):
        with sqlite3.connect(self.database) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM AUGMENTED_PROVENANCE;")
            for row in cursor:
                try:
                    self.createDOIRecords(row)
                except Exception as e:
                    self.log_result(json.dumps(row), str(e))
                    trace = sys.exc_info()[2]
                    print(e)
                    traceback.print_tb(trace)


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
