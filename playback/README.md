# Installation

## Initial Setup
The following instructions are for MacOS. If you have a different platform, follow the links for instructions specific to 
your OS.

Install Python 3: (c.f. https://www.python.org/downloads/)
`sudo brew install python3`

Install the Python package installer:
`sudo brew install pip` (c.f. https://pip.pypa.io/en/stable/installing/)

Install the Python environment handler: (c.f. https://docs.pipenv.org/en/latest/)
`sudo brew install pipenv`

Setup local development folder for jupyter notebook: (c.f. https://jupyter.readthedocs.io/en/latest/install.html)
`cd ~/<your notebook folder>`
`pipenv install jupyter`

Configure your notebook virtual environment:
`pipenv --three`
`pipenv --install requests`

Copy `playback.ipynb` into your folder: (you can also use this to update the notebook to the latest version)
`wget https://raw.githubusercontent.com/oceanobservatories/ooi-tools/master/playback/playback.ipynb`

# Development Environment (Notebook on local machine)
The following instructions assume you have already completed the steps in Initial Setup and are ready to use the notebook.

Enter the notebook virtual environment:
`cd ~/<your notebook folder>`
`pipenv shell`
`jupyter notebook &`

This will open up a tab in your default web browser. Click on `playback` to open the playback notebook.

# Usage
## Tips
Once you have access to the notebook, you can alter and run the available commands. Here are a few general tips:
* Avoid the "run all" option (the fast forward icon) as this will run all commands for the entire session as is. You will 
need to make modification to the session for your particular purpose before running and you should familiarize yourself with 
each command prior to running the entire set (which is rarely what you'll want to do).
* Each time you load the notebook, you will need to run each cell to make it active.
* The last executed line in the cell will be output below the cell (similar to the Python command line interface). 
This is helpful examining data as you don't have to use a print command.
* Most of the m2m commands will return the json data from the request. You may want to examine the data prior to parsing it
so you can see the precise format of the contents.

## Initial Setup
You will need to specify your M2M credentials prior to running any m2m commands:
```python
### SETUP CONTEXT for M2M USER

config_prod = {
    'url': 'https://ooinet.oceanobservatories.org',
    'apiname': 'OOIAPI-XXXXX',
    'apikey': 'XXXXX'}
...
user = 'joe@whoi.edu'
```

### Machine to Machine Interface
The following class provides convenience methods for utilizing the M2M interface:

```python
class MachineToMachine(object):
    def __init__(self, base_url, api_user, api_key, username='danmergens@gmail.com'):
        self.base_url = base_url
        self.api_user = api_user
        self.api_key = api_key
        self.username = username
        self.auth = (api_user, api_key)
        self.inv_url = self.base_url + '/api/m2m/12576/sensor/inv'
        self.ingest_url = self.base_url + '/api/m2m/12589/ingestrequest'

    def get(self, url, params=None):  # TODO remove
        response = requests.get(url, auth=self.auth, params=params)
        if response.status_code != requests.codes.ok:
            print('request failed (%s) for %s - %s' % (response.reason, url, params))
            return None
        return response.json()

    def toc(self):
        """table of contents"""
        url = '/'.join((self.inv_url, 'toc'))
        return requests.get(url).json()

    def node_inventory(self, subsite, node):
        """returns sensors on the specified node"""
        url = '/'.join((self.inv_url, subsite, node))
        return ['-'.join((subsite, node, sensor)) for sensor in self.get(url)]

    def streams(self):
        """returns list of all streams"""
        toc = self.toc()
        stream_map = {}
        toc = toc['instruments']
        for row in toc:
            rd = row['reference_designator']
            for each in row['streams']:
                stream_map.setdefault(rd, {}).setdefault(each['method'], set()).add(each['stream'])
        return stream_map
    
    def instruments(self):
        nodes = []
        for subsite in self.get(self.inv_url):
            for node in self.get('/'.join((self.inv_url, subsite))):
                nodes.extend(self.node_inventory(subsite, node))
        return nodes
    
    def inv_request(self, refdes, method=None, stream=None, payload=None):
        subsite, node, inst = refdes.split('-', 2)
        url = '/'.join((self.inv_url, subsite, node, inst))
        if method:
            url = '/'.join((url, method))
            if stream:
                url = '/'.join((url, stream))
        return self.get(url, params=payload)
    
    def playback(self, payload):
        """create a request for playback ingest"""
        return requests.post(self.ingest_url, auth=(self.api_user, self.api_key), json=payload)
    
    def ingest_status(self, request_id):
        """get the current status of an ingest request"""
        response = requests.get('/'.join((self.ingest_url, str(request_id))), auth=self.auth)
        return response
        # if response.status_code == 200:
        #     return response.json()['status']
        # else:
        #     print('invalid ingest request: %d' % request_id)
        #     return None
        
    def ingest_jobs(self, request_id):
        """get the status of all jobs created by an ingest request"""
        payload = { 'ingestRequestId': request_id }
        response = requests.get('/'.join((self.ingest_url, 'jobs')), auth=self.auth, params=payload)
        return response

    def ingest_job_counts(self, request_id):
        """get overall status on files processed by an ingest request"""
        payload = { 'ingestRequestId': request_id, 'groupBy': 'status' }
        response = requests.get('/'.join((self.ingest_url, 'jobcounts')), auth=self.auth, params=payload)
        return response

    def purge(self, refdes):
        """purge all data for a particular reference designator""" # TODO add stream option
        print('Purging %s on %s' % (refdes, self.base_url))
        on_production = False
        if 'ooinet.oceanobservatories.org' in self.base_url:
            on_production = True
        subsite, node, sensor = refdes.split('-', 2)
        payload = {
            'username': self.username,
            'subsite': subsite,
            'node': node,
            'sensor': sensor
        }
        url = '/'.join((self.ingest_url, 'purgerecords'))
        if on_production:
            print('Cowardly refusing to purge data on production. Use the following information to purge:')
            print('curl --request PUT %s \\' % url)
            print('--data \'%s\'' % json.dumps(payload))
            return None
        response = requests.put(url, auth=self.auth, json=payload)

    def metadata_times(self, refdes):
        """fetch the stream metadata for an instrument"""
        subsite, node, inst = refdes.split('-', 2)
        return self.get('/'.join((self.inv_url, subsite, node, inst, 'metadata/times')))
    
    def availability(self, refdes):
        return self.get('/'.join((self.base_url, 'api/data_availability', refdes)))
    
    def context(self):
        return '%s %s' % (self.base_url, self.username)
```

### Configure M2M
Using your credentials, instantiate an M2M object for use:

```python
# configuration parameters must be valid credentials from the Data Portal of an authorized user, e.g.:
config = {
    'url': 'https://ooinet.oceanobservatories.org',
    'apiname': 'OOIAPI-XXXXX',
    'apikey': 'XXXXXXX'}
m2m = MachineToMachine(config['url'], config['apiname'], config['apikey'])
```

### Cabled Settings
The following settings identify the mapping of drivers and formats to specific instruments using the reference designators.
The playback format is typically `DATALOG` however, there are some instrument types that use other formats. The following 
will determine the playback format based on reference designator:

```python
playback_formats = {
    'BOTPT': 'CHUNKY',
    'D1000': 'CHUNKY',
    'VEL3D': 'CHUNKY',
    'ZPLSC': 'ZPLSC',
}

def playback_format(refdes):
    sensor = refdes.split('-')[3]
    sensor_type = sensor[0:5]
    file_format = playback_formats.get(sensor_type, None)
    # default format is DATALOG, however earlier raw datafiles can vary based on time (TODO)
    if not file_format:
        file_format = 'DATALOG'
    return file_format
```

E.g.:

```python
for refdes in ['CE02SHBP-LJ01D-05-ADCPTB104', 'RS03ASHS-MJ03B-09-BOTPTA304']:
    print(refdes, playback_format(refdes))
```
```
CE02SHBP-LJ01D-05-ADCPTB104 DATALOG
RS03ASHS-MJ03B-09-BOTPTA304 CHUNKY
```

The cabled instrument driver code needs to be provided for the playback and is mapped as follows:

```python
parser_drivers = {
    'ADCPS': 'mi.instrument.teledyne.workhorse.adcp.driver',
    'ADCPT': 'mi.instrument.teledyne.workhorse.adcp.driver',
    'BOTPT': 'mi.instrument.noaa.botpt.ooicore.driver',
    'CTDBP': 'mi.instrument.seabird.sbe16plus_v2.ctdbp_no.driver',
    'CTDPFA': 'mi.instrument.seabird.sbe16plus_v2.ctdpf_sbe43.driver',
    'CTDPFB': 'mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver',
    'D1000': 'mi.instrument.mclane.ras.d1000.driver',
    'FLORD': 'mi.instrument.wetlabs.fluorometer.flort_d.driver',
    'FLORT': 'mi.instrument.wetlabs.fluorometer.flort_d.driver',
    'HPIES': 'mi.instrument.uw.hpies.ooicore.driver',
    'NUTNR': 'mi.instrument.satlantic.suna_deep.ooicore.driver',
    'OPTAA': 'mi.instrument.wetlabs.ac_s.ooicore.driver',
    'PARAD': 'mi.instrument.satlantic.par_ser_600m.driver',
    'PCO2WA': 'mi.instrument.sunburst.sami2_pco2.pco2a.driver',
    'PCO2WB': 'mi.instrument.sunburst.sami2_pco2.pco2b.driver',
    'PHSEN': 'mi.instrument.sunburst.sami2_ph.ooicore.driver',
    'PREST': 'mi.instrument.seabird.sbe54tps.driver',
    'SPKIR': 'mi.instrument.satlantic.ocr_507_icsw.ooicore.driver',
    'TMPSF': 'mi.instrument.rbr.xr_420_thermistor_24.ooicore.driver',
    'TRHPH': 'mi.instrument.uw.bars.ooicore.driver',
    'VEL3DB': 'mi.instrument.nobska.mavs4.ooicore.driver',
    'VEL3DC': 'mi.instrument.nortek.vector.ooicore.driver',
    'VELPT': 'mi.instrument.nortek.aquadopp.ooicore.driver',
    'CE04OSPS-PC01B-4A-CTDPFA109': 'mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver',
    'RS01SBPS-PC01A-4A-CTDPFA103': 'mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver',
    'RS01SBPS-PC01A-06-VADCPA101MAIN': 'mi.instrument.teledyne.workhorse.vadcp.playback4',
    'RS01SBPS-PC01A-06-VADCPA101-5TH': 'mi.instrument.teledyne.workhorse.vadcp.playback5',
}

def parser_driver(refdes):
    # check for exceptional case first (uses entire refdes)
    driver = parser_drivers.get(refdes, None)
    if not driver:
        sensor = refdes.split('-')[3]
        sensor_type = sensor[0:5]
        driver = parser_drivers.get(sensor_type, None)
        if driver is None:
            sensor_type = sensor[0:6]
            driver = parser_drivers.get(sensor_type, None)
    return driver
```

E.g.:
```python
print(parser_driver('RS01SBPS-SF01A-4F-PCO2WA101'))
print(parser_driver('CE02SHBP-LJ01D-09-PCO2WB103'))
print(parser_driver('RS03INT2-MJ03D-12-VEL3DB304'))
print(parser_driver('CE04OSPS-PC01B-4A-CTDPFA109'))  # 'ctdpf_optode_sample'
print(parser_driver('RS01SBPS-PC01A-4A-CTDPFA103'))  # 'ctdpf_optode_sample'
print(parser_driver('RS01SBPS-PC01A-06-VADCPA101MAIN'))
print(parser_driver('RS01SBPS-PC01A-06-VADCPA101-5TH'))
```
```
mi.instrument.sunburst.sami2_pco2.pco2a.driver
mi.instrument.sunburst.sami2_pco2.pco2b.driver
mi.instrument.nobska.mavs4.ooicore.driver
mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver
mi.instrument.seabird.sbe16plus_v2.ctdpf_jb.driver
mi.instrument.teledyne.workhorse.vadcp.playback4
mi.instrument.teledyne.workhorse.vadcp.playback5
```

Creation of the playback payload uses the format, driver, and data ranges. Here is a class that will create the payload for the ingestion request:

```python
def cabledRequestFactory(username, refdes, filemasks, file_range=None, data_range=None, force=False, priority=5, 
                         max_files=None):
    subsite, node, sensor = refdes.split('-', 2)
    request = {
        'username': username,
        'state': 'RUN',
        'type': 'PLAYBACK',  # 'RECOVERED', 'TELEMETERED', 'PLAYBACK'
        'options': {
        },
        'ingestRequestFileMasks': [],
    }
    request['priority'] = priority
    if max_files:
        request['maxNumFiles'] = max_files
    request['options']['format'] = playback_format(refdes)

    driver = parser_driver(refdes)
    if driver is None:
        print('unable to find driver for sensor: ', refdes)
        return None

    for mask in filemasks:
        request['ingestRequestFileMasks'].append(
        {
            'dataSource': 'streamed',
            'refDes': {
                'full': True,
                'node': node,
                'sensor': sensor,
                'subsite': subsite,
            },
            'refDesFinal': True,
            'parserDriver': driver,
            'fileMask': mask,
            'deployment': 0,  # always 0 for cabled playback
        })

    if file_range:
        request['options']['beginFileDate'] = file_range[0]
        request['options']['endFileDate'] = file_range[1]

    if data_range:
        request['options']['beginData'] = data_range[0]
        request['options']['endData'] = data_range[1]
    
    if force:
        request['options']['checkExistingFiles'] = False
    
    return request
```

Note that the class will require the data range
## Playback
Execute playback using the following steps:
1. Establish the Data Gap
1. Cnfigure Playback
1. Test Playback
1. Verify Playback
1. Execute Playback
1. Verify Playback

### Establish the Data Gap
For system-wide outage, an estimate of the gap is known, (e.g. July 26-July 29). However, you can get a more precise value
by using the data availability service:

```python
### DETERMINE DATA GAP
avail = m2m.availability('CE02SHBP-LJ01D-06-CTDBPN106')
dp_avail = [x['data'] for x in avail['availability'] if 'Data Products' in x['measure']][0]
[x for x in dp_avail if 'Missing' in x and '2019-07-26' in x[0]]
```
The result will be the gap(s) starting on the provided date to the nearest second. As some sensors produce data points more frequently than that, this will result in some redundant data points being added to the database. Note that stream engine will filter out duplicate data points (data for the same time point) and return only one value for the time (although it is not deterministic which point will be returned in cases where the associated data differs). In the case where it is determined that previous data are invalid, they should be purged from the system and reingested. 

Note here that we provided a start date for the known gap and then filtered the results to show just that gap. This same 
command can be used after the data are ingested to confirm the data gap has been resolved, although it is recommended that 
you check using the data plotting interface of OOINet which will provide visual confirmation. 

### Configure Playback

To construct the playback command, the data range, associated date range and file masks are passed to the request generator. A few items to note:

* In the file range, which specifies the limit of files to be considered for parsing, second date is exclusive (it is the day after the end date of the range).
* For the file mask, dates before July 2016 have a different location and therefore a different mask is required.

E.g.: 



### Test Playback
### Verify Playback
### Execute Playback
### Verify Playback
