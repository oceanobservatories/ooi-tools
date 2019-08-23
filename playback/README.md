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

Note here that we provided a start date for the known gap and then filtered the results to show just that gap. This same 
command can be used after the data are ingested to confirm the data gap has been resolved, although it is recommended that 
you check using the data plotting interface of OOINet which will provide visual confirmation. 

### Cnfigure Playback
### Test Playback
### Verify Playback
### Execute Playback
### Verify Playback
