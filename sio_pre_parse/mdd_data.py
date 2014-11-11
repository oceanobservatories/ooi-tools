# mdd_data.py:
# Persistent data object for mdd data processor
# 08jul2013 dpingal@teledyne.com  Initial
# 04sep2013 dpingal@teledyne.com  only one database object, added offsets

import mdd_config
import os
import pickle

# Where we save data
dbfile = mdd_config.datafile('mdd.pckl')
db = None

class matrix(object):
    def __init__(self):
        self.x = {}
        self.y = set()
    
    # Increment value at (xkey, ykey, tag) by increment
    def accumulate(self, xkey, ykey, tag, increment = 1):
        if xkey not in self.x:
            self.x[xkey] = {}
        column = self.x[xkey]
        if ykey not in column:
            column[ykey] = {}
        loc = column[ykey]
        if ykey not in self.y:
            self.y.add(ykey)
        if tag not in loc:
            loc[tag] = 0
        loc[tag] += increment

   # Increment value at (xkey, ykey, tag) by increment
    def max(self, xkey, ykey, tag, value):
        if xkey not in self.x:
            self.x[xkey] = {}
        column = self.x[xkey]
        if ykey not in column:
            column[ykey] = {}
        loc = column[ykey]
        if ykey not in self.y:
            self.y.add(ykey)
        if tag not in loc:
            loc[tag] = 0
        if value > loc[tag]:
            loc[tag] = value

    def get(self, xkey, ykey, tag):
        if xkey not in self.x or ykey not in self.x[xkey]:
            return 0
        return self.x[xkey][ykey][tag]
    
    def xkeys(self):
        return self.x.keys()

    def ykeys(self):
        return self.y

class data_section(object):
    def __init__(self, node, port, start, end, data):
        self.node = node
        self.port = port
        self.start = start
        self.end = end
        self.data = data
    def __repr__(self):
        return 'data_section node: ' + str(self.node) + ' port: ' + str(self.port) \
                + ' start: ' + str(self.start) + ' end: ' + str(self.end)

class mdddb(object):
    def __init__(self):
        self.stats = matrix()
        self.sects = []
        self.offsets = {}
                
class mdd_data(object):
    # There is only one actual database, ever
    db = None
    def __init__(self):
        if not mdd_data.db:
            try:
                mdd_data.db = pickle.load(open(dbfile))
            except IOError:
                mdd_data.db = mdddb()

    def save(self):
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            tempfn = os.tempnam(mdd_config.data_path, 'mdd.')
        pickle.dump(mdd_data.db, open(tempfn, 'w'))
        os.rename(tempfn, dbfile)

    def reset(self):
        mdd_data.db.stats = matrix()
        
    def stats(self):
        return mdd_data.db.stats
    
    def sects(self):
        return mdd_data.db.sects
    
    def offsets(self):
        # Migrate old data: add member if its not there
        try:
            return mdd_data.db.offsets
        except AttributeError:
            mdd_data.db.offsets = {}
            return mdd_data.db.offsets
 

