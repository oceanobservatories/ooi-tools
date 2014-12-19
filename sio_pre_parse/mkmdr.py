# mkmdr.py:
# output a .mdr file for a particular mooring
# 08jul2013 dpingal@teledyne.com  Initial
# 04sep2013 dpingal@teledyne.com  added starting offset

import mdd_data
import os

final = 9999999
db = mdd_data.mdd_data()
sects = db.sects()

class mdrfile(object):
    def __init__(self, fn):
        self.file = open(fn, 'w')
    
    def sect(self, start, end, port):
        self.file.write('STARTOFFSET: %d\n' % start)
        self.file.write('ENDOFFSET: %d\n' % end)
        self.file.write('PORT: %d\n' % port)
        
    def close(self):
        self.file.close()
    
def port(f, node, port, minval = 0, maxval = final):
    posn = minval
    for s in sects:
        if s.node != node or s.port != port:
            continue
        
        if s.start > posn:
            f.sect(posn, s.start, port)
        posn = max(minval, s.end + 1)
    f.sect(posn, maxval, port)

def mdr(node, path):
    ofile = mdrfile(os.path.join(path, str(node) + '.mdr'))
    start = db.offsets().setdefault(node, 0)
    port(ofile, node, 1, start)
    port(ofile, node, 2)
    ofile.close()

def genmdr(node, path, max):
    ofile = mdrfile(os.path.join(path, str(node) + '.mdr'))
    start = db.offsets().setdefault(node, 0)
    port(ofile, node, 1, start, max)
    ofile.close()

def genmdrs(path, nodes):
    for (id, max) in nodes.items():
        genmdr(id, path, max)

if __name__ == '__main__':
    import sys
    mdr(int(sys.argv[1]), '.')