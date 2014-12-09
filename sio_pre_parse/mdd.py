# mdd.py:
# Parse an mdd file
# Revision history:
# 2aug2013	dpingal@teledyne.com	Fix segment lengths, zero fills in wrong place
# 8aug2013	dpingal@teledyne.com	Fix bug parsing fileopen_time with single digit day
# 5jun2014	dpingal@teledyne.com	Fix compatability with 7.14 glider firmware
# 9dec2014  ehahn@bbn.com           Add sio parsing into individual instrument group files

import calendar
import mdd_config
import mdd_data
import re
import time

from sio_unpack import SioParse

subunder = re.compile('_+')

MDD_TAGS = ['NODE:', 'PORT:', 'STARTOFFSET:', 'ENDOFFSET:']


class mdd(object):
    def __init__(self, fn):
        self.data = open(fn, 'rb').read()
        offset = 0
        self.sections = []
        self.glider = self.gettag('full_filename').split('-')[0]
        asctime = self.gettag('fileopen_time')
        wstime = subunder.sub(' ', asctime)
        self.time = calendar.timegm(time.strptime(wstime, '%a %b %d %H:%M:%S %Y'))
        # initialize tags
        self.endoffset = None
        self.startoffset = None
        self.node = None
        self.port = None
        while True:
            (tag, value, offset) = self.nextag(offset)
            if not tag:
                break
            self.__setattr__(tag, value)
            # once all tags have been set, this header has been read and section is complete
            if self.port is not None and self.node is not None and \
                    self.endoffset is not None and self.startoffset is not None:
                dlen = 1 + self.endoffset - self.startoffset
                sdata = self.data[offset:offset + dlen]
                s = mdd_data.data_section(self.node, self.port, self.startoffset, self.endoffset, sdata)
                self.sections.append(s)
                # clear tags for next increment, node and port might not be present
                self.endoffset = None
                self.startoffset = None

    def gettag(self, tag):
        # find first occurence of this tag
        found = self.data.find(tag + ':') + len(tag) + 1
        # find next new line after tag
        end = self.data.find('\n', found)
        return self.data[found:end].strip()

    def nextag(self, offset=0):
        # search the data and find the next tag starting at offset
        tag = None
        value = None
        end = 0
        min_found = len(self.data)
        # loop over MDD tags, updating with min tag index
        for seek in MDD_TAGS:
            found_idx = self.data.find(seek, offset)
            if 0 <= found_idx < min_found:
                min_found = found_idx

        if min_found != len(self.data):
            # get the index of the end of the tag line
            end = self.data.find('\n', min_found)
            # pull out value from tag and value line
            (tag, value) = self.data[min_found:end].split(':')
            # format tag and value
            tag = tag.lower()
            value = int(value.strip())

        return tag, value, end + 1


def procall(fns):
    """
    Process a list of .mdd files
    :param fns: List of .mdd files to process
    :return: sections
    """
    # Prepare object to collect data into
    db = mdd_data.mdd_data()
    db.reset()
    sects = db.sects()
    stats = db.stats()
    changed_files = []
    
    # Ingest all sections in all input files
    for fn in fns:
        d = mdd(fn)
        for sect in d.sections:
            sect.glider = d.glider
            sect.time = d.time
            #print fn, sect.node, sect.start, sect.end
            # Basic validation: we know gliders make these...
            if sect.end <= sect.start:
                #print 'start > end?? node %d port %d start %d end %d' % (sect.node, sect.port, sect.start, sect.end)
                continue
            # Create new or open existing output file
            filename = 'node%dp%d.dat' % (sect.node, sect.port)
            ofn = mdd_config.datafile(filename)
            # keep track of which port 1 node files change so sio block parsing can be done on them
            if sect.port == 1 and filename not in changed_files:
                changed_files.append(filename)
            try:
                of = open(ofn, 'r+b')
            except IOError:
                of = open(ofn, 'wb')
            # If we are past end of file, fill with zeroes
            of.seek(0, 2)
            if sect.start > of.tell():
                of.write('\0' * (sect.start - of.tell()))
            # Write each section out at its address
            of.seek(sect.start)
            of.write(sect.data)
            of.close()
            # Keep metadata for what we have processed
            sects.append(sect)
            stats.accumulate(sect.node, sect.glider, 'bytes', 1 + sect.end - sect.start)
            stats.max(sect.node, sect.glider, 'last', sect.time)
        
    # Merge adjacent sections into one, start sorted by node/port/start
    sects.sort(lambda a, b: cmp(a.node, b.node) or cmp(a.port, b.port) or cmp(a.start, b.start))
    n = 0
    while n < len(sects) - 1:
        curr = sects[n]
        # Merge subsequent sections into this one until we can't anymore
        while n < len(sects) - 1:
            next_sect = sects[n + 1]
            if curr.node != next_sect.node or curr.port != next_sect.port:
                break
            elif curr.end < next_sect.start - 1:
                break
            curr.end = max(curr.end, next_sect.end)
            curr.time = max(curr.time, next_sect.time)
            del sects[n + 1]
        n += 1
    #print '\n'.join([repr(s) for s in sects])
    db.save()

    # the following section of code was added to parse the initially created node files,
    # locate complete sio blocks, and copy those into fixed instrument specific files
    sio_parse = SioParse()

    # loop over each node file that has changed with this run and parse it
    for changed_file in changed_files:
        sio_parse.parse_file(changed_file)

    # save the sio parse database
    sio_parse.save()

    return sects

if __name__ == '__main__':
    import sys
    procall(sys.argv[1:])

