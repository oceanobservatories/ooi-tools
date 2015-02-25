# mdd.py:
# Parse an mdd file
# Revision history:
# 2aug2013	dpingal@teledyne.com	Fix segment lengths, zero fills in wrong place
# 8aug2013	dpingal@teledyne.com	Fix bug parsing fileopen_time with single digit day
# 5jun2014	dpingal@teledyne.com	Fix compatability with 7.14 glider firmware
# 9dec2014  ehahn@bbn.com           Add sio parsing into individual instrument group files
# 24feb2015 ehahn@bbn.com           Fixed handling old and new .mdd header format

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
            # find and set tag values for the next group, overwriting previous values
            offset = self.get_next_tag_group(offset)
            if offset is None:
                # no more tags were found, this is the end of the file
                break

            # Need all tags to have been set, if one has not been set yet this section cannot be processed
            if self.port is not None and self.node is not None and self.endoffset is not None and \
                    self.startoffset is not None:
                dlen = 1 + self.endoffset - self.startoffset
                sdata = self.data[offset:offset + dlen]
                s = mdd_data.data_section(self.node, self.port, self.startoffset, self.endoffset, sdata)
                self.sections.append(s)

    def get_next_tag_group(self, offset):
        """
        find a group of tags on consecutive lines and set their values, which must include start and end offset tags
        :param offset: The input offset to start searching at
        """

        # make an array to keep track of tag start indices
        tag_idx = []
        found_start = False
        found_end = False

        # loop over possible tags, storing their next starting index if found
        for tag in MDD_TAGS:
            found_idx = self.data.find(tag, offset)
            if found_idx != -1:
                # store the index of the start of the tag line that has been identified
                tag_idx.append(found_idx)
                # start and end are required, make sure they are in the group
                if tag is MDD_TAGS[2]:
                    found_start = True
                elif tag is MDD_TAGS[3]:
                    found_end = True

        if tag_idx and found_start and found_end:
            # tags were found, including both start and end tags
            start_line = min(tag_idx)

            # loop over the tags and set their values for the tags in this group
            while start_line in tag_idx:
                end_line = self.set_tag_value(start_line)
                # add one to get to the start of the next line
                start_line = end_line + 1

            # return the start of the line following the tag group
            return start_line

        # no tags were found
        return None

    def set_tag_value(self, tag_start):
        """
        Retrieve tag name and value from a line known to contain a tag, and set the corresponding tag variable value
        :param tag_start: The starting index of the tag line
        """

        # find the index of the end of the tag lin
        end_line = self.data.find('\n', tag_start)
        # split the line into tag and value
        (tag, value) = self.data[tag_start:end_line].split(':')
        # format tag and value
        tag = tag.lower()
        value = int(value.strip())
        # set the tag value
        self.__setattr__(tag, value)
        # return the end of the line
        return end_line

    def gettag(self, tag):
        """
        Get the value associated with the input tag
        :param tag: The text of the input tag to locate the value for
        """
        # find first occurence of this tag
        found = self.data.find(tag + ':') + len(tag) + 1
        # find next new line after tag
        end = self.data.find('\n', found)
        return self.data[found:end].strip()


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

