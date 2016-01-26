import os
import fnmatch
from obspy import read
from obspy.core.utcdatetime import UTCDateTime

#slice time, in seconds
SLICE_TIME = 3600 * 24
SAMPLING_RATE = 1.0
SAN = '/san_data'


def slice_mseed_file(root, file):

    st = read(file)

    # each stream will contain a single trace object
    if len(st) <= 0:
        return False

    # We only want to slice files for instruments sampling at 1 Hz
    tr = st[0]
    rate = tr.stats.sampling_rate

    if rate != SAMPLING_RATE:
        return False

    start = tr.stats.starttime
    end = tr.stats.endtime

    if end.day == start.day:
        # no need to slice
        return False

    print "Original trace: %s" % tr

    while start < end:

        # slice needs to end at the end of the day!
        if end.day > start.day:
            cutoff = UTCDateTime(start.year, start.month, start.day, 23, 59, 59)
            print "end day greater than start day!"
        else:
            cutoff = start + SLICE_TIME - 1
            print "start day and end day are same"

        if cutoff > end:
            cutoff = end

        slice = tr.slice(start, cutoff)
        print "Slice: %s " % slice

        # save the slice
        fname = slice.id.replace('.', '-') + '_' + str(slice.stats.starttime)[:-1] + '.mseed'

        print fname

        # write out the sliced file
        slice.write(os.path.join(root, fname), format="MSEED")

        start = UTCDateTime(cutoff + 1)

    return True

out = open('modified_mseed_files.txt', 'w')

for root, dirs, files in os.walk(SAN):

    for fname in fnmatch.filter(files, '*.mseed'):

        if slice_mseed_file(root, os.path.join(root, fname)):
            out.write(os.path.join(root, fname) + '\n')
