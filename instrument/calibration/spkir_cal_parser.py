#!/usr/bin/env python

# SPKIR calibration parser
#
# Create the necessary CI calibration ingest information from an SPKIR calibration file

import csv
import json
import os
import sys


class SpkirCalibration:
    def __init__(self):
        self.offset = []
        self.scale = []
        self.immersion_factor = []

    def read_cal(self, filename):
        with open(filename) as fh:
            read_record = False  # indicates next line is record we want to read
            for line in fh:
                if line[0] == '#':  # skip comments
                    continue

                parts = line.split()
                if not len(parts):  # skip blank lines
                    continue

                if parts[0] == 'ED':
                    read_record = True
                    continue

                if read_record:
                    if len(parts) == 3:  # only parse if we have all the data
                        offset, scale, factor = parts
                        self.offset.append(float(offset))
                        self.scale.append(float(scale))
                        self.immersion_factor.append(float(factor))
                        read_record = False

    def write_cal_info(self, fileroot):

        with open('%s.csv' % fileroot, 'w') as info:
            writer = csv.writer(info)
            writer.writerow(['CC_offset', json.dumps(self.offset)])
            writer.writerow(['CC_scale', json.dumps(self.scale)])
            writer.writerow(['CC_immersion_factor', self.immersion_factor])


def main():
    if len(sys.argv) < 2:
        print 'Usage: %s <filename>' % sys.argv[0]
        sys.exit(1)

    if not os.path.exists(sys.argv[1]):
        print 'Error filename does not exist: %s' % sys.argv[1]

    cal = SpkirCalibration()
    cal.read_cal(sys.argv[1])
    cal.write_cal_info('spkir-calinfo')


if __name__ == '__main__':
    main()
