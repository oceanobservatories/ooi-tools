#!/usr/bin/env python

# OPTAA calibration parser
#
# Create the necessary CI calibration ingest information from an OPTAA calibration file

import csv
import json
import os
import sys


class OptaaCalibration:
    def __init__(self, serial_number):
        self.serial_number = serial_number
        self.cwlngth = []
        self.awlngth = []
        self.tcal = None
        self.tbins = None
        self.ccwo = []
        self.acwo = []
        self.tcarray = []
        self.taarray = []
        self.nbins = None  # number of temperature bins

    def read_acs(self, filename):
        with open(filename) as fh:
            for line in fh:
                parts = line.split(';')
                if len(parts) != 2:
                    parts = line.split()
                    if parts[0] == '"tcal:':
                        self.tcal = parts[1]
                    continue
                data, comment = parts

                if comment.startswith(' temperature bins'):
                    self.tbins = data.split()
                    self.tbins = [float(x) for x in self.tbins]

                elif comment.startswith(' number of temperature bins'):
                    self.nbins = int(data)

                elif comment.startswith(' C and A offset'):
                    if self.nbins is None:
                        print 'Error - failed to read number of temperature bins'
                        sys.exit(1)
                    parts = data.split()
                    self.cwlngth.append(float(parts[0][1:]))
                    self.awlngth.append(float(parts[1][1:]))
                    self.ccwo.append(float(parts[3]))
                    self.acwo.append(float(parts[4]))
                    tcrow = [float(x) for x in parts[5:self.nbins+5]]
                    tarow = [float(x) for x in parts[self.nbins+5:2*self.nbins+5]]
                    self.tcarray.append(tcrow)
                    self.taarray.append(tarow)

    def write_cal_info(self):

        if self.serial_number is None:
            print 'Error - serial number not specified - exiting'
            sys.exit(1)

        with open('ACS-%s_calinfo.csv' % self.serial_number, 'w') as info:
            writer = csv.writer(info)
            writer.writerow(['CC_cwlngth', json.dumps(self.cwlngth)])
            writer.writerow(['CC_ccwo', json.dumps(self.ccwo)])
            writer.writerow(['CC_tcal', self.tcal])
            writer.writerow(['CC_tbins', json.dumps(self.tbins)])
            writer.writerow(['CC_awlngth', json.dumps(self.awlngth)])
            writer.writerow(['CC_acwo', json.dumps(self.acwo)])
            writer.writerow(['CC_tcarray', 'SheetRef:ACS-%s_CC_tcarray' % self.serial_number])
            writer.writerow(['CC_taarray', 'SheetRef:ACS-%s_CC_taarray' % self.serial_number])

        def write_array(filename, cal_array):
            with open(filename, 'w') as out:
                array_writer = csv.writer(out)
                array_writer.writerows(cal_array)

        write_array('ACS-%s_CC_tcarray.csv' % self.serial_number, self.tcarray)
        write_array('ACS-%s_CC_taarray.csv' % self.serial_number, self.taarray)


def main():
    if len(sys.argv) < 3:
        print 'Usage: %s <filename> <serial number>' % sys.argv[0]
        sys.exit(1)

    if not os.path.exists(sys.argv[1]):
        print 'Error filename does not exist: %s' % sys.argv[1]

    cal = OptaaCalibration(sys.argv[2])
    cal.read_acs(sys.argv[1])
    cal.write_cal_info()


if __name__ == '__main__':
    main()
