#!/usr/bin/env python

# OPTAA calibration parser
#
# Create the necessary CI calibration ingest information from an OPTAA calibration file

import csv
import json
import os
import sys


class NutnrCalibration:
    def __init__(self, lower=217, upper=240):
        self.cal_temp = None
        self.wavelengths = []
        self.eno3 = []
        self.eswa = []
        self.di = []
        self.lower_limit = lower
        self.upper_limit = upper

    def read_cal(self, filename):
        with open(filename) as fh:
            for line in fh:
                parts = line.split(',')

                if len(parts) < 2:
                    continue  # skip anything that is not key value paired
                record_type = parts[0]
                if record_type == 'H':
                    key_value = parts[1].split()
                    if len(key_value) == 2:
                        name, value = key_value
                        if name == 'T_CAL':
                            self.cal_temp = float(value)
                elif record_type == 'E':
                    _, wavelength, eno3, eswa, _, di = parts
                    self.wavelengths.append(float(wavelength))
                    self.eno3.append(float(eno3))
                    self.eswa.append(float(eswa))
                    self.di.append(float(di))

    def write_cal_info(self, fileroot):

        with open('%s.csv' % fileroot, 'w') as info:
            writer = csv.writer(info)
            writer.writerow(['CC_cal_temp', self.cal_temp])
            writer.writerow(['CC_wl', json.dumps(self.wavelengths)])
            writer.writerow(['CC_eno3', json.dumps(self.eno3)])
            writer.writerow(['CC_eswa', json.dumps(self.eswa)])
            writer.writerow(['CC_di', json.dumps(self.di)])
            writer.writerow(['CC_lower_wavelength_limit_for_spectra_fit', self.lower_limit])
            writer.writerow(['CC_upper_wavelength_limit_for_spectra_fit', self.upper_limit])


def usage():
    print 'Usage: %s <filename> [lower limit] [upper limit]' % sys.argv[0]
    sys.exit(1)


def main():
    if len(sys.argv) < 2 or len(sys.argv) > 4:
        usage()

    if not os.path.exists(sys.argv[1]):
        print 'Error filename does not exist: %s' % sys.argv[1]

    if len(sys.argv) == 3:
        if not isinstance(sys.argv[2], int):
            usage()
        cal = NutnrCalibration(sys.argv[2])
    elif len(sys.argv) == 4:
        if not isinstance(sys.argv[2], int) or not isinstance(sys.argv[3], int):
            usage()
        cal = NutnrCalibration(sys.argv[2], sys.argv[3])
    else:
        cal = NutnrCalibration()

    cal.read_cal(sys.argv[1])
    cal.write_cal_info('suna-calinfo')


if __name__ == '__main__':
    main()
