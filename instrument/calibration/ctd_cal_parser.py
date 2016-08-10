#!/usr/bin/env python

# CTD calibration parser
#
# Create the necessary CI calibration ingest information from an CTD calibration file

import csv
import os
import sys


class CtdCalibration:
    def __init__(self):
        self.coefficient_name_map = {
            'TA0': 'CC_a0',
            'TA1': 'CC_a1',
            'TA2': 'CC_a2',
            'TA3': 'CC_a3',
            'CPCOR': 'CC_cpcor',
            'CTCOR': 'CC_ctcor',
            'CG': 'CC_g',
            'CH': 'CC_h',
            'CI': 'CC_i',
            'CJ': 'CC_j',
            'PA0': 'CC_pa0',
            'PA1': 'CC_pa1',
            'PA2': 'CC_pa2',
            'PTEMPA0': 'CC_ptempa0',
            'PTEMPA1': 'CC_ptempa1',
            'PTEMPA2': 'CC_ptempa2',
            'PTCA0': 'CC_ptca0',
            'PTCA1': 'CC_ptca1',
            'PTCA2': 'CC_ptca2',
            'PTCB0': 'CC_ptcb0',
            'PTCB1': 'CC_ptcb1',
            'PTCB2': 'CC_ptcb2',
            # additional types for series O
            'C1': 'CC_C1',
            'C2': 'CC_C2',
            'C3': 'CC_C3',
            'D1': 'CC_D1',
            'D2': 'CC_D2',
            'T1': 'CC_T1',
            'T2': 'CC_T2',
            'T3': 'CC_T3',
            'T4': 'CC_T4',
            'T5': 'CC_T5',
        }

        # dictionary with calibration coefficient names and values
        self.coefficients = {}

    def read_cal(self, filename):
        with open(filename) as fh:
            for line in fh:
                parts = line.split('=')

                if len(parts) != 2:
                    continue  # skip anything that is not key value paired

                key = parts[0]
                value = parts[1].strip()

                if key == 'INSTRUMENT_TYPE' and value != 'SEACATPLUS':
                    print 'Error - unexpected type calibration file (%s != SEACATPLUS)' % value
                    sys.exit(1)

                name = self.coefficient_name_map.get(key)
                if name is None:
                    continue

                self.coefficients[name] = value

    def write_cal_info(self, filename):

        with open(filename, 'w') as info:
            writer = csv.writer(info)
            for each in sorted(self.coefficients.items()):
                writer.writerow(each)


def usage():
    print 'Usage: %s <filename>' % sys.argv[0]
    sys.exit(1)


def main():
    if len(sys.argv) < 2:
        usage()

    if not os.path.exists(sys.argv[1]):
        print 'Error filename does not exist: %s' % sys.argv[1]

    cal = CtdCalibration()

    cal.read_cal(sys.argv[1])
    cal.write_cal_info('ctd-calinfo.csv')


if __name__ == '__main__':
    main()
