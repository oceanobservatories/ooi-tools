#!/usr/bin/env python

# CTD calibration parser
#
# Create the necessary CI calibration ingest information from an CTD calibration file

import csv
import os
import sys


class SBE43Calibration:
    def __init__(self):
        self.coefficient_name_map = {
            'E': 'CC_residual_temperature_correction_factor_e',
            'C': 'CC_residual_temperature_correction_factor_c',
            'VOFFSET': 'CC_voltage_offset',  # note that this was previously called CC_frequency_offset
            'SOC': 'CC_oxygen_signal_slope',
            'A': 'CC_residual_temperature_correction_factor_a',
            'B': 'CC_residual_temperature_correction_factor_b',
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

                if key == 'INSTRUMENT_TYPE' and value != 'SBE43':
                    print 'Error - unexpected type calibration file (%s != SBE43)' % value
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

    cal = SBE43Calibration()

    cal.read_cal(sys.argv[1])
    cal.write_cal_info('sbe43-calinfo.csv')


if __name__ == '__main__':
    main()
