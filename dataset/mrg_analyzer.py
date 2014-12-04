#!/usr/bin/env python
import time

__author__ = 'pcable'

import sys
import pprint

particle_map = {
    'sent_data': {'sci_x_sent_data_files'},
    'disk_status': {'sci_m_disk_free', 'sci_m_disk_usage', 'sci_x_disk_files_removed'},
    'ctd': {'sci_water_cond', 'sci_water_pressure', 'sci_water_temp'},
    'optode': {'sci_oxy4_oxygen', 'sci_oxy4_saturation'},
    'flort': {'sci_flbbcd_bb_units', 'sci_flbbcd_cdom_units', 'sci_flbbcd_chlor_units'},
    'parad': {'sci_bsipar_par'}
}

def analyze(filename):
    fh = open(filename)
    data = fh.readlines()
    data = [x.strip() for x in data]

    header = data[:14]

    header = {x.split(':')[0].strip(): x.split(':')[1].strip() for x in header}
    keys = data[14].split()
    units = data[15].split()
    sizes = data[16].split()
    data = data[17:]

    record_list = []

    for record in data:
        d = {}
        parts = record.split()
        for i,x in enumerate(parts):
            if x != 'NaN':
                key = keys[i]
                d[key] = x
        record_list.append(d)

    sci_dict = {}
    eng_dict = {}
    eng_particles = []

    eng_times = {'m_present_secs_into_mission', 'm_present_time'}
    sci_times = {'sci_m_present_secs_into_mission', 'sci_m_present_time'}
    for record in record_list:
        keyset = set(record.keys())

        if sci_times.issubset(keyset):
            for particle_name, particle_key_set in particle_map.items():
                if particle_key_set.issubset(keyset):
                    d = {}
                    for key in sci_times.union(particle_key_set):
                        d[key] = record.get(key)
                    sci_dict.setdefault(particle_name, []).append(d)


        if len(keyset.intersection(eng_times)) > 0:
            eng_particles.append(record)

        # if sci_times in keyset:
        #     # science data
        #     keys = tuple(sorted(list(keyset-IGNORE)))
        #     sci_dict.setdefault(key, []).append(record)
        # else:
        #     key = tuple(sorted(list(keyset-IGNORE)))
        #     eng_dict.setdefault(key, []).append(record)

    pprint.pprint(header)
    print 'science particles:'
    for key in sci_dict:
        times = [float(x.get('sci_m_present_time')) for x in sci_dict[key]]
        min_time = min(times)
        max_time = max(times)
        print '    %-16s: %6d First: %20s Last: %20s' % (key, len(sci_dict[key]), time.ctime(min_time), time.ctime(max_time))

    print 'eng data particles: %d' % len(eng_particles)



def main():
    filename = sys.argv[1]
    analyze(filename)

if __name__ == '__main__':
    main()