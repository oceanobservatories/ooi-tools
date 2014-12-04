#!/usr/bin/env python
"""

    Usage:
        ./mrg_analyzer <file>...

"""
import time
import sys
import pprint
import docopt

__author__ = 'pcable'


particle_map = {
    'sent_data': {'sci_x_sent_data_files'},
    'disk_status': {'sci_m_disk_free', 'sci_m_disk_usage', 'sci_x_disk_files_removed'},
    'ctd': {'sci_water_cond', 'sci_water_pressure', 'sci_water_temp'},
    'dosta': {'sci_oxy4_oxygen', 'sci_oxy4_saturation'},
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


        else:
            eng_particles.append(record)

        # if sci_times in keyset:
        #     # science data
        #     keys = tuple(sorted(list(keyset-IGNORE)))
        #     sci_dict.setdefault(key, []).append(record)
        # else:
        #     key = tuple(sorted(list(keyset-IGNORE)))
        #     eng_dict.setdefault(key, []).append(record)

    return header, sci_dict, eng_particles


def gap_analysis(diffs, diff_stamps, gap_size):
    gaps = []
    for i, diff in enumerate(diffs):
        if diff > gap_size:
            gaps.append((diff, diff_stamps[i][0], diff_stamps[i][1]))
    gaps.sort()
    gaps.reverse()
    return gaps


def stream_stats(records, gap_threshold_percentage):
    if 'sci_m_present_time' in records[0]:
        time_key = 'sci_m_present_time'
    else:
        time_key = 'm_present_time'

    times = []
    for record in records:
        try:
            t = float(record.get(time_key))
            times.append(t)
        except TypeError:
            print 'ERROR: %s not a float (%s)' % (record.get(time_key), time_key)

    times.sort()
    diffs = []
    diff_stamps = []
    last_time = 0

    for t in times:
        if last_time == 0:
            last_time = t
        else:
            diff_stamps.append((last_time, t))
            diffs.append(t - last_time)
            last_time = t

    min_time = min(times)
    max_time = max(times)
    mean_diff = sum(diffs) / len(diffs)

    gaps = gap_analysis(diffs, diff_stamps, mean_diff * (gap_threshold_percentage))

    return min_time, max_time, mean_diff, gaps


def dump_stats(name, particles, gap_threshold):
    result = []
    min_time, max_time, mean_diff, gaps = stream_stats(particles, gap_threshold)
    result.append('    %-20s: %6d First: %s Last: %s MeanDiff: %8.2f minutes' % \
          (name, len(particles), time.ctime(min_time), time.ctime(max_time), mean_diff/60.0))
    result.append('    Found %d gaps above the threshold of %6.2f%%' % (len(gaps), gap_threshold*100))
    result.append('    Displaying the 10 largest gaps:')
    for gap in gaps[:10]:
        secs, start, stop = gap
        start = time.ctime(start)
        stop = time.ctime(stop)
        result.append('        GAP: %s -> %s (%6.2f minutes)' % (start, stop, secs/60))
    return '\n'.join(result)


def main():
    options = docopt.docopt(__doc__)

    gap_threshold = 5.0

    headers = []
    sci = {}
    eng = []
    for filename in options['<file>']:
        header, sci_dict, eng_particles = analyze(filename)
        headers.append(header)
        for stream in sci_dict:
            sci.setdefault(stream, []).extend(sci_dict[stream])
        eng.extend(eng_particles)

    print 'particles:'
    for key in sci:
        print
        print dump_stats(key, sci[key], gap_threshold)

    print
    print dump_stats('ENG', eng, gap_threshold)



if __name__ == '__main__':
    main()