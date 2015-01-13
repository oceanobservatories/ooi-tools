#!/usr/bin/env python
from itertools import count
import logging
from threading import Event

from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent, execute_concurrent_with_args
from cassandra.query import BatchStatement
import time
import math


def get_logger():
    logger = logging.getLogger('perf_test')
    logger.setLevel(logging.DEBUG)

    # create console handler and set level to debug
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # add formatter to ch
    ch.setFormatter(formatter)

    # add ch to logger
    logger.addHandler(ch)
    return logger


log = get_logger()

test_data = {
    'refdesig': 'abc-123-xyz',
    'year': 2104,
    'jday': 123,
    'time' : 1.0,
    'driver_timestamp': 1.0,
    'internal_timestamp': 1.0,
    'preferred_timestamp': 'internal_timestamp',
    'ingestion_timestamp': 1.0,
    'vel3d_k_id': 0,
    'vel3d_k_version': 0,
    'vel3d_k_serial': 100000000,
    'vel3d_k_configuration': 0,
    'date_time_array': [0,0,0,0],
    'date_time_array_dims': 1,
    'date_time_array_sizes': [1],
    'vel3d_k_micro_second': 0,
    'vel3d_k_speed_sound': 0,
    'vel3d_k_temp_c': 0,
    'vel3d_k_pressure': 100000000,
    'vel3d_k_heading': 0,
    'vel3d_k_pitch': 0,
    'vel3d_k_roll': 0,
    'vel3d_k_error': 0,
    'vel3d_k_status': 0,
    'vel3d_k_beams_coordinate': 0,
    'vel3d_k_cell_size': 0,
    'vel3d_k_blanking': 0,
    'vel3d_k_velocity_range': 0,
    'vel3d_k_battery_voltage': 0,
    'vel3d_k_mag_x': 0,
    'vel3d_k_mag_y': 0,
    'vel3d_k_mag_z': 0,
    'vel3d_k_acc_x': 0,
    'vel3d_k_acc_y': 0,
    'vel3d_k_acc_z': 0,
    'vel3d_k_ambiguity': 0,
    'vel3d_k_data_set_description': [0, 0, 0, 0],
    'vel3d_k_data_set_description_dims': 1,
    'vel3d_k_data_set_description_sizes': [1],
    'vel3d_k_transmit_energy': 0,
    'vel3d_k_v_scale': 0,
    'vel3d_k_power_level': 0,
    'vel3d_k_vel0': 0,
    'vel3d_k_vel1': 0,
    'vel3d_k_vel2': 0,
    'vel3d_k_amp0': 0,
    'vel3d_k_amp1': 0,
    'vel3d_k_amp2': 0,
    'vel3d_k_corr0': 0,
    'vel3d_k_corr1': 0,
    'vel3d_k_corr2': 0
}

keys = sorted(test_data.keys())

insert_stmt_fmt = '''
INSERT INTO ooi.vel3d_k_wfp_instrument
(%s) VALUES (%s)
'''

insert_stmt = insert_stmt_fmt % (', '.join(keys), ', '.join(['?' for _ in keys]))

cluster = Cluster(protocol_version=3)
session = cluster.connect()
session.set_keyspace('ooi')

insert = session.prepare(insert_stmt)

def create_rows(count):
    rows = []

    for i in xrange(count):
        data = test_data.copy()
        data['internal_timestamp'] = float(i)
        data['time'] = float(i)
        rows.append(tuple([data[k] for k in keys]))
    return rows


def main():
    row_count = 100000
    max_insert = 10

    log.info('truncate table')
    session.execute('truncate ooi.vel3d_k_wfp_instrument')
    log.info('done truncating')

    log.info('generating row data')
    rows = create_rows(row_count)
    now = time.time()
    batches = []
    batch = BatchStatement()
    for i, row in enumerate(rows):
        if (i+1) % max_insert == 0:
            batches.append((batch, []))
            batch = BatchStatement()
        batch.add(insert, row)
    batches.append((batch, []))
    log.info('inserting')
    execute_concurrent(session, batches, concurrency=50)
    log.info('%d rows: %7.2f sec elapsed', row_count, time.time()-now)

    # # QUERIES
    # now = time.time()
    # count = 0
    # results = session.execute("select * from ooi.vel3d_k_wfp_instrument where refdesig='abc-123-xyz' and bin=0")
    # for r in results:
    #     count += 1
    # log.info('select all rows (unbinned)(%d): %7.2f sec elapsed', count, time.time()-now)
    #
    # now = time.time()
    # count = 0
    # for x in range(100,100+int(float(row_count)/bin_size)):
    #     results = session.execute("select * from ooi.vel3d_k_wfp_instrument where refdesig='abc-123-xyz' and bin=%d" % x)
    #     for r in results:
    #         count += 1
    # log.info('select all rows (binned)(%d): %7.2f sec elapsed', count, time.time()-now)
    #
    # now = time.time()
    # count = 0
    # results = session.execute("select * from ooi.vel3d_k_wfp_instrument where refdesig='abc-123-xyz' and bin=0 and time>=%d and time<%d" % (bin_size, 2*bin_size))
    # for r in results:
    #     count += 1
    # log.info('select one bin of rows (unbinned)(%d): %7.4f sec elapsed', count, time.time()-now)
    #
    # now = time.time()
    # count = 0
    # results = session.execute("select * from ooi.vel3d_k_wfp_instrument where refdesig='abc-123-xyz' and bin=102")
    # for r in results:
    #     count += 1
    # log.info('select one bin of rows (binned)(%d): %7.4f sec elapsed', count, time.time()-now)


if __name__ == '__main__':
    main()