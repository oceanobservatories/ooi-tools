#!/usr/bin/env python

import json
import logging
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement
import time
import ntplib

import sys
# import numpy
#
# sys.path.append('/Users/pcable/src/ion-functions')
#
# from ion_functions.data.ctd_functions import ctd_sbe16plus_tempwat


def get_logger():
    logger = logging.getLogger('ctdbp')
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

# CREATE TABLE ooi.ctdbp_cdef_cp_instrument_recovered (
#     refdesig text,
#     year int,
#     jday int,
#     time double,
#     conductivity int,
#     ctd_time int,
#     driver_timestamp double,
#     ingestion_timestamp double,
#     internal_timestamp double,
#     preferred_timestamp text,
#     pressure int,
#     pressure_temp int,
#     temperature int,
#     PRIMARY KEY ((refdesig, year, jday), time)

keys = '''
    refdesig
    year
    jday
    time
    conductivity
    ctd_time
    driver_timestamp
    ingestion_timestamp
    internal_timestamp
    preferred_timestamp
    pressure
    pressure_temp
    temperature
'''.split()

insert_stmt_fmt = '''
INSERT INTO ooi.ctdbp_cdef_cp_instrument_recovered
(%s) VALUES (%s)
'''

insert_stmt = insert_stmt_fmt % (', '.join(keys), ', '.join(['?' for _ in keys]))

cluster = Cluster(('10.0.1.50','10.0.1.56'))
session = cluster.connect()
session.set_keyspace('ooi')

insert = session.prepare(insert_stmt)

def flatten(particle):
    for each in particle.get('values', []):
        particle[each['value_id']] = each['value']
    del(particle['values'])
    del(particle['quality_flag'])
    del(particle['stream_name'])
    del(particle['pkt_format_id'])
    del(particle['pkt_version'])
    particle['time'] = particle.get(particle.get('preferred_timestamp', {}))
    particle['ingestion_timestamp'] = ntplib.system_to_ntp_time(time.time())
    ts = ntplib.ntp_to_system_time(particle['time'])
    ts = time.gmtime(ts)
    particle['refdesig'] = 'TEST'
    particle['year'] = ts.tm_year
    particle['jday'] = ts.tm_yday

def load_particles():
    flat_particles = []
    particle_dict = json.load(open('ctdbp_cdef_cp_instrument_recovered.json'))
    for particle_name in particle_dict:
        particles = particle_dict[particle_name]
        for p in particles:
            p = json.loads(p)
            flatten(p)
            flat_particles.append(p)
    return flat_particles

def timeit(func, *args, **kwargs):
    now = time.time()
    rval = func(*args, **kwargs)
    return rval, time.time() - now

def truncate():
    session.execute('truncate ooi.ctdbp_cdef_cp_instrument_recovered')

def load_naive(particles):
    for p in particles:
        session.execute(insert, [p[k] for k in keys])

def load_batch(particles, batch_size=100):
    batch = BatchStatement()
    batched_count = 0
    for p in particles:
        batch.add(insert, [p[k] for k in keys])
        batched_count += 1
        if batched_count == batch_size:
            session.execute(batch)
            batch = BatchStatement()
            batched_count = 0
    if batched_count:
        session.execute(batch)

def load_concurrent(particles, concurrent_size=50):
    execute_concurrent_with_args(session, insert, particles, concurrency=concurrent_size)

def query_all():
    count = 0
    results = session.execute('select * from ooi.ctdbp_cdef_cp_instrument_recovered')
    for x in results:
        if count == 0:
            first = x
        count += 1
    last = x
    return count

def execute_tempwat():
    results = session.execute('select temperature from ooi.ctdbp_cdef_cp_instrument_recovered')
    temps = []
    for x in results:
        temps.append(x.temperature)

    # temps = numpy.array(temps)
    # l1 = ctd_sbe16plus_tempwat(temps, 1.0, 1.0, 1.0, 1.0)
    # return len(l1)
    return len(temps)

def main():
    particles, elapsed = timeit(load_particles)
    log.info('loaded particles into memory: %7.3f secs', elapsed)
    # truncate()
    # log.info('naive load: %7.3f' % timeit(load_naive, particles)[1])
    truncate()
    log.info('batch load: %7.3f secs', timeit(load_batch, particles)[1])
    # truncate()
    # log.info('concurrent load: %7.3f secs', timeit(load_concurrent, particles)[1])
    # count, elapsed = timeit(query_all)
    # log.info('query_all: %d records in %7.3f secs', count, elapsed)
    # count, elapsed = timeit(execute_tempwat)
    # log.info('execute_tempwat: %d records in %7.3f secs', count, elapsed)


if __name__ == '__main__':
    main()