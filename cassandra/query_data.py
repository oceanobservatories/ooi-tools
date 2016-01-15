#!/usr/bin/env python

from multiprocessing import Pool, BoundedSemaphore
import sys
import time

import cassandra
from cassandra.cluster import Cluster
from cassandra.query import tuple_factory, SimpleStatement

lock = BoundedSemaphore(6)

class QueryManager(object):

    batch_size = 10

    def __init__(self, cluster, process_count=None):
        self.pool = Pool(processes=process_count, initializer=self._setup, initargs=(cluster,))

    @classmethod
    def _setup(cls, cluster):
        with lock:
            cls.session = cluster.connect('ooi')
            cls.session.row_factory = tuple_factory
            print 'worker ready'

    def close_pool(self):
        self.pool.close()
        self.pool.join()

    def get_results(self, query, params):
        results = self.pool.map(_get_multiproc, [(query,p) for p in params], self.batch_size)
        return results

    def get_num_results(self, query, params):
        results = self.pool.map(_get_len_multiproc, [(query,p) for p in params], self.batch_size)
        return results

    @classmethod
    def _execute_request(cls, query, params):
        now = time.time()
        stmt = SimpleStatement(query, params, consistency_level=cassandra.ConsistencyLevel.LOCAL_QUORUM)
        rval = list(cls.session.execute(stmt, params))
        print 'query returned: %d rows in %.2f secs' % (len(rval), time.time() - now)
        return rval


def _get_multiproc(params):
    query, params = params
    return QueryManager._execute_request(query, params)

def _get_len_multiproc(params):
    query, params = params
    return len(QueryManager._execute_request(query, params))


if __name__ == '__main__':
    refdes = sys.argv[1]
    stream = sys.argv[2]
    count = int(sys.argv[3])
    pool_size = int(sys.argv[4])

    subsite, node, sensor = refdes.split('-', 2)

    cluster = Cluster(['192.168.144.101'])
    qm = QueryManager(cluster, pool_size)
    time.sleep(2 * pool_size)
    print 'Beginning query'

    bins = qm.get_results('select method, bin from partition_metadata where stream=%%s and refdes=%%s limit %s' % count, [(stream, refdes)])
    params = [(subsite, node, sensor, method, bin) for method, bin in bins[0]]

    values = qm.get_num_results('select * from %s where subsite=%%s and node=%%s and sensor=%%s and method=%%s and bin=%%s' % stream, params)
    print values