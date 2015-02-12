#!/usr/bin/env python

import uuid
import timeit
import functools
from cassandra.cluster import Cluster

cluster = Cluster()
session = cluster.connect()

keyspace_cql = '''
create keyspace if not exists size_test
with replication = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }
'''

string_tablename = 'string_test'
list_tablename = 'list_test'

table_cql = '''
create table if not exists size_test.%s (
    group int,
    id uuid,
    data %s,
    primary key ((group), id))
'''

string_table_cql = table_cql % (string_tablename, 'text')
list_table_cql = table_cql % (list_tablename, 'list<int>')

insert_cql = '''
insert into size_test.%s (group, id, data)
values (?, ?, ?)
'''

select_cql = 'select * from size_test.%s where group=?'

string_insert_cql = insert_cql % string_tablename
list_insert_cql = insert_cql % list_tablename
string_select_cql = select_cql % string_tablename
list_select_cql = select_cql % list_tablename

string_truncate_cql = 'truncate size_test.%s' % string_tablename
list_truncate_cql = 'truncate size_test.%s' % list_tablename

def create_keyspace():
    session.execute(keyspace_cql)

def insert_one(stmt, seed, size):
    session.execute(stmt, (size, uuid.uuid4(), seed * size))

def select_some(stmt, group):
    session.execute(stmt, (group,))
    
def test(start, stop, step, insert, select, seed, count=100):
    session.execute(string_truncate_cql)
    for x in range(start, stop, step):
        insert_time = timeit.timeit(functools.partial(insert_one, insert, seed, x), number=count)
        select_time = timeit.timeit(functools.partial(select_some, select, x), number=count)
        insert_rate = count / insert_time
        select_rate = count / select_time
        print '%3s %-6d %7.2f %7.2f %7.2f %7.2f' % (seed, x, insert_time, insert_rate, select_time, select_rate)

def main():
    # create keyspace, tables, flush tables
    create_keyspace()
    session.execute(string_table_cql)
    session.execute(list_table_cql)
    session.execute(string_truncate_cql)
    session.execute(list_truncate_cql)

    # prepare statements
    string_insert_ps = session.prepare(string_insert_cql)
    string_select_ps = session.prepare(string_select_cql)
    list_insert_ps = session.prepare(list_insert_cql)
    list_select_ps = session.prepare(list_select_cql)

    # execute tests
    test(100, 1000, 100, string_insert_ps, string_select_ps, '.')
    test(100, 1000, 100, list_insert_ps, list_select_ps, [0])
    session.shutdown()

if __name__ == '__main__':
    main()

# vim: tabstop=4 expandtab shiftwidth=4 softtabstop=4
