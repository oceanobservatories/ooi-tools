# Cassandra Utilities

These tools provide utilities for working with the Cassandra NoSQL database used to store OOI scientific and engineering 
data. These are not intended for use on production, but may be helpful in working with test clusters and provide insight for
working with cassandra.

| utility | purpose | notes |
| --- | --- | --- |
| `cassandra_data_util` | utility | load/dump data, upgrade cassandra |
| `ctdbp_insert` | planning | compare single/batch data ingest |
| `delete_data` | utility | remove all data for a reference designator from cassandra |
| `hourly_to_partition` | utility | convert metadata to align to partitions |
| `perf_test` | planning | time row insertion |
| `query_data` | planning | compare retrieval times by processing pools |
| `test` | planning | estimate insertion performance

## cassandra_data_util

Provides several utility functions for working with cassandra. 

### Usage

```
cassandra_data_util.py <dir> (--load|--dump) [--filter=<regex>] [--keyspace=<name>] [--contact=<ip_address>] [--upgrade=<upgrade_id>] [--preload=<preload>]
cassandra_data_util.py --direct --remote_contact=<ip_address> --remote_keyspace=<name> [--keyspace=<name>] [--contact=<contact>]

  dir         directory containing data for loading or output of dump
  --load      load all available data from dir
  --dump      dumps cassandra data to dir
  --filter    provides regular expression to match streams to be dumped
  --keyspace  specifies the cassandra keyspace [default: ooi]
  --contact   cassandra cluster IP [default: 127.0.0.1]
  --upgrade   if '5.1-to-5.2' applies necessary time correction to record bins
  --preload   specifies the preload database to use
  --direct    not yet implemented
```

It is likely that this utility has not be used recently and should not be used on production. It can be useful for working 
with test clusters and serves as an example of how to work with the cassandra cluster. 

## ctdbp_insert

An example timing check for comparing single to batch loading of a sample CTD stream. 

This sample code is tied to a specific cassandra cluster and was used to check the performance benefit of using batch 
loading of data into cassandra. It provides an example of how a performance check can be exercised and remains here for 
reference only.

## hourly_to_partition

Realign the hourly metadata records to stream-specific partitions.

The original metadata records were uniform for all stream; metadata was segregated hourly to track number of particles. This 
was replaced with stream-specific partitions based on anticipated stream rates resulting in overall performance improvement. 
Since this was done once, this will utility will not be required again, but remains here for reference.

## perf_test

Measures row insertion metrics. 

Uses prototype data to measure cassandra performance. Note that this is not the same as the current OOI data. This was used 
for initial design and is here for reference only.

## query_data

Returns retrieval performance of cassandra cluster for sample data. Returns timing results for data retrieval for the 
specified number of subjobs. 

### Usage

```
query_data.py refdes stream count pool_size

  refdes     fully qualified reference designator
  stream     name of stream
  count      number of particles to fetch
  pool_size  number of subjobs used to process request
```
  
This code was used to establish the current subjob retrieval for cassandra that is now in use by uFrame and remains here for 
reference only.

## test

Returns timing results for various string and list data insertions and deletions for a cassandra cluster.  

This code was used to explore the performance interactions with cassandra and is here for reference only.

