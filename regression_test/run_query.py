#!/usr/bin/env python
import glob
import os
import sys

import datetime
import requests
import yaml


sep = '  '
format_string = sep.join(['{subsite:<{subsite_size}}',
                          '{node:<{node_size}}',
                          '{sensor:<{sensor_size}}',
                          '{method:<{method_size}}',
                          '{stream:<{stream_size}}',
                          '{result}'])


def build_url(query):
    return 'http://{host}:12576/sensor/inv/{subsite}/{node}/{sensor}/{method}/{stream}'.format(**query)


def build_params(query):
    return {
        'beginDT': query['start'],
        'endDT': query['stop'],
        'limit': 100,
        'user': 'regression_test'
    }


def make_query(query):
    url = build_url(query)
    params = build_params(query)
    query['query_time'] = datetime.datetime.utcnow().isoformat()
    response = requests.get(url, params=params)
    return response.status_code, response.json()


def compare_results(query, last, this, col_sizes):
    if last == this:
        query['result'] = 'PASS'
    else:
        query['result'] = 'FAIL'

    print format_string.format(**query)


def compare(query, results, col_sizes):
    if not os.path.exists('results'):
        os.mkdir('results')

    result_glob = 'results/{subsite}-{node}-{sensor}-{method}-{stream}*'.format(**query)
    result_files = sorted(glob.glob(result_glob))
    if result_files:
        last_run = yaml.load(open(result_files[-1]))
        compare_results(query, last_run, results, col_sizes)


def write_results(query, results):
    result_fname = 'results/{subsite}-{node}-{sensor}-{method}-{stream}-{query_time}.yml'.format(**query)
    yaml.safe_dump(results, open(result_fname, 'w'), default_flow_style=False)


def query(query_file, col_sizes):
    query = yaml.load(open(query_file))
    status, results = make_query(query)
    query.update(col_sizes)
    if status == 200:
        compare(query, results, col_sizes)
        write_results(query, results)
    else:
        print format_string.format(result='QUERY_FAILED', **query)


def get_sizes(query_files):
    col_sizes = {}
    for q_file in query_files:
        query = yaml.safe_load(open(q_file))
        for k in query:
            col_sizes[k] = max(col_sizes.get(k, 0), len(query[k]))
    return {k + '_size': v for k, v in col_sizes.iteritems()}


def main():
    query_files = sys.argv[1:]
    col_sizes = get_sizes(query_files)
    for q_file in query_files:
        query(q_file, col_sizes)


if __name__ == '__main__':
    main()
