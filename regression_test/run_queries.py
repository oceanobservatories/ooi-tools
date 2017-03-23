#!/usr/bin/env python
"""
OOI Data Retrieval Regression Test Tool

Usage:
  run_queries.py [--output=<output> --compare=<compare>] <queries>...
  run_queries.py -h | --help

Options:
  -h --help            Show this screen.
  --output=<output>    Record the output of these queries in <output>.
  --compare=<compare>  Compare the output of these queries to previous results from <compare>.

"""


import glob
import os

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


def get_sizes(query_files):
    col_sizes = {}
    for q_file in query_files:
        query = yaml.safe_load(open(q_file))
        for k in query:
            col_sizes[k] = max(col_sizes.get(k, 0), len(query[k]))
    return {k + '_size': v for k, v in col_sizes.iteritems()}


class QueryTool(object):
    def __init__(self, query_files, output_dir=None, compare_dir=None):
        self.query_files = query_files
        self.output_dir = output_dir
        self.compare_dir = compare_dir

        self.prepare()
        # precompute maximum sizes for columnar output
        self.col_sizes = get_sizes(self.query_files)

    def prepare(self):
        if self.compare_dir and not os.path.isdir(self.compare_dir):
            raise UserWarning('Compare directory specified does not exist!')
        if self.output_dir:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

    def run(self):
        for q_file in self.query_files:
            self.query(q_file)

    def load_query(self, query_file):
        query = yaml.load(open(query_file))
        query.update(self.col_sizes)
        return query

    def query(self, query_file):
        query = self.load_query(query_file)
        status, results = make_query(query)

        if status == 200:
            self.compare(query, results)
            self.write_results(query, results)
        else:
            print format_string.format(result='QUERY_FAILED', **query)

    def compare_results(self, query, last, this):
        if last == this:
            query['result'] = 'PASS'
        else:
            query['result'] = 'FAIL'

        print format_string.format(**query)

    def compare(self, query, results):
        if self.compare_dir:
            result_glob = '{subsite}-{node}-{sensor}-{method}-{stream}-20*'.format(**query)
            result_files = sorted(glob.glob(os.path.join(self.compare_dir, result_glob)))
            if result_files:
                last_run = yaml.load(open(result_files[-1]))
                self.compare_results(query, last_run, results)

    def write_results(self, query, results):
        if self.output_dir:
            result_fname = '{subsite}-{node}-{sensor}-{method}-{stream}-{query_time}.yml'.format(**query)
            result_fpath = os.path.join(self.output_dir, result_fname)

            yaml.safe_dump(results, open(result_fpath, 'w'), default_flow_style=False)


def main():
    import docopt
    options = docopt.docopt(__doc__)

    tool = QueryTool(options.get('<queries>'),
                     output_dir=options.get('--output'),
                     compare_dir=options.get('--compare'))
    tool.run()


if __name__ == '__main__':
    main()
