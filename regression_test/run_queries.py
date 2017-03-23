#!/usr/bin/env python
"""
OOI Data Retrieval Regression Test Tool

Usage:
  run_queries.py [--output=<output> --compare=<compare> -v] <queries>...
  run_queries.py -h | --help

Options:
  -h --help            Show this screen.
  -v                   Verbose logging
  --output=<output>    Record the output of these queries in <output>.
  --compare=<compare>  Compare the output of these queries to previous results from <compare>.

"""
import glob
import json
import os

import datetime

import logging
import requests
import yaml

from dictdiffer import diff


log = logging.getLogger('run_queries')
log.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
log.addHandler(ch)


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
    log.debug('Fetching %r %r', url, params)
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
        self.results_file = None
        self.error_file = None

        # precompute maximum sizes for columnar output
        self.col_sizes = get_sizes(self.query_files)
        self.prepare()

    def prepare(self):
        if self.compare_dir and not os.path.isdir(self.compare_dir):
            raise UserWarning('Compare directory specified does not exist!')
        if self.output_dir:
            if not os.path.exists(self.output_dir):
                os.makedirs(self.output_dir)

    def make_file(self, prefix):
        fname = '%s-%s.txt' % (prefix, datetime.datetime.utcnow().isoformat())
        if self.output_dir:
            fname = os.path.join(self.output_dir, fname)
        return open(fname, 'w')

    def make_error_file(self):
        self.error_file = self.make_file('errors')

    def make_results_file(self):
        self.results_file = self.make_file('results')
        header = {
            'subsite': 'subsite',
            'node': 'node',
            'sensor': 'sensor',
            'method': 'method',
            'stream': 'stream',
            'result': 'result'
        }
        separator = {k: '-' * self.col_sizes.get('%s_size' % k, len(k)) for k in header}
        header.update(self.col_sizes)
        separator.update(self.col_sizes)
        self.write_results(format_string.format(**header))
        self.write_results(format_string.format(**separator))

    def write_results(self, msg):
        if self.results_file is None:
            self.make_results_file()
        self.results_file.write(msg)
        self.results_file.write('\n')

    def write_errors(self, msg):
        if self.error_file is None:
            self.make_error_file()
        self.error_file.write(msg)
        self.error_file.write('\n')

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
            self.write_response(query, results)
        else:
            msg = format_string.format(result='QUERY_FAILED', **query)
            log.error(msg)
            log.error('Received non-200 response: %r', results)
            self.write_results(msg)

    def compare_results(self, query, last, this):
        result = list(diff(this, last))
        if not result:
            query['result'] = 'PASS'
        else:
            query['result'] = 'FAIL'
            loggable_result = repr(result)
            if len(loggable_result) > 120:
                loggable_result = loggable_result[:100] + '...'
            log.error('Results differ from previous run: %s', loggable_result)
            self.write_errors(json.dumps(query))
            self.write_errors(json.dumps(result))

        msg = format_string.format(**query)
        self.write_results(msg)
        log.info(msg)

    def compare(self, query, results):
        if self.compare_dir:
            result_glob = '{subsite}-{node}-{sensor}-{method}-{stream}-20*'.format(**query)
            result_files = sorted(glob.glob(os.path.join(self.compare_dir, result_glob)))
            if result_files:
                log.debug('Comparing %r to %r', query, result_files[-1])
                last_run = yaml.load(open(result_files[-1]))
                self.compare_results(query, last_run, results)
            else:
                log.info('Requested compare but no matching results found for %r', query)

    def write_response(self, query, results):
        if self.output_dir:
            result_fname = '{subsite}-{node}-{sensor}-{method}-{stream}-{query_time}.yml'.format(**query)
            result_fpath = os.path.join(self.output_dir, result_fname)
            log.debug('Recording results to: %r', result_fpath)
            yaml.safe_dump(results, open(result_fpath, 'w'), default_flow_style=False)


def main():
    import docopt
    options = docopt.docopt(__doc__)

    if options['-v']:
        log.setLevel(logging.DEBUG)

    tool = QueryTool(options.get('<queries>'),
                     output_dir=options.get('--output'),
                     compare_dir=options.get('--compare'))
    tool.run()


if __name__ == '__main__':
    main()
