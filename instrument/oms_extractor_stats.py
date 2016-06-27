#!/usr/bin/env python
"""
This tool will parse the log files from the OMS Extractor and produce a CSV
file containing statistics of the response times from the OMS.  The CSV file
will have a row statistics for each node in the OMS Extractor config file.

Usage:
    oms_extractor_stats.py <config> <log>
"""

import sys
import docopt
import yaml
from pkg_resources import resource_string
import mi.platform.rsn
import csv
import traceback
import numpy
from subprocess import check_output

__author__ = 'Rene Gelinas'
__license__ = 'Apache 2.0'

GREP_CMD_START = 'egrep "exit.*get_platform_attribute_values.*'


class NodeStats(object):
    def __init__(self, node, total_requests, requests_list):
        self.node = node
        self.total_requests = total_requests
        self.requests_list = requests_list
        self.request_percents_list = self.calculate_percents()
        self.row = []
        self.row.append(self.node)

    def calculate_percents(self):
        request_percents_list = []
        for requests in self.requests_list:
            if requests == '':
                request_percents_list.append('')
            else:
                request_percent = (float(requests) /
                                   float(self.total_requests)) * 100.0
                request_percents_list.append("%0.2f" % request_percent)

        return request_percents_list

    def expand_stats_row(self, max_requests):
        for index in range(max_requests):
            if index < len(self.requests_list):
                self.row.append(self.requests_list[index])
            else:
                self.row.append('')

        for index in range(max_requests):
            if index < len(self.request_percents_list):
                self.row.append(self.request_percents_list[index])
            else:
                self.row.append('')


def parse_log_file(log_file, config_files):
    node_stats_list = []
    max_delay_requests = 0
    grep_cmd_end = '" ' + log_file + ' | wc -l'

    for config_file in config_files:
        try:
            print '.',
            sys.stdout.flush()
            node_config_string = resource_string(mi.platform.rsn.__name__,
                                                 config_file)
            node_config = yaml.load(node_config_string)
            node = node_config['node_meta_data']['node_id_name']
            grep_cmd_begin = GREP_CMD_START + node

            grep_cmd = grep_cmd_begin + grep_cmd_end
            total_requests_str = check_output(grep_cmd, shell=True)
            total_requests = int(total_requests_str[:-1])

            requests_list = []
            num_requests = 0
            secs_delay = 0
            while num_requests < total_requests:
                secs = "%02d" % (secs_delay,)
                grep_cmd = grep_cmd_begin + '.*0:00:' + secs + grep_cmd_end
                requests_for_delay_str = check_output(grep_cmd, shell=True)
                requests_for_delay = int(requests_for_delay_str[:-1])
                if requests_for_delay != 0:
                    requests_list.append(requests_for_delay)
                else:
                    requests_list.append("")
                secs_delay += 1
                num_requests += requests_for_delay

            node_stats = NodeStats(node, total_requests, requests_list)
            node_stats_list.append(node_stats)
            if len(requests_list) > max_delay_requests:
                max_delay_requests = len(requests_list)

        except Exception or IOError as e:
            print "Error: %s occurred parsing config file  : %s" %\
                  (e, config_file)

    print ''

    node_stats_list.sort(key=lambda node_stat: node_stat.node)

    return node_stats_list, max_delay_requests


def write_stats(node_stats_list, max_delay_requests):
    with open('oms_extractor_stats.csv', 'w') as file_handler:
        writer = csv.writer(file_handler)

        # Generate the title row.
        column_titles = ['']
        for delay in range(1, max_delay_requests+1):
            sec_title = '<' + str(delay) + ' Sec '
            column_titles.append(sec_title + 'Response')

        for delay in range(1, max_delay_requests+1):
            sec_title = '<' + str(delay) + ' Sec '
            column_titles.append(sec_title + '%')

        # Separate the nodes into categories
        primary_node_stats_list = []
        secondary_node_stats_list = []
        other_node_stats_list = []
        for node_stats in node_stats_list:
            node_stats.expand_stats_row(max_delay_requests)
            if node_stats.node.startswith('Node'):
                primary_node_stats_list.append(node_stats)
            elif node_stats.node.startswith('SN'):
                secondary_node_stats_list.append(node_stats)
            else:
                other_node_stats_list.append(node_stats)

        column_titles[0] = 'Primary Node'
        writer.writerow(column_titles)
        for node_stats in primary_node_stats_list:
            try:
                writer.writerow(node_stats.row)
            except Exception or IOError as e:
                traceback.print_exc()

        column_titles[0] = 'Secondary Node'
        writer.writerow(column_titles)
        for node_stats in secondary_node_stats_list:
            try:
                writer.writerow(node_stats.row)
            except Exception or IOError as e:
                traceback.print_exc()

        column_titles[0] = 'Other Node'
        writer.writerow(column_titles)
        for node_stats in other_node_stats_list:
            try:
                writer.writerow(node_stats.row)
            except Exception or IOError as e:
                traceback.print_exc()


def main():
    options = docopt.docopt(__doc__)
    oms_extractor_config = options['<config>']
    oms_extractor_log = options['<log>']

    config = yaml.load(open(oms_extractor_config))
    node_config_files = config['node_config_files']

    node_stats_list, max_delay_requests = parse_log_file(oms_extractor_log,
                                                         node_config_files)
    write_stats(node_stats_list, max_delay_requests)


if __name__ == '__main__':
    main()
