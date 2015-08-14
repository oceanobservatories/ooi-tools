#!/usr/bin/env python
"""
generate_port_agent_config.py

Generate test case .yml files from CSV input

Usage:
    generate_port_agent_config.py <csv_file>
"""
import jinja2
import docopt
from csv import DictReader

loader = jinja2.FileSystemLoader(searchpath="templates")
env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
super_template = env.get_template('test_case.jinja')


def create_tc_dict(csv_file):
    tc_dict = {}
    with open(csv_file) as fh:
        for row in DictReader(fh):
            name = create_tc_name(row)
            if name is not None:
                tc_dict[name] = row
    return tc_dict


def create_tc_name(tc_dict):
    if any([tc_dict['instrument'] == '', tc_dict['refdes'] == '']):
        return None
    return '%s' % ( tc_dict['refdes'])


def create_tc_config(tc_dict):

    #todo need special case for VADCP and MASSP, for now hand mod
    return super_template.render(**tc_dict)


def create_tc_configs(tc_dict):
    tc_configs = {}
    for name in tc_dict:
        config = create_tc_config(tc_dict[name])
        tc_configs[name] = config
    return tc_configs


def write(tc_configs):

    for tc_name in tc_configs:
        #todo move to a folder
        with open(('%s.yml' % tc_name), 'wb') as fh:
            fh.write(tc_configs[tc_name])


def main():
    options = docopt.docopt(__doc__)
    csv_file = options['<csv_file>']
    tc_dict = create_tc_dict(csv_file)
    tc_configs = create_tc_configs(tc_dict)
    write(tc_configs)


if __name__ == '__main__':
    main()
