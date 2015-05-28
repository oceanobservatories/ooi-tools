#!/usr/bin/env python
"""
generate_port_agent_config.py

Generate port agent configuration files and a supervisord file from CSV input

Usage:
    generate_port_agent_config.py <path> <name> <csv_file>
"""
import shutil
import jinja2
import os
import docopt
from csv import DictReader

loader = jinja2.FileSystemLoader(searchpath="templates")
env = jinja2.Environment(loader=loader, trim_blocks=True, lstrip_blocks=True)
pa_template = env.get_template('pa_config.jinja')
super_template = env.get_template('supervisord.jinja')

def prep_dir(path, name):
    configdir = os.path.join(path, 'configs')
    configfile = os.path.join(path, '%s.conf' % name)
    if os.path.exists(path):
        if not os.path.isdir(path):
            raise Exception('path matches existing file, looking for directory')

        if os.path.exists(configdir):
            shutil.rmtree(configdir)
        if os.path.exists(configfile):
            os.remove(configfile)

    os.makedirs(configdir)


def create_pa_dict(csv_file):
    pa_dict = {}
    with open(csv_file) as fh:
        for row in DictReader(fh):
            name = create_pa_name(row)
            if name is not None:
                pa_dict[name] = row
    return pa_dict


def create_pa_name(pa_dict):
    if any([pa_dict['instrument'] == '', pa_dict['refdes'] == '']):
        return None
    return '%s_%s' % (pa_dict['instrument'], pa_dict['refdes'])


def create_pa_config(pa_dict):
    return pa_template.render(**pa_dict)


def create_pa_configs(pa_dict):
    pa_configs = {}
    for name in pa_dict:
        config = create_pa_config(pa_dict[name])
        pa_configs[name] = config
    return pa_configs


def create_supervisord_config(name, pa_dict):
    groups = {}
    for name in pa_dict:
        each = pa_dict[name]
        group = each['group']
        groups.setdefault(group, []).append(each)
    return super_template.render(name=name, groups=groups)


def write(path, name, supervisord_config, pa_configs):
    with open(os.path.join(path, '%s.conf' % name), 'wb') as fh:
        fh.write(supervisord_config)

    for pa_name in pa_configs:
        with open(os.path.join(path, 'configs', '%s.conf' % pa_name), 'wb') as fh:
            fh.write(pa_configs[pa_name])


def main():
    options = docopt.docopt(__doc__)
    path = options['<path>']
    name = options['<name>']
    csv_file = options['<csv_file>']
    prep_dir(path, name)
    pa_dict = create_pa_dict(csv_file)
    pa_configs = create_pa_configs(pa_dict)
    supervisord_config = create_supervisord_config(name, pa_dict)
    write(path, name, supervisord_config, pa_configs)


if __name__ == '__main__':
    main()