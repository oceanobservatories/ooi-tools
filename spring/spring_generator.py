#!/usr/bin/env python
"""Spring Generator

Usage:
  spring_generator.py [--single|--multi]
  spring_generator.py <csv_file> [--single|--multi]

Options:
  --single  Generate a single XML file with a single camel context
  --multi   Generate a single XML file with multiple camel contexts

"""
from collections import namedtuple
import docopt
import jinja2
import csv
import yaml
import os
import gdata.spreadsheet.service

__author__ = 'rachelmanoni'

# key for the google sheets spreadsheet containing the source data
key = '1PvbltMYMzkOsXQ45V2PHvvbAc1UN5u_DIXMjT9eegrA'

CamelContext = namedtuple('CamelContext', 'context, name, suffix, driver, sensor')

MULTI_FILE_MULTI_CONTEXT = "multi_file_multi_context.jinja"
SINGLE_FILE_MULTI_CONTEXT = "single_file_multi_context.jinja"
SINGLE_FILE_SINGLE_CONTEXT = "single_file_single_context.jinja"

yml_dir = 'yml_files'
xml_dir = 'xml_files'
csv_dir = 'csv_files'

ampersand = '&amp;'
uri_options = {
    'delete': 'true',
    'delay': 500,
    'maxMessagesPerPoll': 1,
    'exclusiveReadLockStrategy': '#fileChangedStrategy'
}
uri_options = ampersand.join(['%s=%s' % (k, v) for k, v in uri_options.items()])


def sheet_generator():
    client = gdata.spreadsheet.service.SpreadsheetsService()
    for sheet in client.GetWorksheetsFeed(key, visibility='public', projection='basic').entry:
        title = sheet.title.text
        id = sheet.id.text.split('/')[-1]

        rows = []
        for x in client.GetListFeed(key, id, visibility='public', projection='values').entry:
           rows.append({k:v.text for k,v in x.custom.items()})
        yield title, rows

def get_csv(filename):
    rows = list(csv.reader(open(filename, 'rb')))
    header = rows[0]
    return_list = []
    for row in rows[1:]:
        d = {header[i]:row[i] for i in range(len(row))}
        d.update({header[i]:None for i in range(len(row), len(header))})
        return_list.append(d)

    return return_list

def validate_rows(rows):
    for row in rows:
        for key in ['driver', 'name', 'suffix']:
            if row[key] is None:
                return False
    return True

def generate_spring(options, rows):
    parsers = {}

    for row in rows:
        parsers.setdefault(row['context'], []).append(row)
    templateLoader = jinja2.FileSystemLoader(searchpath="templates")
    templateEnv = jinja2.Environment(loader=templateLoader)

    filename = None
    if options['--single']:
        filename = 'ooi-datasets.xml'
        template = templateEnv.get_template(SINGLE_FILE_SINGLE_CONTEXT)
    elif options['--multi']:
        filename = 'ooi-datasets.xml'
        template = templateEnv.get_template(SINGLE_FILE_MULTI_CONTEXT)
    else:
        template = templateEnv.get_template(MULTI_FILE_MULTI_CONTEXT)

    if filename is None:
        for context in parsers:
            if validate_rows(parsers[context]):
                output_text = template.render(rows=parsers[context], camel_context=context, uri_options=uri_options)
                file = open('%s/ooi-%s-decode.xml' % (xml_dir, context) , 'w')
                file.write(output_text)
                file.close()

    else:
        context = 'ooi-datasets'
        output_text = template.render(dictionary=parsers, camel_context=context, uri_options=uri_options)
        file = open('%s/%s.xml' % (xml_dir, context), 'w')
        file.write(output_text)
        file.close()

def generate_ingest_csv(rows):
    """
    file dropbox, file_regex, serial_number, delivery_type, ingest_route, rename?
    """
    for each in rows:
        if each['sensor'] and each['name'] and each['suffix']:
            with open(os.path.join(csv_dir, '%(name)s-%(suffix)s-ingest.conf' % each), 'wb') as csvfile:
                writer = csv.writer(csvfile)
                dropbox = '${edex.home}/data/ooi/%s_%s' % (each['name'], each['suffix'])
                regex = each['regex']
                if regex is None:
                    regex = '.*'
                serial = each['sensor']
                delivery = each['suffix']
                ingest = 'jms-durable:queue:Ingest.%s_%s' % (each['name'], each['suffix'])
                # shove this in a dictionary to eliminate dupes
                writer.writerow((dropbox, regex, serial, delivery, ingest, 'true'))

def generate_test_cases(rows):
    for each in rows:
        test_case = {}
        name = '%s_%s' % (each['name'], each['suffix'])

        test_case['instrument'] = name
        test_case['endpoint'] = name
        test_case['resource'] =  each['resourcedir']
        if each['timeout'] is not None:
            test_case['timeout'] = int(each['timeout'])
        if each['rename'] is not None:
            test_case['rename'] = bool(int(each['rename']))
        
        test_case['pairs'] = []
        pairs = zip(sorted([x for x in each if 'input' in x]),
                    sorted([x for x in each if 'output' in x]))

        for input, output in pairs:
            test_case['pairs'].append((input,output))

        if test_case['pairs']:
            with open('%s/%s.yml' % (yml_dir, name), 'wb') as fh:
                yaml.dump(test_case, fh)

def strip_rows(rows):
    for row in rows:
        for k, v in row.items():
            if v is not None:
                row[k] = v.strip()

def main():
    options = docopt.docopt(__doc__)
    if options['<csv_file>'] is not None:
        rows = get_csv(options['<csv_file>'])
    else:
        sheets = list(sheet_generator())
        title, rows = sheets[0]

    strip_rows(rows)

    rows = [row for row in rows if not row['context'].startswith('#')]
    generate_spring(options, rows)
    generate_ingest_csv(rows)
    generate_test_cases(rows)


if __name__ == '__main__':
    main()



