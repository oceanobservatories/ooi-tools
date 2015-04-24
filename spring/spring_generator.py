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
from StringIO import StringIO
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


class Spring(object):
    def __init__(self, contexts, name):
        self.name = name
        self.contexts = contexts

    def to_spring(self, template):
        return template.render(contexts=self.contexts)

    def write_file(self, template):
        filename = '%s/ooi-%s-decode.xml' % (xml_dir, self.name)
        with open(filename, 'wb') as fh:
            fh.write(self.to_spring(template))

    @staticmethod
    def create_springs(rows, style=None):
        # Single file, single context, no need to sort
        if style == 'single':
            return [Spring([Context('OOI-DECODE', rows)], 'all')]

        # multiple contexts, group them
        contexts = Spring.group_by_context(rows)

        if style == 'multi':
            # multiple camel contexts, single file
            return [Spring([Context(c, contexts[c]) for c in contexts], 'all')]

        return [Spring([Context(c, contexts[c])], c) for c in contexts]

    @staticmethod
    def group_by_context(rows):
        d = {}
        for row in rows:
            d.setdefault(row.name, []).append(row)
        return d


class Context(object):
    def __init__(self, name, rows):
        self.name = name
        self.rows = rows

    def __str__(self):
        return '%s: %s' % (self.name, self.rows)

    def __repr__(self):
        return '%s: %s' % (self.name, self.rows)


class Row(object):
    def __init__(self, row_dict):
        self._strip(row_dict)
        self.context = row_dict.get('context')
        self.name = row_dict.get('name')
        self.suffix = row_dict.get('suffix')
        self.driver = row_dict.get('driver').replace("/",".")[:-3]
        self.sensor = row_dict.get('sensor')
        self.regex = row_dict.get('regex')
        self.resource = row_dict.get('resource')
        self.timeout = row_dict.get('timeout')
        self.rename = row_dict.get('rename')
        self.hashkeys = row_dict.get('hashkeys')

        self.bin = row_dict.get('bin')
        if self.bin is None:
            self.bin = 'oneHourBin'
        elif self.bin == 'disabled':
            self.bin = None

        self.klass = row_dict.get('class')
        if self.klass is None:
            self.klass = 'com.raytheon.uf.edex.ooi.decoder.dataset.FileDecoder'
        self.pairs = self._generate_pairs(row_dict)


        if self.hashkeys is not None:
            self.hashkeys = [x.strip() for x in self.hashkeys.split(',')]

    def _strip(self, d):
        for k in d:
            if d[k] is not None:
                d[k] = d[k].strip()

    def _generate_pairs(self, row_dict):
        pairs = []
        keys = zip(sorted([x for x in row_dict if 'input' in x]),
                    sorted([x for x in row_dict if 'output' in x]))

        for input, output in keys:
            input = row_dict[input]
            output = row_dict[output]
            if all([input, output]):
                pairs.append([input, output])
        return pairs

    def is_valid(self):
        if self.context.startswith('#'):
            return False
        if not all([self.driver, self.name, self.suffix]):
            return False
        return True

    def to_csv(self):
        io = StringIO()
        writer = csv.writer(io)
        dropbox = '${edex.home}/data/ooi/%s_%s' % (self.name, self.suffix)
        ingest = 'jms-durable:queue:Ingest.%s_%s' % (self.name, self.suffix)
        if self.regex is None:
            self.regex = '.*'
        writer.writerow((dropbox, self.regex, self.sensor, self.suffix, ingest, 'true'))
        return io.getvalue()

    def to_yml(self):
        test_case = {}
        name = '%s_%s' % (self.name, self.suffix)

        test_case['instrument'] = name
        test_case['endpoint'] = name
        test_case['resource'] =  self.resource
        if self.timeout is not None:
            try:
                test_case['timeout'] = int(self.timeout)
            except:
                pass
        if self.rename is not None:
            try:
                test_case['rename'] = bool(self.rename)
            except:
                pass

        test_case['pairs'] = self.pairs
        return yaml.dump(test_case)

    def write_csv(self):
        with open(os.path.join(csv_dir, '%s-%s-ingest.conf' % (self.name, self.suffix)), 'wb') as csvfile:
            csvfile.write(self.to_csv())

    def write_yml(self):
        with open('%s/%s_%s.yml' % (yml_dir, self.name, self.suffix), 'wb') as fh:
            fh.write(self.to_yml())

    def __str__(self):
        return 'ROW: %s' % self.name

    def __repr__(self):
        return 'ROW: %s' % self.name

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
        if not row:
            continue
        d = {header[i]:row[i] for i in range(len(row))}
        d.update({header[i]:None for i in range(len(row), len(header))})
        for key in d:
            if d[key] == '':
                d[key] = None
        return_list.append(d)

    return return_list


# def generate_spring(options, rows):
#     parsers = {}
#
#     for row in rows:
#         parsers.setdefault(row['context'], []).append(row)
#     templateLoader = jinja2.FileSystemLoader(searchpath="templates")
#     templateEnv = jinja2.Environment(loader=templateLoader)
#
#     filename = None
#     if options['--single']:
#         filename = 'ooi-datasets.xml'
#         template = templateEnv.get_template(SINGLE_FILE_SINGLE_CONTEXT)
#     elif options['--multi']:
#         filename = 'ooi-datasets.xml'
#         template = templateEnv.get_template(SINGLE_FILE_MULTI_CONTEXT)
#     else:
#         template = templateEnv.get_template(MULTI_FILE_MULTI_CONTEXT)
#
#     if filename is None:
#         for context in parsers:
#             if validate_rows(parsers[context]):
#                 output_text = template.render(rows=parsers[context], camel_context=context, uri_options=uri_options)
#                 file = open('%s/ooi-%s-decode.xml' % (xml_dir, context) , 'w')
#                 file.write(output_text)
#                 file.close()
#
#     else:
#         context = 'ooi-datasets'
#         output_text = template.render(dictionary=parsers, camel_context=context, uri_options=uri_options)
#         file = open('%s/%s.xml' % (xml_dir, context), 'w')
#         file.write(output_text)
#         file.close()

def main():
    options = docopt.docopt(__doc__)

    templateLoader = jinja2.FileSystemLoader(searchpath="templates")
    templateEnv = jinja2.Environment(loader=templateLoader, trim_blocks=True, lstrip_blocks=True)
    template = templateEnv.get_template('spring_template.jinja')

    if options['<csv_file>'] is not None:
        rows = get_csv(options['<csv_file>'])
    else:
        sheets = list(sheet_generator())
        title, rows = sheets[0]

    rows = [Row(x) for x in rows]


    if options['--single']:
        springs = Spring.create_springs(rows, 'single')
    elif options['--multi']:
        springs = Spring.create_springs(rows, 'multi')
    else:
        springs = Spring.create_springs(rows)

    for spring in springs:
        spring.write_file(template)

    for row in rows:
        row.write_csv()
        row.write_yml()


if __name__ == '__main__':
    main()



