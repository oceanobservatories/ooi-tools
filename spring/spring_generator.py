#!/usr/bin/env python
"""Spring Generator

Usage:
  spring_generator.py [options]

Options:
  --one_file                      How to generate the spring files, individual files by camel_context or one file for all
  --camel_context                 How to generate the spring files, individual files by camel_context or one file for all

"""



from collections import namedtuple
import docopt

__author__ = 'rachelmanoni'
import jinja2
import csv

CamelContext = namedtuple('CamelContext', 'camel_context, driver_name, sensor_id, suffix, driver_path')

CVS_SPRING_FILE = 'spring_files.csv'
TEMPLATE_FILE_MULTI_FILES = "spring_template.jinja"
TEMPLATE_FILE_ONE_FILE = "spring_template2.jinja"


def get_csv():

    dictionary = {}

    for camel_context in map(CamelContext._make, csv.reader(open(CVS_SPRING_FILE, 'rb')))[1:]:
        dictionary.setdefault(camel_context.camel_context, []).append(camel_context)

    return dictionary


def main():

    options = docopt.docopt(__doc__)

    dictionary = get_csv()

    templateLoader = jinja2.FileSystemLoader(searchpath=".")
    templateEnv = jinja2.Environment(loader=templateLoader)

    if options['camel_context']:
        template = templateEnv.get_template(TEMPLATE_FILE_MULTI_FILES)
        for context in dictionary:
            output_text = template.render(rows=dictionary[context], camel_context=context)
            file = open(context + '.xml', 'w')
            file.write(output_text)
            file.close()
    elif options['one_file']:
        template = templateEnv.get_template(TEMPLATE_FILE_ONE_FILE)
        output_text = template.render(dictionary=dictionary)
        file = open('spring_generation.xml', 'w')
        file.write(output_text)
        file.close()


if __name__ == '__main__':
    main()



