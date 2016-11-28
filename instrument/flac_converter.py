#!/usr/bin/env python
"""
Description:
    The FLAC Converter will take either an mseed file or a root directory
    where mseed files reside in the structure.  It will convert the mseed
    file(s) to flac files and place them in the same directory.

Usage:
    flac_converter.py <mseed_dir_or_file>
"""

import os
import fnmatch
import logging
import docopt
import obspy
from soundfile import SoundFile

__author__ = 'Rene Gelinas'
__license__ = 'Apache 2.0'

MAX_MSEED_VALUE = (2.0**23)-1

log = None


def get_logger():
    logger = logging.getLogger('FLAC_CNV')
    logger.setLevel(logging.INFO)

    # Create a file handler and set level to info.
    log_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'FLAC-CNV.log')
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.INFO)

    # Create and add the formatter
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    fh.setFormatter(formatter)

    # Add fh to the logger.
    logger.addHandler(fh)
    return logger


def convert_mseed_to_flac(mseed_dir, mseed_files):
    for mseed_filename in mseed_files:
        filename, file_extension = os.path.splitext(mseed_filename)
        flac_file = os.path.join(mseed_dir, filename + '.flac')

        # If this flac file does not already exist, convert it.
        if not os.path.exists(flac_file):
            mseed_file = os.path.join(mseed_dir, mseed_filename)
            stream = obspy.read(mseed_file)

            with SoundFile(flac_file, 'w', 64000, 1, subtype='PCM_24') as soundFile:
                soundFile.write(stream[0].data / MAX_MSEED_VALUE)

            log.info('Converted ' + mseed_file + ' to ' + flac_file)


def main():
    # Get the command line options
    options = docopt.docopt(__doc__)
    mseed_dir_or_file = options['<mseed_dir_or_file>']
    mseed_dir_or_file = os.path.expanduser(mseed_dir_or_file)
    mseed_dir_or_file = os.path.abspath(mseed_dir_or_file)

    # If this is a directory, recursively walk through the directory structure converting mseed files to flac files.
    if os.path.isdir(mseed_dir_or_file):
        mseed_exists = False
        for mseed_dir, dirnames, filenames in os.walk(mseed_dir_or_file):
            mseed_filenames = fnmatch.filter(filenames, '*.mseed')
            if mseed_filenames:
                convert_mseed_to_flac(mseed_dir, mseed_filenames)
                mseed_exists = True

        if not mseed_exists:
            err_msg = 'No *.mseed files exist in the directory tree, starting at: ' + mseed_dir_or_file
            log.error(err_msg)

    # If this is a file, check if it's a .mseed file and convert it.
    elif os.path.isfile(mseed_dir_or_file):
        mseed_root_dir, mseed_file = os.path.split(mseed_dir_or_file)
        mseed_files = [mseed_file]

        filename, file_extension = os.path.splitext(mseed_file)
        if file_extension != '.mseed':
            err_msg = 'File does not have the .mseed extension:' + mseed_dir_or_file
            log.error(err_msg)
        else:
            convert_mseed_to_flac(mseed_root_dir, mseed_files)

    else:
        err_msg = 'Directory or file name does not exist: ' + mseed_dir_or_file
        log.error(err_msg)


if __name__ == '__main__':
    log = get_logger()
    main()
