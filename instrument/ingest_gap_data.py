#!/usr/bin/env python
"""
Usage:
    ingest_gap_data.py <playback_config_file> <"gap_glob">
"""

import logging
import yaml
import docopt
import threading
import curses
import sys
from subprocess import call
from time import sleep
from sys import stdout
from os import path, mkdir

DEFAULT_PLAYBACK_THREADS = 5
MAX_PLAYBACK_THREADS = 15
SPINNER_CHAR = ['|', '/', '-', '\\']

log = None
playback_list = {}
list_lock = threading.Lock()
done_displaying = False
ingest_log_filename = ''
playback_logs_dir = ''
num_playback_threads = DEFAULT_PLAYBACK_THREADS
gap_glob = ''


def get_logger():
    global ingest_log_filename

    logger = logging.getLogger('gap_ingest')
    logger.setLevel(logging.INFO)

    # Create a file handler and set level to info.
    ingest_log_filename = 'ingest_gap_data_' + gap_glob + '.log'
    fh = logging.FileHandler(ingest_log_filename)
    fh.setLevel(logging.INFO)

    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Add formatter to fh.
    fh.setFormatter(formatter)

    # Add fh to the logger.
    logger.addHandler(fh)
    return logger


def get_key_input(stdscr):
    global num_playback_threads

    if stdscr.getch() == 27:
        if stdscr.getch() == 91:
            command = stdscr.getch()
            if command == 65:
                num_playback_threads += 1
                if num_playback_threads > MAX_PLAYBACK_THREADS:
                    num_playback_threads = MAX_PLAYBACK_THREADS
            elif command == 66:
                num_playback_threads -= 1
                if num_playback_threads < 1:
                    num_playback_threads = 1


def display_playback_progress():
    stdscr = curses.initscr()
    curses.noecho()
    curses.cbreak()
    stdscr.keypad(True)
    stdscr.nodelay(True)

    while not done_displaying:
        stdout.write('\033c\r\n')
        stdout.write('Ingesting Data :\r\n\n')
        stdout.write('\tDate Glob     : ' + gap_glob + '\r\n')
        stdout.write('\tMax Playbacks : ' + str(num_playback_threads) + '\r\n\n')
        stdout.write('\tInstruments:\r\n')

        list_lock.acquire()
        for refdes in playback_list.keys():
            stdout.write('\t\t' + refdes + ' ' + SPINNER_CHAR[playback_list[refdes]] + '\r\n')
            playback_list[refdes] += 1
            if playback_list[refdes] == SPINNER_CHAR.__len__():
                playback_list[refdes] = 0
        list_lock.release()

        if not done_displaying:
            get_key_input(stdscr)
            stdscr.refresh()
            sleep(0.2)

    stdscr.keypad(False)
    curses.echo()
    curses.nocbreak()
    curses.endwin()

    stdout.write('\033c\n\r')
    stdout.write('\tIngest Log File    : ' + ingest_log_filename + '\r\n')
    stdout.write('\tPlayback Log Files : ' + playback_logs_dir + '\r\n\n')


def execute_playback_command(playback_args, instrument):
    # Add the instrument to the playback list for the progress display.
    list_lock.acquire()
    playback_list[instrument] = 0
    list_lock.release()

    # Execute the playback command and block until it's complete.
    log.info(threading.currentThread().getName() + '\n     playback ' + playback_args)
    call('playback ' + playback_args, shell=True)

    # Remove the instrument from the playback list to be removed from the progress display.
    list_lock.acquire()
    del playback_list[instrument]
    list_lock.release()


def process_playback_commands(playback_config_dict):
    # Start the display playback progress process
    pb_thread = threading.Thread(target=display_playback_progress, name='displayThread')
    pb_thread.start()

    rsn_data_dir = playback_config_dict['rsn_data_dir']
    qpid_server = playback_config_dict['qpid_server']
    instruments = playback_config_dict['instruments']

    playback_number = 0
    initial_active_threads = threading.active_count()
    for instrument in instruments:
        # Generate the playback command.
        log_filename = instrument['refdes'][:-9] + instrument['file_prefix']

        playback_args = ' '.join([
            instrument['datatype'],
            instrument['driver'],
            instrument['refdes'],
            'qpid://guest/guest@%s?queue=Ingest.instrument_events' % qpid_server,
            'qpid://guest/guest@%s?queue=Ingest.instrument_particles' % qpid_server,
            path.join(rsn_data_dir, instrument['node_dir'], '%s*%s*' % (instrument['file_prefix'], gap_glob)),
            '2>&1 >',
            path.join(playback_logs_dir, log_filename + '.log')
        ])

        # Wait for an available thread to spawn the playback command.
        while threading.active_count() - initial_active_threads >= num_playback_threads:
            sleep(1)

        # Create a playback thread for the current command and start it.
        playback_number += 1
        thread_name = 'PlaybackThread-' + instrument['refdes'] + '-' + str(playback_number)
        pb_thread = threading.Thread(target=execute_playback_command,
                                     name=thread_name, args=(playback_args, log_filename))
        pb_thread.start()

    # Wait for the final playback commands to complete.
    while threading.active_count() > initial_active_threads:
        sleep(1)


def main():
    global gap_glob
    global log
    global playback_logs_dir
    global done_displaying

    options = docopt.docopt(__doc__)

    # Get the command line options
    playback_config_file = options['<playback_config_file>']
    gap_glob = options['<"gap_glob">']

    log = get_logger()

    try:
        # Process the config file and start the playback processing.
        playback_config_dict = yaml.load(open(playback_config_file))

        playback_logs_dir = playback_config_dict['logs_dir']
        if not path.exists(playback_logs_dir):
            mkdir(playback_logs_dir)

        process_playback_commands(playback_config_dict)

        done_displaying = True
        sleep(0.5)
        log.info('Playbacks Completed Successfully!!')
        print '\tPlaybacks Completed Successfully!!\n'

    except IOError:
        log.error('Could not open file: ' + playback_config_file)
        print '\nIOError: Could not open file: ' + playback_config_file + '\n'

    except OSError:
        log.error('OSError attempting to create the directory: ' + playback_logs_dir)
        print '\nOSError attempting to create the directory: ' + playback_logs_dir + '\n'

    except:
        done_displaying = True
        sleep(0.5)
        log.error('Unexpected Error: ' + str(sys.exc_info()))
        print 'Unexpected Error: ' + str(sys.exc_info()) + '\n'


if __name__ == '__main__':
    main()
