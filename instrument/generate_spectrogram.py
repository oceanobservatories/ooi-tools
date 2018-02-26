#!/usr/bin/env python
"""
Description:
    Generate spectrogram movie from input mseed file. The result is an MP4 (lib264 video codec) still image with
    high-pass filter audio file converted the mseed file.

Usage:
    generate_spectrogram.py [--highpass=<freq>] [--samplerate=<rate>] FILE
    generate_spectrogram.py -h | --help
    generate_spectrogram.py --version

Arguments:
    FILE    mseed hydrophone data filename

Options:
    -h --help            Display usage statement.
    --highpass=<freq>    Frequency of the highpass filter in Hz [default: 1000].
    --samplerate=<rate>  Sample rate [default: 64000].
"""

import os
from docopt import docopt
import logging
import subprocess
import obspy
from soundfile import SoundFile


__author__ = 'Dan Mergens'
__license__ = 'Apache 2.0'

log = None


def get_logger():
    logger = logging.getLogger('SPEC_MP4')
    logger.setLevel(logging.INFO)

    # Create a file handler and set level to info.
    log_filename = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'SPEC-MP4.log')
    fh = logging.FileHandler(log_filename)
    fh.setLevel(logging.INFO)

    # Create and add the formatter
    formatter = logging.Formatter('%(asctime)s: %(levelname)s: %(message)s')
    fh.setFormatter(formatter)

    # Add fh to the logger.
    logger.addHandler(fh)
    return logger


def generate_spectrogram(mseed_file, highpass, samplerate):
    """
    Execute a high pass filter on the meed input. Create a still image movie of the spectrogram with the resulting
    flac audio.

    :param mseed_file:  Hydrophone audio file in mseed format
    :param highpass:    High pass frequency for filtering
    :return:  output video filename if successful, otherwise None
    """

    root, ext = os.path.splitext(mseed_file)
    if ext != '.mseed':
        log.error('input file (%s) must be mseed format' % mseed_file)
        return None

    stream = obspy.read(mseed_file)
    filtered = stream.copy()

    # generate plot image
    image = '%s.png' % root
    filtered.filter('highpass', freq=highpass)
    filtered.plot(color='blue', outfile=image)

    # generate audio file
    audio = '%s.flac' % root
    filtered.normalize()
    with SoundFile(audio, 'w', samplerate, 1) as f:
        f.write(filtered[0].data)

    # generate movie clip
    clip = '%s.mp4' % root
    try:
        subprocess.check_call(
            ['ffmpeg', '-loop', '1', '-i', image, '-i', audio,
             '-c:v', 'libx264', '-tune', 'stillimage',  # video codec, tuned for still image
             '-c:a', 'aac', '-b:a', '192k',  # audio codec
             '-pix_fmt', 'yuv420p',
             '-shortest', clip])  # output options
        log.info('successfully created clip: %s' % clip)

    except subprocess.CalledProcessError as e:
        log.error('failed to create clip: %s - %r' % (clip, e))
        return None

    return clip


def main():
    options = docopt(__doc__, version='0.1.0')

    highpass = options['--highpass']

    try:
        highpass = float(highpass)
    except ValueError as e:
        log.error('invalid frequency (%r) supplied for --highpass: must be of type float - %r' %
                  (options['--highpass'], e))
        return 1

    samplerate = options['--samplerate']
    try:
        samplerate = int(samplerate)
    except ValueError as e:
        log.error('invalid samplerate (%r) supplied for --samplerate: must be of type int - %r' %
                  (options['--samplerate'], e))

    generate_spectrogram(options['FILE'], highpass, samplerate)


if __name__ == '__main__':
    log = get_logger()
    main()
