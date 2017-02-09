#!/home/elemental/miniconda2/bin/python
import os
import datetime
import logging
import hashlib
import shlex

import subprocess
from tendo import singleton

"""
archive.py - archive video files to the SAN

Archive video files per convention. e.g.: CAMHDA301-20161120T000000Z.mov
Allows 5 minutes to properly close the files before archival.

Logs information and errors to the control.log file. Sends a notification email when an error is encountered.
"""

# only allow one instance of the script to run at a time
instance = singleton.SingleInstance()

camera = 'CAMHDA301'
date_format = '%Y%m%dT%H%M00'
output_dir = '/data/server/camhd'
user = 'asadev'
remote_host = 'ciw-aiad.intra.oceanobservatories.org'
remote_dest = '/san_hdv/RS03ASHS-PN03B-06-CAMHDA301/'
log_file = os.path.join(output_dir, 'control.log')

recipients = 'danmergens@gmail.com,help@ooi.rutgers.edu'

logging.basicConfig(
    level=logging.INFO,
    filename=log_file,
    format='%(asctime)s %(message)s')
logging.getLogger().addHandler(logging.StreamHandler())


def notify(message_text):
    import smtplib
    from email.mime.text import MIMEText

    body = \
        'An error occurred during the archive of the video to the SAN.\n\n%s\n\n' \
        'Check the logfile (%s) on the elemental server for additional details.' % (message_text, log_file)
    msg = MIMEText(body)
    msg['Subject'] = 'CAMHD - Video Archive Copy Failed'
    msg['From'] = 'donotreply@oceanobservatories.org'
    msg['To'] = recipients

    s = smtplib.SMTP('localhost')
    s.sendmail(msg['From'], [msg['To']], msg.as_string())


def hash_bytestr_iter(bytesiter, hasher):
    for block in bytesiter:
        hasher.update(block)
    return hasher.hexdigest()


def file_as_blockiter(afile, blocksize=65536):
    with afile:
        block = afile.read(blocksize)
        while len(block) > 0:
            yield block
            block = afile.read(blocksize)


def generate_checksum(filename, create_file=False):
    """
    Generate an MD5 checksum and write to md5_filename (if supplied)
    :param filename:  file to checksum
    :param create_file:   if set, write a paired md5 output file
    :return:  (checksum hex string, md5 filename)
    """
    md5_file = None
    checksum = hash_bytestr_iter(file_as_blockiter(open(filename, 'rb')), hashlib.md5())
    if create_file:
        md5_file = filename + '.md5'
        with open(md5_file, 'w') as f:
            f.write(checksum)
            f.write('\n')
    return checksum, md5_file


def execute(cmd):
    args = shlex.split(cmd)
    output = '{}'
    try:
        output = subprocess.check_output(args).decode('utf-8')
    except subprocess.CalledProcessError as e:
        logging.error('return failure (%s) executing command (%s)', e.returncode, cmd)
    return output


def publish_file(local_file, user, scp_host, remote_file):
    """
    Publish a file to production
    :param local_file:  filename on local disk
    :param user:  username for scp_host
    :param scp_host:  hostname of the remote machine
    :param remote_file:  filename to publish as
    """
    result = os.system('scp %s %s@%s:%s' % (local_file, user, scp_host, remote_file))
    if result == 0:
        message = 'Archiving %s to %s' % (local_file, remote_file)
        logging.info(message)
        os.remove(local_file)
    else:
        error = 'Failed to archive %s to %s' % (local_file, remote_file)
        logging.error(error)
        notify(error)


def test_generate_checksum():
    filename = '/Users/danmergens/camhd/CAMHDA301-20161231T000000Z.mp4'
    checksum, md5_filename = generate_checksum(filename, create_file=True)
    md5_cmd = 'md5 %s' % filename
    md5_output = execute(md5_cmd).split(' ')[-1].strip()
    assert checksum == md5_output
    with open(md5_filename, 'r') as f:
        checksum_from_file = f.readline().strip()
        assert checksum_from_file == md5_output


# test_generate_checksum()

files = os.listdir(output_dir)

for f in files:
    _, extension = os.path.splitext(f)
    if extension not in ['.mp4', '.mov']:
        continue
    video_file = os.path.join(output_dir, f)
    update_time = datetime.datetime.fromtimestamp(os.stat(video_file).st_mtime)
    now = datetime.datetime.now()
    age = (now - update_time).total_seconds() / 60  # time in minutes
    # only archive files that have not been updated in the last 10 minutes
    if age > 10:
        archive_file = camera + '-' + update_time.strftime(date_format) + extension
        year = str(update_time.year)
        month = '%02d' % update_time.month
        day = '%02d' % update_time.day
        archive_path = os.path.join(remote_dest, year, month, day)
        archive_file = os.path.join(archive_path, archive_file)
        result = os.system('ssh %s@%s mkdir -p %s' % (user, remote_host, archive_path))
        if result != 0:
            error = 'Unable to create directory on remote server (%s:%s)' % (remote_host, archive_path)
            logging.error(error)
            notify(error)
            break

        _, md5_file = generate_checksum(video_file, create_file=True)
        md5_remote_file = archive_file + '.md5'

        publish_file(video_file, user, remote_host, archive_file)
        publish_file(md5_file, user, remote_host, md5_remote_file)

