#!/home/elemental/miniconda2/bin/python
import os
import datetime
import logging
from tendo import singleton

"""
archive.py - archive video files to the SAN

Archive video files per convention. e.g.: CAMHDA301-20161120T000000Z.mov
Assumes that the video files are no longer than 15 minutes, and allows 5 minutes to properly close the files before
archival.

Logs information and errors to the control.log file. Sends a notification email when an error is encountered.
"""

# only allow one instance of the script to run at a time
instance = singleton.SingleInstance()

camera='CAMHDA301'
date_format = '%Y%m%dT%H%M00'
output_dir = '/data/server/camhd'
user = 'asadev'
remote_host = 'aiad.ooi.rutgers.edu'
remote_dest = '/san_data/RS03ASHS-PN03B-06-CAMHDA301/'
log_file = os.path.join(output_dir, 'control.log')

recipients = 'danmergens@gmail.com,help@ooi.rutgers.edu'

logging.basicConfig(
    level=logging.INFO,
    filename=log_file,
    format='%(asctime)s %(message)s')

def notify(message):
    import smtplib
    from email.mime.text import MIMEText

    message = \
        'An error occurred during the archive of the video to the SAN.\n\n%s\n\n' \
        'Check the logfile (%s) on the elemental server for additional details.' % (message, log_file)
    msg = MIMEText(message)
    msg['Subject'] = 'CAMHD - Video Archive Copy Failed'
    msg['From'] = 'donotreply@oceanobservatories.org'
    msg['To'] = recipients

    s = smtplib.SMTP('localhost')
    s.sendmail(msg['From'], [ msg['To'] ], msg.as_string())

files = os.listdir(output_dir)

for f in files:
    _, extension = os.path.splitext(f)
    if extension not in ['.mp4', '.mov']:
        continue
    video_file = os.path.join(output_dir, f)
    create_time = datetime.datetime.fromtimestamp(os.stat(video_file).st_ctime)
    update_time = datetime.datetime.fromtimestamp(os.stat(video_file).st_mtime)
    logging.info('file: %s, created %s, last updated %s', f, create_time, update_time)
    now = datetime.datetime.now()
    age = (now - update_time).total_seconds() / 60  # time in minutes
    # only archive files that have not been updated in the last 5 minutes
    if age > 5:
        archive_file = camera + '-' +  create_time.strftime(date_format) + extension
        year = str(create_time.year)
        month = '%02d' % create_time.month
        day = '%02d' % create_time.day
        archive_path = os.path.join(remote_dest, year, month, day)
        archive_file = os.path.join(archive_path, archive_file)
        result = os.system('ssh %s@%s mkdir -p %s' % (user, remote_host, archive_path))
        if result != 0:
            error = 'Unable to create directory on remote server (%s:%s)' % (remote_host, archive_path)
            logging.error(error)
            notify(error)
            print 'Error: %s' % error
            continue;

        result = os.system('scp %s %s@%s:%s' % (video_file, user, remote_host, archive_file))
        if result == 0:
            message = 'Archiving %s to %s' % (video_file, archive_file)
            logging.info(message)
            print 'Info: %s' % message
            os.remove(video_file)
        else:
            error = 'Failed to archive %s to %s' % (video_file, archive_file)
            logging.error(error)
            notify(error)
            print 'Error: %s' % error
