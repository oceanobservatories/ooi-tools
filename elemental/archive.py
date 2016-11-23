#!/home/elemental/miniconda2/bin/python
import os
import datetime
import logging
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
#remote_host = 'aiad.ooi.rutgers.edu'
remote_host = 'ciw-aiad.intra.oceanobservatories.org'
#remote_dest = '/san_data/RS03ASHS-PN03B-06-CAMHDA301/'
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

        result = os.system('scp %s %s@%s:%s' % (video_file, user, remote_host, archive_file))
        if result == 0:
            message = 'Archiving %s to %s' % (video_file, archive_file)
            logging.info(message)
            os.remove(video_file)
        else:
            error = 'Failed to archive %s to %s' % (video_file, archive_file)
            logging.error(error)
            notify(error)
