# ooi-config/elemental

Video configuration files for controlling the recording and archival of CAMHD video files.

These files should be installed on the Elemental Live server in the /data/server/camhd directory.

record.sh
----------

Script for starting/stopping elemental server MP4 and ProRes recordings of the CAMHD feed.

Can be called with the following options:

```
--start    - starts recording (has no effect if a recording is already in progress)
--stop     - stops the active recording (has no effect if no recording is in progress)
--restart  - stops and restarts the current recording (or starts a recording if there is not a recording in progress)
--rollover - rollover video output to new files
--usage    - prints usage statement
```

archive.py
----------

Script to rename and archive completed movie files to the SAN. Copies all MP4 and ProRes (.mov) files to the appropriate CAMHD folder on the SAN
using the current year and month.  Renames the files with the current date and hour timetag (e.g. CAMHDA301-20161108T000000Z.mp4). 
Compares the timestamp of the video files and only archives files that have not been modified in the last
ten (10) minutes. Adds a matching MD5 file with a checksum users can download to insure data integrity of
the associated video file.

archive.cron
------------

Crontab file use to define the CAMHD recording schedule. The clocks on the elemental live server and the CAMHD controller server should be 
synchronized (at least to a common ntp time service) to ensure video is not lost. The current schedule is:

 1. 72 hour collect starting at 00:00 UTC on the first day of each month
 2. 24 hour collect starting at 00:00 UTC on the 10th and 20th day of each month
 3. 15 minute collect starting at 00:00 UTC, every 3rd hour, every day

This file is loaded using the crontab utility using the following command:

```
crontab /data/server/camhd/archive.cron
```

To confirm the cron jobs have been loaded, use the following command:

```
crontab -l
```

# Revision History

* 2017-02-15 - Corrected problem with 15 minute video clips being split into two parts.
* 2017-02-09 - Added MD5 checksum
* 2016-12-02 - Restructured video recording. Added 72/24 hour collects and extended short recordings from
fourteen (14) to fifteen (15) minutes. 
