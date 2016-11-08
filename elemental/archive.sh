#!/bin/bash
# rename video files and archive to SAN
# -------------------------------------------------------------------------

SUBJECT="CAMHD - Video Archive Copy Failed"
FROM="donotreply@oceanobservatories.org"
TO="dan.mergens@gmail.com,help@ooi.rutgers.edu"
EMAILMESSAGE="/tmp/copyfailed.txt"

host=asadev@aiad.ooi.rutgers.edu
root=/san_data/RS03ASHS-PN03B-06-CAMHDA301/
dest=/san_data/RS03ASHS-PN03B-06-CAMHDA301/$(date +20%y/%m/%d)

ssh $host mkdir -p $dest

cd /data/server/camhd

# archive - save video file to SAN permanent archive with formatted filename
# $1 - video file to be archived, can be either mp4 or mov (ProRes) format
function archive {
        filename=$(basename "$1")
        ext="${filename##.}"
        f=CAMHDA301-`date +20%y%m%dT%H0000Z`.$ext

        # only rename the file if there is no active recording, or the file is from a previous recording
        if [ ! -f .recording ] || [ $1 -ot .recording ]; then
                echo "Info - archiving $1 as $f"
                mv $1 $f
                scp $f $host:$dest
                if [ $? -ne 0 ]; then
                        echo "Failed to copy video archive file $f from elemental to aiad." >> $EMAILMESSAGE
                        mail -r "$FROM" -s "$SUBJECT" "$TO" < $EMAILMESSAGE
                        rm $EMAILMESSAGE
                else
                        rm $f
                fi
        fi
}

# Rename MP4 and MOV files with current hour timestamp
for i in $(ls camhd_*.{mp4,mov} 2>/dev/null); do
        archive $i
done
