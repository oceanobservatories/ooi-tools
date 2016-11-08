#!/bin/bash
# rename video files and archive to SAN
# -------------------------------------------------------------------------

SUBJECT="CAMHD - Video Archive Copy Failed"
FROM="donotreply@oceanobservatories.org"
TO="dan.mergens@gmail.com"
#TO="dan.mergens@gmail.com,help@ooi.rutgers.edu"
EMAILMESSAGE="/tmp/copyfailed.txt"

host=asadev@aiad.ooi.rutgers.edu
root=/san_data/RS03ASHS-PN03B-06-CAMHDA301/
dest=/san_data/RS03ASHS-PN03B-06-CAMHDA301/`date +20%y/%m/%d`

ssh $host 'cd /san_data/RS03ASHS-PN03B-06-CAMHDA301/; mkdir -p `date +20%y/%m/%d`'

cd /data/server/camhd

function archive {
        if [ ! -f $i ]; then
                echo "Error - no such filename: " $i
                return 0
        fi
        i=$1
        filename=$(basename "$1")
        ext="${filename##.}"
        f=CAMHDA301-`date +20%y%m%dT%H0000Z`.$ext

        # only rename the file if there is no active recording, or the file is from a previous recording
        if [ ! -f .recording ] || [ $i -ot .recording ]; then
                echo "Info - archiving " $i " as " $f
                mv $i $f
                scp $f $host:$dest
                if [ $? -ne 0 ]; then
                        echo "Failed to copy video archive file $f from elemental to aiad." >> $EMAILMESSAGE
                        cd /usr/bin
                        mail -r "$FROM" -s "$SUBJECT" "$TO" < $EMAILMESSAGE
                        rm $EMAILMESSAGE
                else
                        rm $f
                fi
        fi
}

# Rename MP$ and MOV files with current hour timestamp
for i in $(ls camhd_*.{mp4,mov} 2>/dev/null); do
        archive $i
done
