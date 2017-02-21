#!/bin/bash
# -------------------------------------------------------------------------
# Start, stop, or restart the video recording for CAMHD on elemental live
# -------------------------------------------------------------------------

LOG="/data/server/camhd/control.log"

function usage {
        echo "record.sh (--start | --stop | --restart | --rollover | --usage)"
}

if [[ $# -eq 1 ]]; then
        key="$1"

        case $key in
                --start)
                do_start=1
                ;;
                --stop)
                do_stop=1
                ;;
                --restart)
                do_start=1
                do_stop=1
                ;;
                --rollover)
                do_rollover=1
                ;;
                --usage)
                usage
                exit 0
                ;;
                *)
                echo "Error - invalid option specified: " $key
                usage
                exit 1
                ;;
        esac
        shift
else
	echo "Error - missing required argument"
	usage
	exit 1
fi

if [ $do_stop ]; then
	echo `date` "Info - Stopping recording" >> $LOG
	curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml" -d "<group_id>27</group_id>" "http://209.124.182.238/api/live_events/4/stop_output_group" 2>&1 >> $LOG
	if [ $do_start ]; then
		# give the mp4 recording enough time to close out before starting again
		sleep 10
	fi
fi

if [ $do_start ]; then
	echo `date` "Info - Starting recording" >> $LOG
	curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml" -d "<group_id>27</group_id>" "http://209.124.182.238/api/live_events/4/start_output_group" 2>&1 >> $LOG
fi

if [ $do_rollover ]; then
        echo `date` "Info - Rollover recording" >> $LOG
        curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml" -d "<output_id>111</output_id>" "http://209.124.182.238/api/live_events/4/rollover_output" 2>&1 >> $LOG
        curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml" -d "<output_id>112</output_id>" "http://209.124.182.238/api/live_events/4/rollover_output" 2>&1 >> $LOG
fi

exit 0
