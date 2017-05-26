#!/bin/bash
# -------------------------------------------------------------------------
# Start, stop, or restart the video recording for CAMHD on elemental live
# -------------------------------------------------------------------------

source /data/server/camhd/auth.sh
AUTH="--login $ELEMENTAL_USER --api-key $ELEMENTAL_KEY"

LOG="/data/server/camhd/control.log"
ACCEPT="Accept: application/xml"
CONTENT="Content-type: application/xml"
GROUP_ID="<group_id>27</group_id>"
STOP_GROUP="http://209.124.182.238/api/live_events/4/stop_output_group"
START_GROUP="http://209.124.182.238/api/live_events/4/start_output_group"
ROLLOVER="http://209.124.182.238/api/live_events/4/rollover_output"
CURL=/opt/elemental_se/web/public/authentication_scripts/auth_curl.pl
CURL_AUTH="$CURL $AUTH"

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
	$CURL_AUTH -H "$ACCEPT" -H "$CONTENT" -d "$GROUP_ID" $STOP_GROUP 2>&1 >> $LOG
	if [ $do_start ]; then
		# give the mp4 recording enough time to close out before starting again
		sleep 10
	fi
fi

if [ $do_start ]; then
	echo `date` "Info - Starting recording" >> $LOG
	$CURL_AUTH -H "$ACCEPT" -H "$CONTENT" -d "$GROUP_ID" $START_GROUP 2>&1 >> $LOG
	# $POST $START
fi

if [ $do_rollover ]; then
	echo `date` "Info - Rollover recording" >> $LOG
	$CURL_AUTH -H "$ACCEPT" -H "$CONTENT" -d "<output_id>111</output_id>" $ROLLOVER 2>&1 >> $LOG
	$CURL_AUTH -H "$ACCEPT" -H "$CONTENT" -d "<output_id>112</output_id>" $ROLLOVER 2>&1 >> $LOG
fi

exit 0
