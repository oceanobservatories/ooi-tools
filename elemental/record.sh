#!/bin/bash
# -------------------------------------------------------------------------
# Start, stop, or restart the video recording for CAMHD on elemental live
# -------------------------------------------------------------------------

function usage {
        echo "record.sh (--start | --stop | --restart) [--usage]"
}

while [[ $# -gt 0 ]]
        do
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
                --usage)
                usage
                exit 0
                ;;
                *)
                echo "Error - invalid option specified: " $key
                usage
                exit 0
                ;;
        esac
        shift
done

if [ $do_stop ]; then
        if [ -f .recording ]; then
                echo "Info - stopping"
                # curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml" -d "<group_id>27</group_id>" http://209.124.182.238/api/live_events/4/stop_output_group
                rm .recording
        else
                echo "Warning - no recording to stop"
        fi
fi

if [ $do_start ]; then
        if [ ! -f .recording ]; then
                echo "Info - starting"
                touch .recording
                # curl -X POST -H "Accept: application/xml" -H "Content-type: application/xml" -d "<group_id>27</group_id>" http://209.124.182.238/api/live_events/4/start_output_group
        else
                echo "Warning - recording already in progress - use --stop or --restart to interrupt"
        fi
fi

exit 0
