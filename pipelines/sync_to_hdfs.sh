#!/bin/bash

LOCAL_FILE="/shared/local_fan_sales.json"
HDFS_FILE="/epl_raw/fan_sales.json"
INTERVAL=5   # seconds between syncs

echo "Starting sync loop..."
echo "Local file: $LOCAL_FILE"
echo "HDFS file:  $HDFS_FILE"
echo "Interval:   $INTERVAL seconds"
echo

while true; do
    if [ -f "$LOCAL_FILE" ]; then
        echo "[`date`] Syncing local_fan_sales.json to HDFS..."
        hdfs dfs -put -f "$LOCAL_FILE" "$HDFS_FILE"
        echo "Done."
    else
        echo "[`date`] Local file not found: $LOCAL_FILE"
    fi

    sleep $INTERVAL
done

