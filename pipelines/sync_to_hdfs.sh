#!/bin/bash

# -------------------------------
# CONFIG
# -------------------------------
LOCAL_FILE="/home/kofi/Downloads/EPL_Pipeline/pipelines/local_fan_sales.json"
CONTAINER_PATH="/tmp/local_fan_sales.json"
CONTAINER_NAME="a03fb6265454_namenode"
HDFS_FILE="/epl_raw/fan_sales.json"
INTERVAL=5  # seconds
# -------------------------------

GREEN="\033[32m"
RED="\033[31m"
YELLOW="\033[33m"
NC="\033[0m"

echo -e "${GREEN}Starting Docker-based HDFS sync loop...${NC}"
echo "Local file:     $LOCAL_FILE"
echo "Container file: $CONTAINER_PATH"
echo "HDFS file:      $HDFS_FILE"
echo "Interval:       ${INTERVAL}s"
echo

while true; do
    TIMESTAMP=$(date)

    if [ ! -f "$LOCAL_FILE" ]; then
        echo -e "[${TIMESTAMP}] ${RED}Local file not found:${NC} $LOCAL_FILE"
        sleep $INTERVAL
        continue
    fi

    echo -e "[${TIMESTAMP}] ${YELLOW}Copying file into Hadoop container...${NC}"
    docker cp "$LOCAL_FILE" "$CONTAINER_NAME:$CONTAINER_PATH" >/dev/null 2>&1

    if [ $? -ne 0 ]; then
        echo -e "[${TIMESTAMP}] ${RED}Failed to copy file into container!${NC}"
        sleep $INTERVAL
        continue
    fi

    echo -e "[${TIMESTAMP}] ${YELLOW}Uploading to HDFS...${NC}"
    docker exec "$CONTAINER_NAME" hdfs dfs -put -f "$CONTAINER_PATH" "$HDFS_FILE" >/dev/null 2>&1

    if [ $? -eq 0 ]; then
        echo -e "[${TIMESTAMP}] ${GREEN}Sync complete.${NC}"
    else
        echo -e "[${TIMESTAMP}] ${RED}HDFS upload failed!${NC}"
    fi

    sleep $INTERVAL
done

