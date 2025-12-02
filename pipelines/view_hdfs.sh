#!/bin/bash

# HDFS Viewer Script (runs from HOST using Docker)
CONTAINER_NAME="a03fb6265454_namenode"

GREEN="\033[32m"
YELLOW="\033[33m"
RED="\033[31m"
NC="\033[0m"

if [ $# -eq 0 ]; then
    echo -e "${YELLOW}Usage:${NC}"
    echo "  ./hdfs_view.sh ls <path>"
    echo "  ./hdfs_view.sh cat <file>"
    echo "  ./hdfs_view.sh head <file>"
    echo "  ./hdfs_view.sh tail <file>"
    echo "  ./hdfs_view.sh stat <file>"
    exit 1
fi

COMMAND=$1
TARGET=$2

case $COMMAND in

  ls)
    echo -e "${GREEN}Listing HDFS path: $TARGET${NC}"
    docker exec -it "$CONTAINER_NAME" hdfs dfs -ls "$TARGET"
    ;;

  cat)
    echo -e "${GREEN}Showing contents of: $TARGET${NC}"
    docker exec -it "$CONTAINER_NAME" hdfs dfs -cat "$TARGET"
    ;;

  head)
    echo -e "${GREEN}First 20 lines of: $TARGET${NC}"
    docker exec -it "$CONTAINER_NAME" hdfs dfs -cat "$TARGET" | head -n 20
    ;;

  tail)
    echo -e "${GREEN}Last 20 lines of: $TARGET${NC}"
    docker exec -it "$CONTAINER_NAME" hdfs dfs -cat "$TARGET" | tail -n 20
    ;;

  stat)
    echo -e "${GREEN}HDFS file info: $TARGET${NC}"
    docker exec -it "$CONTAINER_NAME" hdfs dfs -stat "%n | %b bytes | %y" "$TARGET"
    ;;

  *)
    echo -e "${RED}Unknown command: $COMMAND${NC}"
    ;;

esac

