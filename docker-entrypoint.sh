#!/bin/bash
set -e

# Check if config file is provided
if [ -z "$1" ]; then
    echo "Error: No config file provided"
    echo "Usage: docker run -v /path/to/logs:/var/log/etl -v /path/to/config.yml:/app/config.yml etl-container config.yml"
    exit 1
fi

CONFIG_FILE="$1"
LOG_FILE="/var/log/etl/etl_$(date +%Y%m%d).log"

# Start logrotate in the background
logrotate -f /etc/logrotate.d/etl &

# Run the ETL process with the provided config
python controller.py "$CONFIG_FILE" 2>&1 | tee -a "$LOG_FILE" 