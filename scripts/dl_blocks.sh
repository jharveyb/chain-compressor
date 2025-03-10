#!/bin/bash

# Default values
START=""  # No default, now required
WINDOW=1000
END=""    # No default, now required
REMOTE_HOST=""
REMOTE_PATH=""
LOCAL_PATH="."
SUFFIX="blk"

# Function to download a file using rsync
download_file() {
    local filename="$1"
    echo "Downloading $filename..."
    rsync -aPhu --inplace --info=NAME0,PROGRESS2,MISC,STATS,REMOVE,SKIP \
    --compress-choice=zstd --compress-level=1 \
    "${REMOTE_HOST}:${REMOTE_PATH}/${filename}" "${LOCAL_PATH}/"
    return $?
}

# Function to display usage information
usage() {
    echo "Usage: $0 [OPTIONS]"
    echo "Download files from a remote system where filenames are numbers with a fixed suffix."
    echo
    echo "Options:"
    echo "  --start NUMBER    Starting number (required)"
    echo "  --end NUMBER      Maximum block number to download (required)"
    echo "  --window NUMBER   Window size for increments (default: $WINDOW)"
    echo "  --host HOST       Remote host (required)"
    echo "  --remote-path PATH Remote directory path (required)"
    echo "  --local-path PATH Local directory to save files (default: current directory)"
    echo "  --suffix SUFFIX   File suffix (default: $SUFFIX)"
    echo "  --help            Display this help message and exit"
    echo
    echo "Example:"
    echo "  $0 --start 0 --end 5000 --window 1000 --host example.com --remote-path /path/to/files"
    exit 1
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case "$1" in
        --start)
            START="$2"
            shift 2
            ;;
        --window)
            WINDOW="$2"
            shift 2
            ;;
        --end)
            END="$2"
            shift 2
            ;;
        --host)
            REMOTE_HOST="$2"
            shift 2
            ;;
        --remote-path)
            REMOTE_PATH="$2"
            shift 2
            ;;
        --local-path)
            LOCAL_PATH="$2"
            shift 2
            ;;
        --suffix)
            SUFFIX="$2"
            shift 2
            ;;
        --help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required parameters
if [[ -z "$START" ]]; then
    echo "Error: Start value is required. Use --start option."
    usage
fi

if [[ -z "$END" ]]; then
    echo "Error: End value is required. Use --end option."
    usage
fi

if [[ -z "$REMOTE_HOST" ]]; then
    echo "Error: Remote host is required. Use --host option."
    usage
fi

if [[ -z "$REMOTE_PATH" ]]; then
    echo "Error: Remote path is required. Use --remote-path option."
    usage
fi

# Make sure START is a number
if ! [[ "$START" =~ ^[0-9]+$ ]]; then
    echo "Error: Start value must be a non-negative integer."
    exit 1
fi

# Make sure WINDOW is a positive number
if ! [[ "$WINDOW" =~ ^[0-9]+$ ]] || [[ "$WINDOW" -eq 0 ]]; then
    echo "Error: Window value must be a positive integer."
    exit 1
fi

# Validate END if specified
    if ! [[ "$END" =~ ^[0-9]+$ ]]; then
        echo "Error: End value must be a non-negative integer."
        exit 1
    fi
    
    if [[ "$END" -lt "$START" ]]; then
        echo "Error: End value must be greater than or equal to start value."
        exit 1
    fi

# Create local directory if it doesn't exist
mkdir -p "$LOCAL_PATH"

# Calculate block numbers to download
# Starting from START and incrementing by WINDOW
current=$START

echo "Starting downloads from block $START to $END with window size $WINDOW..."

while true; do
    # Check if we've reached the end (if specified)
    if [[ "$current" -gt "$END" ]]; then
        echo "Reached maximum block number ($END). Download complete."
        break
    fi
    
    filename="${current}.${SUFFIX}"
    download_file "$filename"
    
    # Increment to the next block
    current=$((current + WINDOW))
done