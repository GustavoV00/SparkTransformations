#!/bin/bash

# Function to display help
function show_help {
    echo "Usage: $0 [options]"
    echo "Options:"
    echo "  -h  Display help"
    echo "  -u  Display usage example"
}

# Function to display usage information
function show_usage {
    echo "-This is an example script. It needs four args, data_file input_format output_format dir_result"
    echo "-It's important to notice that, the app doesn't hold the / at the dir args, so don't need to put it"
    echo "-The monitor results are stored at the results folder, with the following name: file_name_input_format_to_output_format_timestamp.txt"
    echo "example: ./starter.sh file_with_data.csv csv parquet saude"
}

# Parse command line options
while getopts ":hu" opt; do
    case $opt in
        h)
            show_help
            exit 0
            ;;
        u)
            show_usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG"
            show_help
            exit 1
            ;;
    esac
done

# If no options are provided, display usage information
if [ $OPTIND -eq 1 ]; then
    show_usage
fi