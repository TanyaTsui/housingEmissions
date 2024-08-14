#!/bin/bash

# Directory containing the .tif files
GEOTIFF_DIR="data/ahn"

# Iterate over each .tif file in the directory
for file in "$GEOTIFF_DIR"/*.tif; do
    # Extract the base name and extension
    base_name=$(basename "$file" .tif)
    
    # Extract the number part of the file name using regex
    if [[ $base_name =~ ^(ahn[0-9]_d[st]m_)([0-9]+)$ ]]; then
        prefix="${BASH_REMATCH[1]}"
        number="${BASH_REMATCH[2]}"
        
        # rename file with reformatted digits
        new_number=$(printf "%04d" "$number")
        new_file="$GEOTIFF_DIR/${prefix}${new_number}.tif"
        mv "$file" "$new_file"
        
        # Print the renaming action (optional)
        echo "Renamed $file to $new_file"
    else
        echo "Skipping $file, does not match expected pattern"
    fi
done
