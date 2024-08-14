#!/bin/bash

# Directory or pattern to search for files
GEOTIFF_DIR="data/ahn"
FILE_PATTERN="ahn*.tif"

# Initialize a flag to indicate if all files are GeoTIFFs
all_geotiffs=true

# Initialize empty array to store invalid GeoTIFFs
invalid_geotiffs=()

# Loop through each file matching the pattern
for file in $GEOTIFF_DIR/$FILE_PATTERN; do
    echo "Checking $file..."
    
    # Check if gdalinfo recognizes the file as a GeoTIFF
    gdalinfo "$file" &> /dev/null
    if [ $? -ne 0 ]; then
        echo "$file is not a valid GeoTIFF."
        all_geotiffs=false
        invalid_geotiffs+=("$file")
    fi
done

# Output the result
if [ "$all_geotiffs" = true ]; then
    echo "All files are valid GeoTIFFs."
else
    echo "Some files are not valid GeoTIFFs."
    echo "invalid GeoTIFFs: ${invalid_geotiffs[@]}"
fi
