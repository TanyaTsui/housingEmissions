#!/bin/bash

# Configuration
DB_NAME="urbanmining"
DB_USER="postgres"
DB_PASSWORD="Tunacompany5694!"
SRID="28992"  
GEOTIFF_DIR="data/ahn"
export PGPASSWORD=$DB_PASSWORD

# Loop through ahn versions
for version in "ahn2" "ahn3" "ahn4"; do
    # Loop through dsm and dtm
    for model_type in "dsm" "dtm"; do
        # create table
        TABLE_NAME="${version}_${model_type}"

        # Connect to the PostgreSQL database and create the raster table
        psql -U $DB_USER -d $DB_NAME -c "
        DROP TABLE IF EXISTS $TABLE_NAME;
        CREATE TABLE $TABLE_NAME (
            rid SERIAL PRIMARY KEY,
            filename TEXT,
            rast RASTER
        );" > /dev/null

        # Insert files in batches
        batch_size=100
        count=0
        files=()

        for file in "$GEOTIFF_DIR"/"$version"_"$model_type"*.tif; do
            if [[ ! -e "$file" ]]; then
                continue
            fi

            if [[ ! $file =~ [0-9]+\.tif$ ]]; then
                continue
            fi

            echo "Adding $file to batch..."
            files+=("$file")
            count=$((count + 1))

            if (( count % batch_size == 0 )); then
                raster2pgsql -s $SRID -M -a "${files[@]}" -t auto -F $TABLE_NAME | psql -U $DB_USER -d $DB_NAME > /dev/null
                files=()
                echo "Batch inserted."
            fi
        done

        # Insert any remaining files
        if (( ${#files[@]} > 0 )); then
            raster2pgsql -s $SRID -M -a "${files[@]}" -t auto -F $TABLE_NAME | psql -U $DB_USER -d $DB_NAME > /dev/null
            echo "Remaining files inserted."
        fi

        # Create index on rast after bulk insert
        psql -U $DB_USER -d $DB_NAME -c "
        CREATE INDEX ON $TABLE_NAME USING GIST (ST_ConvexHull(rast));
        " > /dev/null
        echo "Index created."

        # # Loop through each GeoTIFF file and import it into the PostgreSQL database
        # for file in "$GEOTIFF_DIR"/"$version"_"$model_type"*.tif; do
        #     echo "Importing $file..."
        #     raster2pgsql -s $SRID -I -M -a $file -t auto -F $TABLE_NAME | psql -U $DB_USER -d $DB_NAME > /dev/null
        # done

    done
done

echo "All GeoTIFF files have been imported into the PostgreSQL database."