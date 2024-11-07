#!/bin/bash

# Configuration
DB_NAME="urbanmining"
DB_USER="postgres"
DB_PASSWORD="Tunacompany5694!"
SRID="28992"  
GEOTIFF_DIR="data/ahn"
export PGPASSWORD=$DB_PASSWORD

# Create the ahn_elevation table
psql -U $DB_USER -d $DB_NAME -c "
DROP TABLE IF EXISTS ahn_elevation;
CREATE TABLE ahn_elevation (
    rid SERIAL PRIMARY KEY,
    rast RASTER,
    ahn_version TEXT
);" > /dev/null
echo "Created table ahn_elevation"

# Loop through ahn versions
for version in "ahn2" "ahn3" "ahn4"; do

    TABLE_NAME="${version}_elevation"

    # Create raster table for elevation
    psql -U $DB_USER -d $DB_NAME -c "
    DROP TABLE IF EXISTS $TABLE_NAME;
    CREATE TABLE $TABLE_NAME (
        rid SERIAL PRIMARY KEY,
        rast RASTER
    );" > /dev/null
    echo "Created table $TABLE_NAME"

    # Calculate values for elevation raster table for each verion 
    psql -U $DB_USER -d $DB_NAME -c "
    INSERT INTO ${TABLE_NAME} (rast)
    SELECT ST_MapAlgebra(dsm.rast, dtm.rast, '([rast1] - [rast2])'::text)
    FROM ${version}_dsm dsm, ${version}_dtm dtm
    WHERE dsm.rid = dtm.rid;
    " > /dev/null
    echo "Calculated elevation values for $TABLE_NAME"

    # intert elevation rasters for each version into ahn_elevation
    psql -U $DB_USER -d $DB_NAME -c "
    INSERT INTO ahn_elevation (rast, ahn_version)
    SELECT rast, '$version'
    FROM ${version}_elevation;
    " > /dev/null
    echo "Inserted $TABLE_NAME into ahn_elevation"

    # # drop raster tables for each version
    # psql -U $DB_USER -d $DB_NAME -c "
    # DROP TABLE IF EXISTS ${version}_elevation;
    # DROP TABLE IF EXISTS ${version}_dsm;
    # DROP TABLE IF EXISTS ${version}_dtm;
    # " > /dev/null

done