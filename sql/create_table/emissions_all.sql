DROP TABLE IF EXISTS emissions_all; 
CREATE TABLE emissions_all (
    year INT,
    neighborhood_code VARCHAR(50),
    neighborhood_name VARCHAR(255),
    municipality VARCHAR(255),
    geometry GEOMETRY,  -- Assuming you're using PostGIS for spatial data
    geom_4326 GEOMETRY,  -- Geometry in SRID 4326
    emissions_operational NUMERIC,
    emissions_embodied NUMERIC,
    emissions_total NUMERIC,
    n_households INT,
    n_residents INT,
    sqm_total NUMERIC,
    woz NUMERIC
);