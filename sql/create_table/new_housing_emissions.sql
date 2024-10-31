DROP TABLE IF EXISTS new_housing_emissions; 
CREATE TABLE new_housing_emissions (
    id_pand VARCHAR,
    sqm BIGINT,
    status TEXT,
    year INTEGER,
    geom GEOMETRY,
    geom_28992 GEOMETRY,
    neighborhood_code VARCHAR,
    neighborhood VARCHAR,
    municipality VARCHAR,
    emissions_operational_kg DOUBLE PRECISION,
    emissions_embodied_kg BIGINT
);