DROP TABLE IF EXISTS demolished_buildings_nl;
CREATE TABLE demolished_buildings_nl (
    id_pand VARCHAR,
    geometry VARCHAR,
    build_year VARCHAR,
    status VARCHAR,
    document_date VARCHAR,
    document_number VARCHAR,
    registration_start VARCHAR,
    registration_end VARCHAR,
    geom GEOMETRY,             
    geom_28992 GEOMETRY,       
    province VARCHAR, 
	neighborhood_code VARCHAR, 
    neighborhood VARCHAR,      
    municipality VARCHAR,
	function VARCHAR[],
    sqm BIGINT
);