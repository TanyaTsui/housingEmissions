DROP TABLE IF EXISTS housing_nl_s1; 
CREATE TABLE housing_nl_s1(
    status VARCHAR, 
    function TEXT, 
    sqm BIGINT, 
    id_pand VARCHAR, 
    build_year VARCHAR, 
    document_date VARCHAR, 
    document_number VARCHAR, 
    registration_start VARCHAR, 
    registration_end VARCHAR, 
    geom GEOMETRY, 
    geom_28992 GEOMETRY, 
    neighborhood_code VARCHAR, 
    neighborhood VARCHAR, 
    municipality VARCHAR, 
    province VARCHAR
);