DROP TABLE IF EXISTS nl_buurten;
CREATE TABLE nl_buurten (
    neighborhood_geom geometry, 
    neighborhood_code character varying,
    neighborhood character varying,
    municipality character varying, 
    province character varying
)