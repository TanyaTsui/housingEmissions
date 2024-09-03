DROP TABLE IF EXISTS cbs_map_all;
CREATE TABLE cbs_map_all (
    electricity_kwh      double precision,
    woz                  double precision,
    gas_m3               double precision,
    year                 integer,
    neighborhood_geom    geometry,
    population           double precision,
    n_households         double precision,
    municipality         character varying,
    neighborhood         character varying,
    neighborhood_code    character varying
);