DROP TABLE IF EXISTS housing_bybuurt; 
CREATE TABLE housing_bybuurt(
    municipality VARCHAR, 
	neighborhood_code VARCHAR,
    year INTEGER,
    in_use NUMERIC,
    construction NUMERIC,
    renovation NUMERIC,
    demolition NUMERIC
);