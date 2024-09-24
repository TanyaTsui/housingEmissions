DROP TABLE IF EXISTS emissions_embodied_housing_nl_s1; 
CREATE TABLE emissions_embodied_housing_nl_s1 (
    year INT, 
    province VARCHAR, municipality VARCHAR, 
    neighborhood VARCHAR, neighborhood_code VARCHAR, 
    status VARCHAR, 
    emissions_embodied_kg NUMERIC, sqm NUMERIC 
); 
