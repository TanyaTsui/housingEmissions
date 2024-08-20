DROP TABLE IF EXISTS emissions_embodied_housing_nl; 
CREATE TABLE emissions_embodied_housing_nl (
    year INT, 
    province VARCHAR, municipality VARCHAR, 
    neighborhood VARCHAR, neighborhood_code VARCHAR, 
    status VARCHAR, 
    emissions_embodied_kg NUMERIC, sqm NUMERIC 
); 
