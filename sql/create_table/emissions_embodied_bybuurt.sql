DROP TABLE IF EXISTS emissions_embodied_bybuurt; 
CREATE TABLE emissions_embodied_bybuurt(
    municipality VARCHAR, 
	neighborhood_code VARCHAR,
    year INTEGER,
    in_use NUMERIC,
    construction NUMERIC,
    renovation NUMERIC,
	transformation NUMERIC, 
    demolition NUMERIC, 
	emissions_embodied_kg NUMERIC 
);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_indexes 
        WHERE tablename = 'housing_inuse_2012_2022' 
        AND indexname = 'idx_municipality'
    ) THEN
        CREATE INDEX idx_municipality 
        ON housing_inuse_2012_2022 (municipality);
    END IF;
END $$;

