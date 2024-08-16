ALTER TABLE housing_nl ADD COLUMN IF NOT EXISTS landuse VARCHAR; 

-- assign the landuse 'Residential' to all buildings where function was not guessed
UPDATE housing_nl
SET landuse = 'Residential'
WHERE ahn_version IS NULL;
