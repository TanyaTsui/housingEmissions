-- add delete old columns for emissions to cbs_map_all of they exist
ALTER TABLE cbs_map_all
DROP COLUMN IF EXISTS emissions_kg_gas,
DROP COLUMN IF EXISTS emissions_kg_electricity,
DROP COLUMN IF EXISTS emissions_kg_total,
DROP COLUMN IF EXISTS emissions_kg_pp;

-- add new columns for emissions to cbs_map_all
ALTER TABLE cbs_map_all
ADD COLUMN emissions_kg_gas NUMERIC,
ADD COLUMN emissions_kg_electricity NUMERIC,
ADD COLUMN emissions_kg_total NUMERIC,
ADD COLUMN emissions_kg_pp NUMERIC;

UPDATE cbs_map_all
SET emissions_kg_gas = gas_m3 * n_households * 1.9,
    emissions_kg_electricity = electricity_kwh * n_households * 0.45,
    emissions_kg_total = (gas_m3 * n_households * 1.9) + (electricity_kwh * n_households * 0.45),
    emissions_kg_pp = CASE 
                        WHEN population <= 0 THEN NULL
                        ELSE ((gas_m3 * n_households * 1.9) + (electricity_kwh * n_households * 0.45)) / population 
                      END;
