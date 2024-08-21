ALTER TABLE cbs_map_all
DROP COLUMN IF EXISTS emissions_kg_gas,
DROP COLUMN IF EXISTS emissions_kg_electricity,
DROP COLUMN IF EXISTS emissions_kg_total,
DROP COLUMN IF EXISTS emissions_kg_pp;

ALTER TABLE cbs_map_all
ADD COLUMN emissions_kg_gas NUMERIC,
ADD COLUMN emissions_kg_electricity NUMERIC,
ADD COLUMN emissions_kg_total NUMERIC,
ADD COLUMN emissions_kg_pp NUMERIC;

UPDATE cbs_map_all
SET emissions_kg_gas = g_gas_tot * aantal_hh * 1.9,
    emissions_kg_electricity = g_elek_tot * aantal_hh * 0.45,
    emissions_kg_total = (g_gas_tot * aantal_hh * 1.9) + (g_elek_tot * aantal_hh * 0.45),
    emissions_kg_pp = CASE 
                        WHEN aant_inw <= 0 THEN NULL
                        ELSE ((g_gas_tot * aantal_hh * 1.9) + (g_elek_tot * aantal_hh * 0.45)) / aant_inw 
                      END;
