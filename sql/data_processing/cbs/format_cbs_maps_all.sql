UPDATE cbs_map_all
SET aant_inw = CASE WHEN aant_inw < -9999 THEN NULL ELSE aant_inw END,
    aantal_hh = CASE WHEN aantal_hh < -9999 THEN NULL ELSE aantal_hh END,
    woz = CASE WHEN woz < -9999 THEN NULL ELSE woz END,
    g_gas_tot = CASE WHEN g_gas_tot < -9999 THEN NULL ELSE g_gas_tot END,
    g_elek_tot = CASE WHEN g_elek_tot < -9999 THEN NULL ELSE g_elek_tot END
WHERE aant_inw < -9999 
   OR aantal_hh < -9999 
   OR woz < -9999 
   OR g_gas_tot < -9999 
   OR g_elek_tot < -9999;