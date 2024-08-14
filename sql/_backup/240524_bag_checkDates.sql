-- SELECT -- registration_start, COUNT(*) AS row_count
-- 	build_year, status, document_date, document_number, registration_start, registration_end
-- FROM bag_pand
-- WHERE 
-- 	CAST(build_year AS INTEGER) < 2024 AND 
-- 	status NOT IN ('Niet gerealiseerd pand', 'Pand ten onrechte opgevoerd') AND 
-- 	CAST(SUBSTRING(registration_end, 1, 4) AS INTEGER) < CAST(build_year AS INTEGER) -- - 1
-- -- GROUP BY registration_start 
-- -- ORDER BY row_count DESC
-- ORDER BY CAST(SUBSTRING(registration_end, 1, 4) AS INTEGER) ASC

SELECT * 
FROM bag_pand
WHERE registration_end IS NULL