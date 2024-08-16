WITH housing_nl_sample AS (
    SELECT *, ctid  -- Capture the ctid from the original housing_nl table
    FROM housing_nl
    WHERE municipality = 'Delft'
), 
duplicates AS (
    SELECT 
        ctid,  -- Use ctid from the original table
        id_pand, 
        LEFT(registration_start, 4) AS year, 
        status, 
        ROW_NUMBER() OVER (
            PARTITION BY id_pand, LEFT(registration_start, 4)
            ORDER BY 
                CASE 
                    WHEN status = 'transformation - adding units' THEN 1
                    WHEN status = 'transformation - function change' THEN 2
                    WHEN status = 'renovation - post2020' THEN 3
                    WHEN status = 'renovation - pre2020' THEN 4
                END
        ) AS row_num
    FROM housing_nl_sample
)

DELETE FROM housing_nl
WHERE ctid IN (
    SELECT ctid 
    FROM duplicates 
    WHERE row_num > 1  -- Delete only rows with row_num > 1
)
AND municipality = 'Delft';