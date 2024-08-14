from data_processing._common.params_manager import ParamsManager

class QueryManager(): 
    def __init__(self): 
        self.params_manager = ParamsManager()
        self.bag_pand_table_name = self.params_manager.get_table_params('pand')['table_name']
        self.bag_vbo_table_name = self.params_manager.get_table_params('vbo')['table_name']
        self.ahn_table_name = self.params_manager.get_table_params('ahn_elevation')['table_name']
        self.housing_table_name = self.params_manager.get_table_params('housing_nl')['table_name']

    ###############################################################################################################################################################################
    # MAKE DELFT SAMPLE
    ###############################################################################################################################################################################

    def query_make_delft_sample(self): 
        return ''' 
            DROP TABLE IF EXISTS bag_pand_delft;
            CREATE TABLE bag_pand_delft AS 
            WITH delft AS (
                SELECT * FROM nl_gemeentegebied WHERE naam = 'Delft'
            )
            SELECT b.*
            FROM bag_pand b
            JOIN delft d
            ON ST_Contains(d.geom, b.geom_28992);

            DROP TABLE IF EXISTS bag_vbo_delft;
            CREATE TABLE bag_vbo_delft AS
            WITH delft AS (
                SELECT * FROM nl_gemeentegebied WHERE naam = 'Delft'
            )
            SELECT b.*
            FROM bag_vbo b
            JOIN delft d
            ON ST_Contains(d.geom, b.geom_28992);

            DROP TABLE IF EXISTS ahn_elevation_delft; 
            CREATE TABLE ahn_elevation_delft AS

            WITH delft AS (
                SELECT * FROM nl_gemeentegebied WHERE naam = 'Delft'
            )

            SELECT 
                d.naam AS municipality, d.ligt_in_provincie_naam AS province, 
                a.*
            FROM ahn_elevation a 
            JOIN delft d 
            ON ST_Intersects(a.rast_geom, d.geom); 
            '''
    
    ###############################################################################################################################################################################
    # ADD MUNICIPALITY AND PROVINCE INFO TO AHN_ELEVATION, BAG_PAND, BAG_VBO, AND LANDUSE_NL
    # This allows queries to be run by municipality, which dramatically reduces runtime 
    ###############################################################################################################################################################################
    
    def query_add_geom_columns_bag_pand(self):
        return f''' 
                -- Step 1: Add the Geometry Columns
                ALTER TABLE bag_pand
                ADD COLUMN IF NOT EXISTS geom geometry(Polygon, 4326),
                ADD COLUMN IF NOT EXISTS geom_28992 geometry(Polygon, 28992);

                -- Step 2: Convert Text Data to Geometry and Populate the geom_28992 Column
                UPDATE bag_pand
                SET geom_28992 = ST_GeomFromText(
                                    'POLYGON((' || 
                                    RTRIM(REPLACE(geometry, ' 0.0', ', '), ', ')
                                    || '))', 28992)
                WHERE geometry IS NOT NULL;

                -- Step 3: Transform the 28992 Geometry to 4326 and Populate the geom Column
                UPDATE bag_pand
                SET geom = ST_Transform(geom_28992, 4326)
                WHERE geom_28992 IS NOT NULL;

                -- Step 4: Create Spatial Indexes for the New Geometry Columns
                CREATE INDEX IF NOT EXISTS idx_bag_pand_geom ON bag_pand USING GIST (geom);
                CREATE INDEX IF NOT EXISTS idx_bag_pand_geom_28992 ON bag_pand USING GIST (geom_28992);
                '''
    
    def query_add_municipality_ahn(self): 
        ahn_table_name = self.ahn_table_name
        return f''' 
            ALTER TABLE {ahn_table_name} ADD COLUMN IF NOT EXISTS municipality VARCHAR;
            ALTER TABLE {ahn_table_name} ADD COLUMN IF NOT EXISTS province VARCHAR;

            WITH municipality AS (
                SELECT * FROM nl_gemeentegebied WHERE naam = %s 
            )
                
            UPDATE {ahn_table_name} 
            SET 
                municipality = m.naam, 
                province = m.ligt_in_provincie_naam
            FROM municipality m 
            WHERE ST_Intersects({ahn_table_name}.rast_geom, m.geom); 
            ''' 
    
    def query_add_columns_bag_pand(self): 
        bag_pand_table_name = self.bag_pand_table_name
        return f''' 
            ALTER TABLE {bag_pand_table_name} ADD COLUMN neighborhood_code VARCHAR;
            ALTER TABLE {bag_pand_table_name} ADD COLUMN neighborhood VARCHAR;
            ALTER TABLE {bag_pand_table_name} ADD COLUMN municipality VARCHAR;
            ALTER TABLE {bag_pand_table_name} ADD COLUMN province VARCHAR;
            '''
    
    def query_add_columns_bag_vbo(self): 
        bag_vbo_table_name = self.bag_vbo_table_name
        return f''' 
            ALTER TABLE {bag_vbo_table_name} ADD COLUMN neighborhood_code VARCHAR;
            ALTER TABLE {bag_vbo_table_name} ADD COLUMN neighborhood VARCHAR;
            ALTER TABLE {bag_vbo_table_name} ADD COLUMN municipality VARCHAR;
            ALTER TABLE {bag_vbo_table_name} ADD COLUMN province VARCHAR;
            '''
    
    def query_match_bag_to_admin_boundaries(self): 
        return f''' 
            WITH municipality AS (
                SELECT * FROM nl_buurten WHERE municipality_name = %s
            )
                
            UPDATE {self.bag_pand_table_name}
            SET 
                neighborhood_code = b.neighborhood_code, 
                neighborhood = b.neighborhood, 
                municipality = b.municipality_name,
                province = b.province
            FROM municipality b
            WHERE 
                {self.bag_pand_table_name}.geom_28992 && b.neighborhood_geom
                AND ST_Intersects({self.bag_pand_table_name}.geom_28992, b.neighborhood_geom);

            WITH municipality AS (
                SELECT * FROM nl_buurten WHERE municipality_name = %s
            )
                
            UPDATE {self.bag_vbo_table_name}
            SET 
                neighborhood_code = b.neighborhood_code, 
                neighborhood = b.neighborhood, 
                municipality = b.municipality_name,
                province = b.province
            FROM municipality b
            WHERE 
                {self.bag_vbo_table_name}.geom_28992 && b.neighborhood_geom
                AND ST_Intersects({self.bag_vbo_table_name}.geom_28992, b.neighborhood_geom);
            '''

    def query_create_landuse_nl_table(self): 
        return '''
            DROP TABLE IF EXISTS landuse_nl;
            CREATE TABLE landuse_nl (
                gml_id VARCHAR, description VARCHAR, geom_28992 GEOMETRY,
                municipality VARCHAR, province VARCHAR
            );
            ''' 
    
    def query_add_buurtInfo_to_landuse_nl(self): 
        return f''' 
            INSERT INTO landuse_nl (
                gml_id, description, geom_28992, municipality, province
            )

            WITH municipality AS (
                SELECT * 
                FROM nl_gemeentegebied
                WHERE naam = %s 
            ), 
            landuse_municipality AS (
                SELECT 
                    l.gml_id, l.description AS landuse, l.geom_28992, 
                    m.naam AS municipality, m.ligt_in_provincie_naam AS province
                FROM existinglanduseobject l 
                JOIN municipality m 
                ON 
                    l.geom_28992 && m.geom 
                    AND ST_Intersects(l.geom_28992, m.geom)
            )

            SELECT * FROM landuse_municipality
            '''
    
    ###############################################################################################################################################################################
    # ADD RENOVATION INFO TO BAG_PAND
    # This includes the following types of renovation: 
    # - pre-2020 renovations that were not captured in the BAG data since 'Verbouwing pand' wasn't introduced yet 
    # - transformation of building from non-housing to housing function 
    # - transformation of building from adding new units 
    ###############################################################################################################################################################################

    def query_add_renovation_column(self): 
        bag_pand_table_name = self.bag_pand_table_name
        return f'''
            ALTER TABLE {bag_pand_table_name} DROP COLUMN IF EXISTS renovation_pandingebruiknietingemeten;
            ALTER TABLE {bag_pand_table_name} DROP COLUMN IF EXISTS renovation;
            ALTER TABLE {bag_pand_table_name} ADD COLUMN IF NOT EXISTS renovation VARCHAR;
            CREATE INDEX IF NOT EXISTS idx_bag_pand_municipality_registration_start ON {bag_pand_table_name} (municipality, (LEFT(registration_start, 4)::INTEGER));
            CREATE INDEX IF NOT EXISTS idx_bag_pand_id_pand_registration_start ON {bag_pand_table_name} (id_pand, registration_start);
            CREATE INDEX IF NOT EXISTS idx_bag_pand_status ON {bag_pand_table_name} (status);
            '''
    
    def query_add_pre2020_renovations(self): 
        return f'''
                -- Delete rows where municipality = %s AND status = 'renovation - pre2020'
                DELETE FROM {self.housing_table_name}
                WHERE 
                    municipality = %s 
                    AND status = 'renovation - pre2020';

                -- Insert rows from the query into {self.housing_table_name}
                WITH bag_pand_municipality AS (
                    SELECT * FROM {self.bag_pand_table_name}
                    WHERE 
                        municipality = %s 
                        AND LEFT(registration_start, 4)::INTEGER < 2020
                ), 
                ranked_pand AS (
                    SELECT *,
                        LAG(status) OVER (PARTITION BY id_pand ORDER BY registration_start) AS previous_status
                    FROM bag_pand_municipality
                ), 
                pand_renovations AS (
                    SELECT 
                        id_pand, geometry::TEXT,  -- Casting geometry to text to match table column
                        build_year::TEXT,         -- Casting build_year to text to match table column
                        'renovation - pre2020' AS status, 
                        document_date::TEXT, 
                        document_number, 
                        registration_start::TEXT, 
                        registration_end::TEXT, 
                        geom, geom_28992, 
                        neighborhood_code, neighborhood, municipality, province
                    FROM ranked_pand
                    WHERE 
                        status = 'Pand in gebruik (niet ingemeten)' 
                        AND previous_status IS NOT NULL 
                        AND previous_status = 'Pand in gebruik'
                ), 
                housing_units_inuse AS (
                    SELECT * 
                    FROM {self.bag_vbo_table_name} 
                    WHERE 
                        municipality = %s 
                        AND status = 'Verblijfsobject in gebruik'
                        AND function = 'woonfunctie'
                        AND sqm::INTEGER < 9999
                ), 
                housing_units_unique AS (
                    SELECT DISTINCT ON (id_vbo) * 
                    FROM housing_units_inuse
                ), 
                housing_sqm AS (
                    SELECT id_pand, SUM(sqm::INTEGER) AS sqm
                    FROM housing_units_unique
                    GROUP BY id_pand
                ), 
                housing_sqm_function AS (
                    SELECT 'woonfunctie' AS function, *
                    FROM housing_sqm
                )
                INSERT INTO {self.housing_table_name} (
                    function, sqm, id_pand, geometry, build_year, status, 
                    document_date, document_number, registration_start, registration_end, 
                    geom, geom_28992, neighborhood_code, neighborhood, municipality, province
                )
                SELECT 
                    h.function, h.sqm, 
                    r.id_pand, r.geometry, r.build_year, r.status, 
                    r.document_date, r.document_number, r.registration_start, r.registration_end, 
                    r.geom, r.geom_28992, r.neighborhood_code, r.neighborhood, r.municipality, r.province
                FROM pand_renovations r
                LEFT JOIN housing_sqm_function h
                ON r.id_pand = h.id_pand
                '''
    
    def query_add_post2020_renovations(self):
        return f'''

                -- Delete rows where municipality = %s AND status = 'renovation - pre2020'
                DELETE FROM {self.housing_table_name}
                WHERE 
                    municipality = %s 
                    AND status = 'renovation - post2020';

                -- Insert rows from the query into {self.housing_table_name}
                WITH building_renovations AS (
                    SELECT 
                        id_pand, geometry, build_year, 
                        CASE 
                            WHEN status = 'Verbouwing pand' THEN 'renovation - post2020'
                            ELSE status
                        END AS status, 
                        document_date, document_number, registration_start, registration_end, 
                        geom, geom_28992, neighborhood_code, neighborhood, municipality, province
                    FROM {self.bag_pand_table_name}
                    WHERE 
                        municipality = %s 
                        AND status = 'Verbouwing pand'
                        AND LEFT(registration_start, 4)::INTEGER >= 2020
                ), 
                housing_units_inuse AS (
                    SELECT * 
                    FROM {self.bag_vbo_table_name} 
                    WHERE 
                        municipality = %s 
                        AND status = 'Verblijfsobject in gebruik'
                        AND function = 'woonfunctie'
                        AND sqm::INTEGER < 9999
                ), 
                housing_units_unique AS (
                    SELECT DISTINCT ON (id_vbo) * 
                    FROM housing_units_inuse
                ), 
                housing_sqm AS (
                    SELECT id_pand, SUM(sqm::INTEGER) AS sqm
                    FROM housing_units_unique
                    GROUP BY id_pand
                ), 
                housing_sqm_function AS (
                    SELECT 'woonfunctie' AS function, *
                    FROM housing_sqm
                )
                INSERT INTO {self.housing_table_name} (
                    function, sqm, id_pand, geometry, build_year, status, 
                    document_date, document_number, registration_start, registration_end, 
                    geom, geom_28992, neighborhood_code, neighborhood, municipality, province
                )
                SELECT 
                    h.function, h.sqm, 
                    r.id_pand, r.geometry, r.build_year, r.status, 
                    r.document_date, r.document_number, r.registration_start, r.registration_end, 
                    r.geom, r.geom_28992, r.neighborhood_code, r.neighborhood, r.municipality, r.province
                FROM building_renovations r
                LEFT JOIN housing_sqm_function h
                ON r.id_pand = h.id_pand
            
                '''
    
    def query_add_transformation_function_change(self): 
        bag_pand_table_name = self.bag_pand_table_name
        return f''' 
            DROP TABLE IF EXISTS transformed_housing_buildings; 

            -- delete rows about transformation (from previous queries)
            DELETE FROM {bag_pand_table_name}
            WHERE 
                municipality = %s
                AND renovation = 'transformation - function change'; 

            -- create temp table transformed_housing_buildings
            CREATE TEMP TABLE transformed_housing_buildings AS 
            WITH bag_vbo_municipality AS (
                SELECT * 
                FROM bag_vbo 
                WHERE municipality = %s
            ), 
            function_counts AS (
                SELECT 
                    id_vbo,
                    COUNT(DISTINCT function) AS n_functions
                FROM bag_vbo_municipality
                GROUP BY id_vbo
            ), 
            idvbos_functionchange AS (
                SELECT DISTINCT id_vbo 
                FROM function_counts 
                WHERE n_functions > 1
            ), 
            bag_vbo_functionchange AS (
                SELECT b.*
                FROM bag_vbo_municipality b
                JOIN idvbos_functionchange i
                ON b.id_vbo = i.id_vbo
            ), 
            previousfunction AS (
                SELECT 
                    LAG(function) OVER (PARTITION BY id_vbo ORDER BY registration_start) AS previous_function, 
                    * 
                FROM bag_vbo_functionchange
            ), 
            transformed_housing_units AS (
                SELECT * 
                FROM previousfunction
                WHERE 
                    function = 'woonfunctie' 
                    AND previous_function != 'woonfunctie'
                    AND previous_function IS NOT NULL
            ), 
            transformed_housing_buildings AS (
                SELECT id_pand, LEFT(registration_start, 4) AS year
                FROM transformed_housing_units
                GROUP BY id_pand, LEFT(registration_start, 4)
            )

            SELECT * FROM transformed_housing_buildings; 

            -- update renovation column of {bag_pand_table_name} with transformation 
            -- where there was a match in id_pand and registration year 
            UPDATE {bag_pand_table_name} b
            SET renovation = 'transformation - function change'
            FROM transformed_housing_buildings t 
            WHERE 
                b.id_pand = t.id_pand 
                AND municipality = %s
                AND LEFT(b.registration_start, 4) = t.year;

            -- add new rows to {bag_pand_table_name} on transformation 
            -- where there was no match in id_pand and registration year 
            INSERT INTO {bag_pand_table_name} (
                id_pand, geometry, build_year, status, document_date, registration_start, registration_end, 
                geom, geom_28992, neighborhood_code, neighborhood, municipality, province, renovation
            )
            WITH bag_pand_municipality AS (
                SELECT * 
                FROM bag_pand
                WHERE municipality = %s
            ), 	
            transformation_notinbag AS (
                SELECT t.* 
                FROM transformed_housing_buildings t 
                LEFT JOIN bag_pand_municipality b 
                ON 
                    b.id_pand = t.id_pand 
                    AND LEFT(b.registration_start, 4) = t.year
                WHERE b.id_pand IS NULL
            ), 
            ranked_bag_pand AS (
                SELECT 
                    t.id_pand, b.geometry, b.build_year, b.status, 
                    year || '-01-01' AS document_date, year || '-01-01' AS registration_start, year || '-12-31' AS registration_end, 
                    b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province, 
                    'transformation - function change (inserted)' AS renovation, 
                    ROW_NUMBER() OVER (PARTITION BY t.id_pand ORDER BY b.registration_start DESC) AS rn
                FROM 
                    transformation_notinbag t
                JOIN 
                    bag_pand_municipality b 
                ON 
                    b.id_pand = t.id_pand
                WHERE 
                    b.registration_start <= t.year
            )
            SELECT 
                id_pand, geometry, build_year, status, document_date, registration_start, registration_end, 
                geom, geom_28992, neighborhood_code, neighborhood, municipality, province, renovation
            FROM ranked_bag_pand
            WHERE rn = 1;
            ''' 

    ###############################################################################################################################################################################
    # ESTIMATE HOUSING FUNCTION AND SQM 
    # Some buildings in the BAG dataset don't have info on function and sqm, so we need to estimate it: 
    # - function is estimated using land use data and function of nearest buildings within a 50m radius 
    # - sqm is estimated using AHN elevation data and building footprint
    ###############################################################################################################################################################################

    def query_create_housing_nl_table(self): 
        housing_table_name = self.housing_table_name
        return f''' 
            DROP TABLE IF EXISTS {housing_table_name};
            CREATE TABLE {housing_table_name} (
                function TEXT,
                sqm BIGINT,
                id_pand VARCHAR,
                geometry VARCHAR,
                build_year VARCHAR,
                status VARCHAR,
                document_date VARCHAR,
                document_number VARCHAR,
                registration_start VARCHAR,
                registration_end VARCHAR,
                geom GEOMETRY,             -- Assuming GEOMETRY is the intended type
                geom_28992 GEOMETRY,       -- Assuming GEOMETRY is the intended type
                neighborhood_code VARCHAR, -- Added column
                neighborhood VARCHAR,      -- Added column
                municipality VARCHAR,
                province VARCHAR
            );
            '''
    
    def query_estimate_housing_function(self): 
        housing_table_name = self.housing_table_name
        bag_vbo_table_name = self.bag_vbo_table_name
        bag_pand_table_name = self.bag_pand_table_name
        return f'''
            INSERT INTO {housing_table_name} (
                function, sqm, id_pand, geometry, build_year, status, 
                document_date, document_number, registration_start, registration_end, 
                geom, geom_28992, neighborhood_code, neighborhood, municipality, province
            )
                
            -- select relevant units and buildings from BAG
            WITH bag_vbo_sample AS (
                SELECT *
                FROM {bag_vbo_table_name}
                WHERE municipality = %s
            ), 
            units AS (
                SELECT DISTINCT ON (id_vbo) * 
                FROM bag_vbo_sample
                WHERE 
                    status = 'Verblijfsobject in gebruik' AND sqm::INTEGER < 9999
                ORDER BY id_vbo, status, registration_start, document_number
            ), 
            bag_pand_sample_without_reno AS (
                SELECT * 
                FROM {bag_pand_table_name}
                WHERE municipality = %s 
            ), 
            bag_pand_sample AS (
                SELECT 
                    id_pand, geometry, build_year, 
                    CASE 
                        WHEN renovation = 'Verbouwing pand' THEN 'Verbouwing pand'
                        WHEN renovation = 'transformation - function change (inserted)' THEN 'transformation - function change (inserted)'
                        WHEN renovation = 'transformation - function change' THEN 'transformation - function change'
                        ELSE status
                    END AS status,
                    document_date, document_number, 
                    registration_start, registration_end, geom, geom_28992, 
                    neighborhood_code, neighborhood, municipality, province
                FROM bag_pand_sample_without_reno
            ), 
            buildings_construction_demolition AS (
                SELECT DISTINCT ON (id_pand, status) * 
                FROM bag_pand_sample
                WHERE status IN ('Bouw gestart', 'Pand gesloopt')
                ORDER BY id_pand, status, registration_start, document_number
            ), 
            buildings_renovation AS (
                SELECT * 
                FROM bag_pand_sample
                WHERE status = 'Verbouwing pand'
            ), 
            buildings AS (
                SELECT * FROM buildings_construction_demolition
                UNION ALL 
                SELECT * FROM buildings_renovation
            ), 

            -- get buildings with unknown function
            buildings_unknown AS (
                SELECT b.*
                FROM buildings b
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM units u
                    WHERE u.id_pand = b.id_pand
                )
                ORDER BY b.id_pand, b.status, b.registration_start, b.document_number
            ),

            -- find units within 50m radius of buildings_unknown
            units_near_unknown AS (
                SELECT b.id_pand AS id_pand_b, u.*
                FROM units u
                JOIN buildings_unknown b 
                ON ST_DWithin(u.geom_28992, b.geom_28992, 50) AND u.neighborhood_code = b.neighborhood_code
            ), 
            units_near_unknown_grouped AS (
                SELECT 
                    id_pand_b,
                    function,
                    COUNT(*) AS function_count
                FROM units_near_unknown
                GROUP BY id_pand_b, function
            ), 
            units_near_unknown_ranked AS (
                SELECT 
                    id_pand_b,
                    function,
                    function_count,
                    ROW_NUMBER() OVER (PARTITION BY id_pand_b ORDER BY function_count DESC) AS rn
                FROM 
                    units_near_unknown_grouped
            ), 

            -- estimate unknown functions
            housing_guesses AS (
                SELECT DISTINCT ON (id_pand_b)
                    id_pand_b AS id_pand, function::TEXT AS function, NULL::BIGINT AS sqm
                FROM units_near_unknown_ranked 
                WHERE rn = 1 AND function = 'woonfunctie'
            ), 
            housing_fromvbo AS (
                SELECT id_pand, SUM(sqm::INTEGER) AS sqm, 'woonfunctie' AS function
                FROM units
                WHERE function = 'woonfunctie'
                GROUP BY id_pand
            ), 
            housing_combined AS (
                SELECT id_pand, function, sqm FROM housing_guesses
                UNION
                SELECT id_pand, function, sqm FROM housing_fromvbo 
            ), 
            housing_final AS (
                SELECT 
                    u.function AS function, u.sqm AS sqm, 
                    b.*
                FROM buildings b 
                JOIN housing_combined u ON b.id_pand = u.id_pand 
            )
            SELECT * FROM housing_final; 
            ''' 
    
    def query_estimate_housing_sqm(self): 
        housing_table_name = self.housing_table_name
        ahn_table_name = self.ahn_table_name
        return f'''
            -- create indexes
            CREATE INDEX IF NOT EXISTS idx_housing_nl_status_registration_start ON {housing_table_name} (status, registration_start);
            CREATE INDEX IF NOT EXISTS idx_buildings_geom_28992 ON {housing_table_name} USING gist (geom_28992);
            CREATE INDEX IF NOT EXISTS idx_ahn_elevation_rast_geom ON {ahn_table_name} USING gist (rast_geom);

            ALTER TABLE {housing_table_name} 
            ADD COLUMN IF NOT EXISTS ahn_version VARCHAR; 

            INSERT INTO {housing_table_name} (
                sqm, ahn_version, function, id_pand, build_year, status, 
                document_date, registration_start, registration_end, 
                geom, geom_28992, 
                neighborhood_code, neighborhood, municipality, province
            )

            WITH buildings AS (
                SELECT 
                    sqm, function, id_pand, build_year, status, 
                    document_date, registration_start, registration_end, 
                    geom, geom_28992, neighborhood_code, neighborhood, municipality, province
                FROM {housing_table_name}
                WHERE 
                    sqm IS NULL AND 
                    ST_Area(geom_28992) < 100000 AND 
                    municipality = %s
            ), 	

            -- assign rows to ahn version
            buildings_ahnversion AS (
                SELECT 
                    CASE 		
                        -- cases for bouw gestart 
                        WHEN status = 'Bouw gestart' AND registration_start BETWEEN '2011-01-01' AND '2014-01-01' THEN 'ahn3'
                        WHEN status = 'Bouw gestart' AND registration_start BETWEEN '2014-01-01' AND '2020-01-01' THEN 'ahn4'
                        -- must be NULL, definitely don't have height data for buildings built after 2020 
                        WHEN status = 'Bouw gestart' AND registration_start > '2020-01-01' THEN 'no ahn available' 
                
                        -- cases for verbouwing pand 
                        WHEN status = 'Verbouwing pand' AND registration_start BETWEEN '2011-01-01' AND '2013-06-30' THEN 'ahn2'
                        WHEN status = 'Verbouwing pand' AND registration_start BETWEEN '2013-07-01' AND '2019-12-31' THEN 'ahn3' 
                        WHEN status = 'Verbouwing pand' AND registration_start > '2020-01-01' THEN 'ahn4'
                
                        -- cases for pand gesloopt 
                        -- buildings demolished pre-2013 could use ahn2, but there is a risk that the elevation data was recorded during / after demolition. 
                        -- for 100 percent accuracy, AHN1 data is needed, but (easily) available. (online has only DSM at 5m resolution) 
                        WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2011-01-01' AND '2012-12-31' THEN 'ahn2' -- not 100 percent accurate 
                        WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2013-01-01' AND '2019-12-31' THEN 'ahn2'
                        WHEN status = 'Pand gesloopt' AND registration_start BETWEEN '2020-01-01' AND '2022-12-31' THEN 'ahn3'
                        WHEN status = 'Pand gesloopt' AND registration_start >= '2023-01-01' THEN 'ahn4'
                
                        -- other null cases 
                        ELSE 'unforseen case'
                    END AS ahn_version, 
                    * 
                FROM buildings
            ), 

            -- clip rasters by building footprint 
            elevation_municipality AS (
                SELECT *
                FROM {ahn_table_name} 
                WHERE municipality = %s
            ), 
            clipped_rasters AS (
                SELECT 
                    b.ahn_version, b.function, b.id_pand, b.build_year, 
                    b.status, b.document_date, b.registration_start, b.registration_end, 
                    b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province, 
                    ST_Union(ST_Clip(r.rast, 1, b.geom_28992, -9999)) AS raster
                FROM buildings_ahnversion b
                LEFT JOIN elevation_municipality r
                ON ST_Intersects(r.rast_geom, b.geom_28992) AND b.ahn_version = r.ahn_version 
                GROUP BY
                    b.ahn_version, b.function, b.id_pand, b.build_year, 
                    b.status, b.document_date, b.registration_start, b.registration_end, 
                    b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province
            ), 

            -- estimate n floors, sqm 
            building_heights AS (
                SELECT
                    *, ST_Area(geom_28992) AS footprint_sqm, 
                    (ST_SummaryStats(raster)).count AS n_pixels, (ST_SummaryStats(raster)).max AS height
                FROM clipped_rasters
            ), 
            building_nfloors AS (
                SELECT 
                    CASE
                        WHEN height < 4 THEN 1 
                        WHEN height > 4 THEN ROUND(height / 3)
                        ELSE 1 
                    END AS n_floors, 
                    * 
                FROM building_heights
            ), 
            building_sqm_ahn AS (
                SELECT
                    (n_floors * footprint_sqm)::INTEGER AS sqm, * 
                FROM building_nfloors
            ), 
            to_insert AS (
                SELECT 
                    sqm::INTEGER AS sqm, ahn_version, function, id_pand, build_year, 
                    status, document_date, registration_start, registration_end, geom, 
                    geom_28992, neighborhood_code, neighborhood, municipality, province
                FROM building_sqm_ahn
            )
            SELECT * FROM to_insert; 

            -- delete row that didn't have sqm data 
            DELETE FROM {housing_table_name}
            WHERE sqm IS NULL AND municipality = %s; 
            ''' 
    
    def query_add_landuse_column(self): 
        housing_table_name = self.housing_table_name
        return f'''
            ALTER TABLE {housing_table_name} ADD COLUMN IF NOT EXISTS landuse VARCHAR; 

            -- assign the landuse 'Residential' to all buildings where function was not guessed
            UPDATE {housing_table_name}
            SET landuse = 'Residential'
            WHERE ahn_version IS NULL;
            ''' 
    
    def query_filter_out_non_residential(self): 
        housing_table_name = self.housing_table_name
        return f'''
            INSERT INTO {housing_table_name} (
                landuse, function, sqm, id_pand, geometry,
                build_year, status, document_date, document_number,
                registration_start, registration_end, geom, geom_28992,
                neighborhood_code, neighborhood, municipality, province,
                ahn_version
            )

            WITH housing_test AS (
                SELECT * 
                FROM {housing_table_name}
                WHERE 
                    municipality = %s
                    AND landuse IS NULL
            ), 
            residential_land AS (
                SELECT * 
                FROM landuse_nl
                WHERE municipality = %s AND description = 'Residential'
            )

            -- get buildings within residential area from housing_test 
            -- insert these buildings back into housing_nl 
            SELECT 
                l.description AS landuse, 
                h.function, h.sqm, 
                h.id_pand, h.geometry,
                h.build_year, h.status, h.document_date, h.document_number,
                h.registration_start, h.registration_end, h.geom, h.geom_28992,
                h.neighborhood_code, h.neighborhood, h.municipality, h.province,
                h.ahn_version
            FROM housing_test h
            JOIN residential_land l
            ON 
                h.geom_28992 && l.geom_28992
                AND ST_Within(h.geom_28992, l.geom_28992)
            ORDER BY h.sqm DESC;

            -- remove all rows from the municipality where landuse IS NULL 
            DELETE FROM {housing_table_name}
            WHERE landuse IS NULL AND municipality = %s; 
            ''' 
    
    ###############################################################################################################################################################################
    # CALCULATE EMBODIED EMISSIONS FOR HOUSING IN NL 
    # Embodied emissions are calculated using emissions intensities for construction, renovation, and demolition of housing in NL. 
    ###############################################################################################################################################################################

    def query_create_emissions_embodied_housing_nl_table(self):
        return ''' 
            DROP TABLE IF EXISTS emissions_embodied_housing_nl; 
            CREATE TABLE emissions_embodied_housing_nl (
                year INT, 
                province VARCHAR, municipality VARCHAR, 
                neighborhood VARCHAR, neighborhood_code VARCHAR, 
                status VARCHAR, 
                emissions_embodied_tons NUMERIC, sqm NUMERIC 
            ); 
            '''
    
    def query_calculate_embodied_emissions(self): 
        return f''' 
            INSERT INTO emissions_embodied_housing_nl (
                year, province, municipality, neighborhood, neighborhood_code, 
                status, emissions_embodied_tons, sqm
            )

            WITH housing_nl AS (
                SELECT * 
                FROM housing_nl
                WHERE municipality = %s
            ), 
            emissions AS (
                SELECT 
                    CASE 
                        WHEN status = 'Bouw gestart' THEN (sqm * 316 / 1000.0)::NUMERIC
                        WHEN status = 'Verbouwing pand' THEN (sqm * 126 / 1000.0)::NUMERIC
                        WHEN status = 'Pand gesloopt' THEN (sqm * 77 / 1000.0)::NUMERIC
                        ELSE NULL 
                    END AS emissions_embodied_tons, 
                    LEFT(registration_start, 4)::INTEGER AS year, 
                    * 
                FROM housing_nl 
            ), 
            emissions_grouped AS (
                SELECT 
                    year, province, municipality, neighborhood, neighborhood_code, status, 
                    ROUND(SUM(emissions_embodied_tons), 3) AS emissions_embodied_tons, 
                    SUM(sqm) AS sqm
                FROM emissions
                GROUP BY year, province, municipality, neighborhood, neighborhood_code, status
            )

            SELECT * 
            FROM emissions_grouped
            '''