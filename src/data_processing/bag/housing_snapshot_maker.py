import sys
from src.data_processing._common.database_manager import DatabaseManager

class HousingSnapshotMaker(): 
    def __init__(self, start_year, end_year):
        self.start_year = start_year 
        self.end_year = end_year
        self.municipalities = DatabaseManager().get_municipalities_list()

    def run(self): 
        print(f'\nCreating snapshot of housing (housing_inuse_{self.start_year}_{self.end_year}) for years {self.start_year} t/m {self.end_year} ...')
        self.connect_to_db() 
        self.make_snapshot_table()
        for year in range(self.start_year, self.end_year + 1):
            self.year = year
            self.query = self.make_query() 
            self.fill_snapshot_table()
        self.create_indexes() 

    def connect_to_db(self):
        self.conn = DatabaseManager().connect()
        self.cursor = self.conn.cursor()
        self.engine = DatabaseManager().get_sqlalchemy_engine()

    def make_snapshot_table(self):
        query = f'''
            DROP TABLE IF EXISTS housing_inuse_{self.start_year}_{self.end_year};
            CREATE TABLE housing_inuse_{self.start_year}_{self.end_year} (
                year INTEGER,
                id_pand VARCHAR,
                sqm BIGINT,
                n_units BIGINT, 
                geom GEOMETRY,
                geom_28992 GEOMETRY,
                neighborhood_code VARCHAR,
                wk_code VARCHAR,
                municipality VARCHAR
            ); 
            '''
        self.conn.rollback()
        self.cursor.execute(query)
        self.conn.commit() 

    def fill_snapshot_table(self):
        n_placeholders = self.query.count('%s')
        for i, municipality in enumerate(self.municipalities): 
            output = f"\rYear ({self.year - self.start_year + 1}/{self.end_year - self.start_year + 1}): {self.year} | Municipality ({i+1}/{len(self.municipalities)}): {municipality}                         "
            sys.stdout.write(output)
            sys.stdout.flush()
            self.cursor.execute(self.query, (municipality,) * n_placeholders)
            self.conn.commit()

    def make_query(self): 
        return f''' 
            INSERT INTO housing_inuse_{self.start_year}_{self.end_year} (
                year, id_pand, sqm, n_units, geom, geom_28992, neighborhood_code, wk_code, municipality
            )

            WITH bag_vbo_municipality AS (
                SELECT * 
                FROM bag_vbo 
                WHERE municipality = %s
            ), 
            bag_pand_municipality AS (
                SELECT DISTINCT ON (id_pand) * 
                FROM bag_pand
                WHERE 
                    municipality = %s
                    AND status = 'Pand in gebruik' 
                    AND LEFT(registration_start, 4)::INTEGER <= {self.year}
                    AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > {self.year})
            ), 
            housing_units AS (
                SELECT DISTINCT ON (id_vbo) * 
                FROM bag_vbo_municipality
                WHERE 
                    status = 'Verblijfsobject in gebruik'
                    AND sqm::INTEGER < 9999
                    AND function = 'woonfunctie'
                    AND LEFT(registration_start, 4)::INTEGER <= {self.year}
                    AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > {self.year})
            ), 
            housing_buildings AS (
                SELECT 
                    id_pand, 
                    SUM(sqm::INTEGER) AS sqm, COUNT(*) AS n_units
                FROM housing_units
                GROUP BY id_pand
            )

            SELECT {self.year} AS year, a.*, b.geom, b.geom_28992, b.neighborhood_code, b.wk_code, b.municipality
            FROM housing_buildings a 
            LEFT JOIN bag_pand_municipality b 
            ON a.id_pand = b.id_pand 
            WHERE b.id_pand IS NOT NULL -- remove 0.05 percent of buildings that are not in bag_pand_municipality 
            '''
    
    def create_indexes(self):
        query = f''' 
        -- Individual indexes for housing_inuse
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_municipality ON housing_inuse_{self.start_year}_{self.end_year} (municipality);
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_year ON housing_inuse_{self.start_year}_{self.end_year} (year);
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_wk_code ON housing_inuse_{self.start_year}_{self.end_year} (wk_code);
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_id_pand ON housing_inuse_{self.start_year}_{self.end_year} (id_pand);
        '''
        self.conn.rollback()
        self.cursor.execute(query)
        self.conn.commit()

    def make_query_withGuesses(self):
        return f''' 
            -- get subset of housing units and buildings in municipality
            WITH bag_vbo_municipality AS (
                SELECT * 
                FROM bag_vbo 
                WHERE municipality = %s
            ), 
            bag_pand_municipality AS (
                SELECT DISTINCT ON (id_pand) * 
                FROM bag_pand
                WHERE 
                    municipality = %s
                    AND status = 'Pand in gebruik' 
                    AND LEFT(registration_start, 4)::INTEGER <= {self.year}
                    AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > {self.year})
            ), 
            housing_units AS (
                SELECT * 
                FROM bag_vbo_municipality
                WHERE 
                    status = 'Verblijfsobject in gebruik'
                    AND sqm::INTEGER < 9999
                    AND function = 'woonfunctie'
                    AND LEFT(registration_start, 4)::INTEGER <= {self.year}
                    AND (registration_end IS NULL OR LEFT(registration_end, 4)::INTEGER > {self.year})
            ), 
            housing_buildings AS (
                SELECT 
                    id_pand, 
                    SUM(sqm::INTEGER) AS sqm, 
                    'woonfunctie' AS function
                FROM housing_units
                GROUP BY id_pand
            ), 

            -- get buildings where function and sqm are unknown 	
            buildings_unknown AS (
                SELECT b.* 
                FROM bag_pand_municipality b 
                LEFT JOIN housing_buildings hb ON b.id_pand = hb.id_pand 
                WHERE b.status = 'Pand in gebruik'
                    AND hb.id_pand IS NULL
            ), 

            -- guess function of unknown buildings using landuse data 
            residential_land AS (
                SELECT * 
                FROM landuse_nl
                WHERE municipality = %s AND description = 'Residential'
            ), 
            buildings_unknown_residential AS (
                SELECT b.*, 'woonfunctie' AS function
                FROM buildings_unknown b 
                JOIN residential_land l 
                ON 
                    b.geom_28992 && l.geom_28992
                    AND ST_Within(b.geom_28992, l.geom_28992)
            ), 
                
            -- guess sqm of unknown buildings using ahn data 
            elevation_municipality AS (
                SELECT * 
                FROM ahn_elevation
                WHERE ahn_version = 'ahn2' AND municipality = %s
            ), 
            clipped_rasters AS (
                SELECT 
                    b.function, b.id_pand, b.build_year, 
                    b.status, b.document_date, b.registration_start, b.registration_end, 
                    b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province, 
                    ST_Union(ST_Clip(r.rast, 1, b.geom_28992, -9999)) AS raster
                FROM buildings_unknown_residential b
                LEFT JOIN elevation_municipality r
                ON ST_Intersects(r.rast_geom, b.geom_28992) 
                GROUP BY
                    b.function, b.id_pand, b.build_year, 
                    b.status, b.document_date, b.registration_start, b.registration_end, 
                    b.geom, b.geom_28992, b.neighborhood_code, b.neighborhood, b.municipality, b.province
            ), 
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
            building_sqm AS (
                SELECT
                    (n_floors * footprint_sqm)::INTEGER AS sqm, * 
                FROM building_nfloors
            ), 
            building_sqm_formatted AS (
                SELECT 
                    sqm::INTEGER AS sqm, function, id_pand, build_year, 
                    status, document_date, registration_start, registration_end, geom, 
                    geom_28992, neighborhood_code, neighborhood, municipality, province
                FROM building_sqm
            ), 

            -- combine unknown buildings with housing_buildings
            housing AS ( -- housing in use for this year
                SELECT id_pand, function, sqm FROM building_sqm_formatted
                UNION
                SELECT id_pand, function, sqm FROM housing_buildings 
            ), 
            housing_withbaginfo AS (
                SELECT 
                    {self.year} AS year, h.sqm, 
                    b.id_pand, b.status, b.geom, b.geom_28992, b.province, b.neighborhood_code, b.neighborhood, b.municipality
                FROM housing h 
                LEFT JOIN bag_pand_municipality b 
                ON h.id_pand = b.id_pand
            )

            -- insert final selection into housing_inuse_{self.start_year}_{self.end_year}
            INSERT INTO housing_inuse_{self.start_year}_{self.end_year} (
                year, sqm, id_pand, status, geom, geom_28992, 
                province, neighborhood_code, neighborhood, municipality
            )
            SELECT * FROM housing_withbaginfo WHERE id_pand IS NOT NULL
        '''
