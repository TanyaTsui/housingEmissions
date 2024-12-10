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
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_geom ON housing_inuse_{self.start_year}_{self.end_year} USING GIST (geom_28992);
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


class HousingSnapshotBuurtStatsAdder(): 
    def __init__(self, start_year, end_year):
        self.start_year = start_year 
        self.end_year = end_year
        self.municipalities = DatabaseManager().get_municipalities_list()

    def run(self):
        print(f'\nAdding buurt stats to housing_inuse_{self.start_year}_{self.end_year} ...')
        self.connect_to_db()
        for year in range(self.start_year, self.end_year + 1):
            self.year = year
            self.fill_table()
        self.create_indexes() 

    def connect_to_db(self):
        self.conn = DatabaseManager().connect()
        self.cursor = self.conn.cursor()
        self.engine = DatabaseManager().get_sqlalchemy_engine()

    def fill_table(self):
        for i, municipality in enumerate(self.municipalities): 
            self.municipality = municipality
            self.query = self.make_query()
            n_placeholders = self.query.count('%s')
            output = f"\rYear ({self.year - self.start_year + 1}/{self.end_year - self.start_year + 1}): {self.year} | Municipality ({i+1}/{len(self.municipalities)}): {municipality}                         "
            sys.stdout.write(output)
            sys.stdout.flush()
            self.cursor.execute(self.query, (municipality,) * n_placeholders)
            self.conn.commit()

    def make_query(self): 
        return f'''  
        -- get 2022 buurt geometry for Delft and make bbox 
        WITH buurt2022_municipality AS (
            SELECT "BU_CODE" AS bu_code, geometry AS bu_geom, "GM_NAAM" AS municipality
            FROM cbs_map_2022 
            WHERE "WATER" = 'NEE' AND "GM_NAAM" = %s
        ), 
        bbox_buurt2022 AS (
            SELECT ST_Buffer(ST_MakeEnvelope(ST_XMin(extent), ST_YMin(extent), ST_XMax(extent), ST_YMax(extent), 28992), 500) AS bbox_geom
            FROM (SELECT ST_Extent(bu_geom) AS extent FROM buurt2022_municipality) AS subquery
        ), 

        -- use bbox to select buurt-level cbs data from cbs_map_{self.year} (and other years as well)
        cbs_data_year AS (
            SELECT b.*, a.geometry, a."BU_CODE" AS bu_code, 
                CASE WHEN "AANT_INW" < 0 THEN NULL ELSE "AANT_INW" END AS population, 
                CASE WHEN "WONINGEN" < 0 THEN NULL ELSE "WONINGEN" END AS n_homes, 
                CASE WHEN "WOZ" < 0 THEN NULL ELSE "WOZ" END AS woz, 
                CASE WHEN "G_GAS_TOT" < 0 THEN NULL ELSE "G_GAS_TOT" END AS av_gas_m3, 
                CASE WHEN "G_ELEK_TOT" < 0 THEN NULL ELSE "G_ELEK_TOT" END AS av_elec_kwh 
            FROM cbs_map_{self.year} a 
            JOIN bbox_buurt2022 b 
            ON a.geometry && b.bbox_geom
        ), 
        bbox_cbs_data_year AS (
            SELECT ST_Buffer(ST_MakeEnvelope(ST_XMin(extent), ST_YMin(extent), ST_XMax(extent), ST_YMax(extent), 28992), 500) AS bbox_geom
            FROM (SELECT ST_Extent(geometry) AS extent FROM cbs_data_year) AS subquery
        ), 

        -- use bbox to select building-level data from housing_inuse_2012_2022 
        -- (this is the only option. Municipality names in housing_inuse are wrong (based on 2012 wijks), I checked)
        housing_inuse_in_bbox AS (
            SELECT a.*
            FROM (SELECT * FROM housing_inuse_2012_2021 WHERE year = {self.year}) a
            JOIN bbox_cbs_data_year b 
            ON a.pd_geom && b.bbox_geom
        ), 

        -- assign {self.year} bu_codes to inuse buildings, get sqm in use per bu_code
        housing_inuse_with_bucodes AS (
            SELECT a.year, a.id_pand, a.sqm, a.n_units, a.pd_geom, 
                b.bu_code AS bu_code_year
            FROM housing_inuse_in_bbox a
            JOIN cbs_data_year b 
            ON a.pd_geom && b.geometry
                AND ST_Intersects(a.pd_geom, b.geometry)
            WHERE ST_Area(ST_Intersection(a.pd_geom, b.geometry)) / ST_Area(a.pd_geom) * 100 > 50
        ), 
        inuse_sqm_per_bucode AS (
            SELECT bu_code_year, SUM(sqm) AS inuse_sqm 
            FROM housing_inuse_with_bucodes
            GROUP BY bu_code_year
        ), 

        -- assign {self.year} cbs data to buildings proportionally with inuse sqm 
        cbs_stats_year AS (
            SELECT a.*, b.geometry AS bu_geom, 
                b.population, b.n_homes, b.woz,  
                b.n_homes*b.av_gas_m3 AS tot_gas_m3, b.n_homes*b.av_elec_kwh AS tot_elec_kwh
            FROM inuse_sqm_per_bucode a 
            LEFT JOIN cbs_data_year b 
            ON a.bu_code_year = b.bu_code
        ), 
        building_stats AS (
            SELECT a.year, a.id_pand, 
                a.sqm, a.n_units, a.pd_geom,  
                b.population * a.sqm / b.inuse_sqm AS population, 
                b.n_homes * a.sqm / b.inuse_sqm AS n_homes,
                b.tot_gas_m3 * a.sqm / b.inuse_sqm AS tot_gas_m3,
                b.tot_elec_kwh * a.sqm / b.inuse_sqm AS tot_elec_kwh,
                b.woz AS woz
            FROM housing_inuse_with_bucodes a 
            JOIN cbs_stats_year b
            ON a.bu_code_year = b.bu_code_year
        ), 

        -- add 2022 buurt code to building_stats 
        building_stats_with_2022bucode AS (
            SELECT a.*, b.bu_code, b.municipality
            FROM building_stats a 
            JOIN buurt2022_municipality b 
            ON a.pd_geom && b.bu_geom 
                AND ST_Intersects(a.pd_geom, b.bu_geom)
            WHERE ST_Area(ST_Intersection(a.pd_geom, b.bu_geom)) / ST_Area(a.pd_geom) * 100 > 50
        )

        UPDATE housing_inuse_2012_2021 AS h
        SET 
            population = b.population,
            n_homes = b.n_homes,
            tot_gas_m3 = b.tot_gas_m3,
            tot_elec_kwh = b.tot_elec_kwh,
            woz = b.woz,
            bu_code = b.bu_code,
            municipality = b.municipality
        FROM building_stats_with_2022bucode AS b
        WHERE h.id_pand = b.id_pand AND h.year = b.year

        '''

    def create_indexes(self):
        print(f'\nCreating indexes for housing_inuse_{self.start_year}_{self.end_year} ...')
        query = f''' 
        -- Individual indexes for housing_inuse
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_municipality ON housing_inuse_{self.start_year}_{self.end_year} (municipality);
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_year ON housing_inuse_{self.start_year}_{self.end_year} (year);
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_id_pand ON housing_inuse_{self.start_year}_{self.end_year} (id_pand);
        CREATE INDEX IF NOT EXISTS idx_housing_inuse_pd_geom ON housing_inuse_{self.start_year}_{self.end_year} USING GIST (pd_geom);
        '''
        self.conn.rollback()
        self.cursor.execute(query)
        self.conn.commit()
