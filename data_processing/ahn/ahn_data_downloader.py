import rasterio
import requests
import geopandas as gpd
from concurrent.futures import ThreadPoolExecutor

class AHNDataDownloader(): 
    def __init__(self): 
        self.grid = gpd.read_file('data/processed/ahn/grid_cropped.shp')
        self.url_dict = {
            'dsm': { # dsm = digital surface model = elevation of earth's surface including all objects in it (e.g. buildings, trees, etc.)
                'ahn2': (
                    f'https://api.ellipsis-drive.com/v3/ogc/wcs/28fb0f5f-e367-4265-b84b-1b8f1a8a6409?service=WCS&request=GetCoverage&version=1.0.0'
                    f'&coverage=ae53fd8e-6310-42b5-9a43-12c588ff61c1&crs=EPSG:28992&format=GeoTIFF&width=2048&height=2048' # &bbox={bbox}'
                ), 
                'ahn3': (
                    f'https://api.ellipsis-drive.com/v3/ogc/wcs/94037eea-4196-48db-9f83-0ef330a7655e?SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage'
                    f'&COVERAGE=7daaa215-4cd0-48cd-9484-2eb5cec674e1&FORMAT=geotiff&CRS=EPSG:28992&width=2048&height=2048' # &WIDTH={max_width}&HEIGHT={max_height}&BBOX={bbox}'
                ), 
                'ahn4': (
                    f'https://api.ellipsis-drive.com/v3/ogc/wcs/78080fff-8bcb-4258-bb43-be9de956b3e0?SERVICE=WCS&VERSION=1.0.0&REQUEST=GetCoverage'
                    f'&COVERAGE=fa6672ac-7a4f-47cd-bcea-806584f0bfe3&FORMAT=GeoTIFF&CRS=EPSG:28992&width=2048&height=2048' # &BBOX={bbox}&WIDTH={max_width}&HEIGHT={max_height}'
                ), 
            }, 
            'dtm': { # dtm = digital terrain model = elevation of the bare earth surface
                'ahn2': (
                    f'https://api.ellipsis-drive.com/v3/ogc/wcs/e96b10b9-e964-414c-958c-57a9dbe24e62?service=WCS&request=GetCoverage&version=1.0.0'
                    f'&coverage=f380e0a0-4446-4d79-b2d1-3d4df40cbc93&crs=EPSG:28992&format=GeoTIFF&width=2048&height=2048'
                ), 
                'ahn3': (
                    f'https://api.ellipsis-drive.com/v3/ogc/wcs/69f81443-c000-4479-b08f-2078e3570394?service=WCS&request=GetCoverage&version=1.0.0'
                    f'&coverage=393408cf-842d-4181-af87-94f6123bdff0&crs=EPSG:28992&format=GeoTIFF&width=2048&height=2048'
                ), 
                'ahn4': (
                    f'https://api.ellipsis-drive.com/v3/ogc/wcs/8b60a159-42ed-480c-ba86-7f181dcf4b8a?service=WCS&request=GetCoverage&version=1.0.0'
                    f'&coverage=54e1e21c-50fe-42a4-b9fb-1f25ccb199af&crs=EPSG:28992&format=GeoTIFF&width=2048&height=2048'
                ), 
            }
        }

    def request_tiff_fromBbox(self, bbox, grid_id, model_name, ahn_version, print=False, return_response=False):
        url = self.url_dict[model_name][ahn_version] + f'&bbox={bbox}'
        response = requests.get(url)
        file_path = f'data/ahn/{ahn_version}_{model_name}_{grid_id}.tif'
        with open(file_path, 'wb') as f:
            f.write(response.content)
        if print: 
            print(f'saved {file_path}')
        if return_response: 
            return response

    def convert_tiff(self, id, model_name, ahn_version):
        file_path = f'data/ahn/{ahn_version}_{model_name}_{id}.tif'
        with rasterio.open(file_path, 'r+') as dataset:
            data = dataset.read(1)
            data = data + 0.1
            dataset.write(data, 1)

    def request_tiff_fromGridApply(self, row):
        for model_name in ['dsm', 'dtm']:
            for ahn_version in ['ahn2', 'ahn3', 'ahn4']:
                self.request_tiff_fromBbox(row.bbox, row.grid_id, model_name, ahn_version)
                print(f'requested {model_name} {ahn_version} {row.grid_id}')
                if model_name == 'dsm' and ahn_version == 'ahn2':
                    self.convert_tiff(row.grid_id, model_name, ahn_version)
        
    def parallel_request_tiff(self, grid):
        with ThreadPoolExecutor(max_workers=10) as executor:
            executor.map(self.request_tiff_fromGridApply, [row for _, row in grid.iterrows()])

    def run(self): 
        self.parallel_request_tiff(self.grid)