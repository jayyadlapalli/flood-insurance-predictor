"""
FEMA Flood Map Data Processing Module
Handles FEMA National Flood Hazard Layer (NFHL) data for Tampa Bay region
"""

import geopandas as gpd
import pandas as pd
import requests
import json
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
from shapely.geometry import Point, Polygon
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FEMADataProcessor:
    """
    Processes FEMA flood hazard data for Tampa Bay region using official FEMA Map Service Center
    """
    
    def __init__(self, data_dir: str = "data/raw/fema"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # FEMA Map Service Center REST API
        self.base_url = "https://hazards.fema.gov/nfhlv2/rest/services/public/NFHLV2/MapServer"
        
        # FEMA flood zone classifications
        self.flood_zones = {
            'A': 'High Risk - 1% annual chance flood',
            'AE': 'High Risk - 1% annual chance flood with BFE',
            'AH': 'High Risk - 1% annual chance shallow flooding',
            'AO': 'High Risk - 1% annual chance sheet flow flooding',
            'AR': 'High Risk - Areas with reduced flood risk',
            'A99': 'High Risk - Areas with flood control systems',
            'V': 'High Risk - Coastal areas with velocity hazard',
            'VE': 'High Risk - Coastal areas with velocity hazard and BFE',
            'X': 'Moderate to Low Risk - 0.2% annual chance',
            'D': 'Undetermined Risk'
        }
        
        # Tampa Bay region bounding box
        self.tampa_bay_bbox = {
            'xmin': -82.9,
            'ymin': 27.3,
            'xmax': -82.0,
            'ymax': 28.7
        }
        
    def download_flood_zones(self, output_format: str = "geojson") -> str:
        """
        Download FEMA flood hazard zones for Tampa Bay region
        
        Args:
            output_format: Output format ('geojson', 'shapefile')
            
        Returns:
            Path to downloaded data
        """
        output_file = self.data_dir / f"tampa_bay_flood_zones.{output_format}"
        
        if output_file.exists():
            logger.info(f"FEMA flood zone data already exists: {output_file}")
            return str(output_file)
        
        logger.info("Downloading FEMA flood zone data for Tampa Bay region...")
        
        # FEMA REST API query parameters
        params = {
            'f': 'json',
            'where': '1=1',
            'outFields': '*',
            'geometry': f"{self.tampa_bay_bbox['xmin']},{self.tampa_bay_bbox['ymin']},{self.tampa_bay_bbox['xmax']},{self.tampa_bay_bbox['ymax']}",
            'geometryType': 'esriGeometryEnvelope',
            'spatialRel': 'esriSpatialRelIntersects',
            'returnGeometry': 'true',
            'maxRecordCount': 2000
        }
        
        try:
            # Query FEMA flood hazard layer
            response = requests.get(f"{self.base_url}/1/query", params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'features' not in data or len(data['features']) == 0:
                raise ValueError("No flood zone data returned from FEMA API")
            
            # Convert to GeoDataFrame
            features = data['features']
            gdf = gpd.GeoDataFrame.from_features(features, crs='EPSG:4326')
            
            # Save to file
            if output_format == 'geojson':
                gdf.to_file(output_file, driver='GeoJSON')
            elif output_format == 'shapefile':
                gdf.to_file(output_file, driver='ESRI Shapefile')
            
            logger.info(f"Downloaded {len(gdf)} flood zones to {output_file}")
            return str(output_file)
            
        except requests.RequestException as e:
            logger.error(f"Failed to download FEMA data: {e}")
            raise
        except Exception as e:
            logger.error(f"Error processing FEMA data: {e}")
            raise
    
    def load_flood_zones(self) -> gpd.GeoDataFrame:
        """
        Load FEMA flood zone data
        """
        file_path = self.download_flood_zones()
        return gpd.read_file(file_path)
    
    def get_zip_code_boundaries(self) -> gpd.GeoDataFrame:
        """
        Download ZIP code boundaries for Tampa Bay region from US Census
        """
        zip_file = self.data_dir / "tampa_bay_zip_codes.geojson"
        
        if zip_file.exists():
            return gpd.read_file(zip_file)
        
        logger.info("Downloading ZIP code boundaries from US Census...")
        
        # US Census ZIP Code Tabulation Areas (ZCTA) API
        base_url = "https://www2.census.gov/geo/tiger/GENZ2020/shp"
        zip_url = f"{base_url}/cb_2020_us_zcta520_500k.zip"
        
        try:
            # Download and extract ZIP code shapefile
            import zipfile
            import urllib.request
            
            zip_download = self.data_dir / "census_zip_codes.zip"
            urllib.request.urlretrieve(zip_url, zip_download)
            
            with zipfile.ZipFile(zip_download, 'r') as zip_ref:
                zip_ref.extractall(self.data_dir)
            
            # Load shapefile
            shapefile_path = self.data_dir / "cb_2020_us_zcta520_500k.shp"
            all_zips = gpd.read_file(shapefile_path)
            
            # Filter to Tampa Bay region
            tampa_zips = all_zips.cx[
                self.tampa_bay_bbox['xmin']:self.tampa_bay_bbox['xmax'],
                self.tampa_bay_bbox['ymin']:self.tampa_bay_bbox['ymax']
            ]
            
            # Save filtered ZIP codes
            tampa_zips.to_file(zip_file, driver='GeoJSON')
            
            logger.info(f"Downloaded {len(tampa_zips)} ZIP codes for Tampa Bay region")
            return tampa_zips
            
        except Exception as e:
            logger.error(f"Failed to download ZIP code boundaries: {e}")
            raise
    
    def calculate_zip_flood_risk(self) -> pd.DataFrame:
        """
        Calculate flood risk metrics for each ZIP code in Tampa Bay
        """
        logger.info("Calculating flood risk metrics for ZIP codes...")
        
        flood_zones = self.load_flood_zones()
        zip_boundaries = self.get_zip_code_boundaries()
        
        zip_risk_data = []
        
        for _, zip_row in zip_boundaries.iterrows():
            zip_code = zip_row['ZCTA5CE20']  # ZIP code column in 2020 census data
            zip_geom = zip_row['geometry']
            
            # Find overlapping flood zones
            overlapping_zones = flood_zones[flood_zones.geometry.intersects(zip_geom)]
            
            if len(overlapping_zones) == 0:
                risk_score = 0.1
                high_risk_pct = 0.0
                moderate_risk_pct = 0.0
                zone_types = []
            else:
                # Calculate area percentages
                total_zip_area = zip_geom.area
                high_risk_area = 0
                moderate_risk_area = 0
                zone_types = []
                
                for _, zone in overlapping_zones.iterrows():
                    zone_type = zone.get('FLD_ZONE', zone.get('ZONE_SUBTY', 'Unknown'))
                    zone_types.append(zone_type)
                    
                    intersection = zip_geom.intersection(zone.geometry)
                    intersection_area = intersection.area
                    
                    if zone_type in ['A', 'AE', 'AH', 'AO', 'AR', 'A99', 'V', 'VE']:
                        high_risk_area += intersection_area
                    elif zone_type in ['X']:
                        moderate_risk_area += intersection_area
                
                high_risk_pct = (high_risk_area / total_zip_area) * 100
                moderate_risk_pct = (moderate_risk_area / total_zip_area) * 100
                
                # Calculate overall risk score (0-1)
                risk_score = min((high_risk_pct * 0.8 + moderate_risk_pct * 0.3) / 100, 1.0)
            
            zip_risk_data.append({
                'zip_code': zip_code,
                'flood_risk_score': risk_score,
                'high_risk_area_pct': high_risk_pct,
                'moderate_risk_area_pct': moderate_risk_pct,
                'total_flood_zones': len(overlapping_zones),
                'zone_types': ','.join(set(zone_types))
            })
        
        return pd.DataFrame(zip_risk_data)
    
    def get_zone_descriptions(self) -> Dict[str, str]:
        """
        Return FEMA flood zone descriptions
        """
        return self.flood_zones