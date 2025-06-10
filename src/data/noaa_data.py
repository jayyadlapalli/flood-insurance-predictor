"""
NOAA Sea Level and Climate Data Processing Module
Handles NOAA tide gauge and sea level rise data for Tampa Bay region
"""

import pandas as pd
import requests
import json
from datetime import datetime, timedelta
from pathlib import Path
import logging
from typing import Dict, List, Optional, Tuple
import numpy as np

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class NOAADataProcessor:
    """
    Processes NOAA sea level, tide, and climate data for Tampa Bay region
    """
    
    def __init__(self, data_dir: str = "data/raw/noaa"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # NOAA API endpoints
        self.tide_api = "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        self.sea_level_api = "https://tidesandcurrents.noaa.gov/sltrends/data"
        
        # Tampa Bay NOAA tide stations
        self.tide_stations = {
            '8726520': {'name': 'St. Petersburg', 'lat': 27.7606, 'lon': -82.6269},
            '8726607': {'name': 'Old Port Tampa', 'lat': 27.8533, 'lon': -82.5533},
            '8726384': {'name': 'Clearwater Beach', 'lat': 27.9783, 'lon': -82.8317},
            '8726724': {'name': 'McKay Bay Entrance', 'lat': 27.9117, 'lon': -82.4317}
        }
        
    def download_sea_level_trends(self, station_id: str) -> pd.DataFrame:
        """
        Download sea level trend data from NOAA for specified station
        
        Args:
            station_id: NOAA station ID
            
        Returns:
            DataFrame with sea level trend data
        """
        cache_file = self.data_dir / f"sea_level_trends_{station_id}.csv"
        
        if cache_file.exists():
            logger.info(f"Loading cached sea level data for station {station_id}")
            return pd.read_csv(cache_file, parse_dates=['Date'])
        
        logger.info(f"Downloading sea level trends for station {station_id}")
        
        try:
            # NOAA Sea Level Trends API
            url = f"{self.sea_level_api}/{station_id}_meantrend.csv"
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Parse CSV data
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            # Clean and standardize columns
            if 'Year' in df.columns:
                df['Date'] = pd.to_datetime(df['Year'], format='%Y')
            
            # Cache the data
            df.to_csv(cache_file, index=False)
            
            logger.info(f"Downloaded {len(df)} sea level records for station {station_id}")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Failed to download sea level data for station {station_id}: {e}")
            raise
    
    def download_tide_data(self, station_id: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Download tide gauge data from NOAA
        
        Args:
            station_id: NOAA station ID
            start_date: Start date (YYYYMMDD)
            end_date: End date (YYYYMMDD)
            
        Returns:
            DataFrame with tide data
        """
        cache_file = self.data_dir / f"tide_data_{station_id}_{start_date}_{end_date}.csv"
        
        if cache_file.exists():
            return pd.read_csv(cache_file, parse_dates=['Date Time'])
        
        logger.info(f"Downloading tide data for station {station_id} from {start_date} to {end_date}")
        
        params = {
            'begin_date': start_date,
            'end_date': end_date,
            'station': station_id,
            'product': 'hourly_height',
            'datum': 'MLLW',
            'units': 'metric',
            'time_zone': 'gmt',
            'format': 'json'
        }
        
        try:
            response = requests.get(self.tide_api, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            if 'data' not in data:
                raise ValueError(f"No tide data returned for station {station_id}")
            
            # Convert to DataFrame
            tide_records = []
            for record in data['data']:
                tide_records.append({
                    'Date Time': pd.to_datetime(record['t']),
                    'Water Level': float(record['v']) if record['v'] != '' else np.nan,
                    'Station': station_id
                })
            
            df = pd.DataFrame(tide_records)
            
            # Cache the data
            df.to_csv(cache_file, index=False)
            
            logger.info(f"Downloaded {len(df)} tide records for station {station_id}")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Failed to download tide data for station {station_id}: {e}")
            raise
    
    def get_all_station_data(self, years_back: int = 5) -> Dict[str, pd.DataFrame]:
        """
        Download data for all Tampa Bay tide stations
        
        Args:
            years_back: Number of years of historical data to download
            
        Returns:
            Dictionary with station data
        """
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years_back * 365)
        
        start_str = start_date.strftime('%Y%m%d')
        end_str = end_date.strftime('%Y%m%d')
        
        station_data = {}
        
        for station_id, info in self.tide_stations.items():
            try:
                # Download tide data
                tide_data = self.download_tide_data(station_id, start_str, end_str)
                
                # Download sea level trends
                try:
                    trend_data = self.download_sea_level_trends(station_id)
                except Exception as e:
                    logger.warning(f"Could not download sea level trends for {station_id}: {e}")
                    trend_data = pd.DataFrame()
                
                station_data[station_id] = {
                    'info': info,
                    'tide_data': tide_data,
                    'trend_data': trend_data
                }
                
            except Exception as e:
                logger.error(f"Failed to download data for station {station_id}: {e}")
                continue
        
        return station_data
    
    def calculate_sea_level_projections(self, station_data: Dict) -> pd.DataFrame:
        """
        Calculate sea level rise projections for Tampa Bay region
        
        Args:
            station_data: Dictionary with station data
            
        Returns:
            DataFrame with sea level projections by year
        """
        logger.info("Calculating sea level rise projections...")
        
        # Aggregate trends from all stations
        trends = []
        
        for station_id, data in station_data.items():
            if not data['trend_data'].empty:
                # Calculate linear trend from historical data
                trend_data = data['trend_data']
                if 'Annual_MSL' in trend_data.columns and len(trend_data) > 10:
                    # Calculate mm/year trend
                    years = trend_data['Date'].dt.year
                    levels = trend_data['Annual_MSL']
                    
                    # Linear regression
                    coeffs = np.polyfit(years, levels, 1)
                    trend_mm_per_year = coeffs[0] * 1000  # Convert to mm/year
                    
                    trends.append({
                        'station_id': station_id,
                        'trend_mm_per_year': trend_mm_per_year
                    })
        
        # Calculate regional average trend
        if trends:
            avg_trend = np.mean([t['trend_mm_per_year'] for t in trends])
        else:
            # Use NOAA Tampa Bay regional estimate if no station data available
            avg_trend = 2.4  # mm/year based on NOAA regional assessments
        
        # Project future sea levels (2025-2035)
        current_year = datetime.now().year
        projections = []
        
        for year in range(current_year, 2036):
            years_from_now = year - current_year
            sea_level_rise_mm = avg_trend * years_from_now
            
            # Add uncertainty bounds
            lower_bound = sea_level_rise_mm * 0.7
            upper_bound = sea_level_rise_mm * 1.3
            
            projections.append({
                'year': year,
                'sea_level_rise_mm': sea_level_rise_mm,
                'lower_bound_mm': lower_bound,
                'upper_bound_mm': upper_bound,
                'trend_mm_per_year': avg_trend
            })
        
        return pd.DataFrame(projections)
    
    def get_storm_surge_frequency(self) -> pd.DataFrame:
        """
        Estimate storm surge frequency based on historical hurricane data
        """
        logger.info("Calculating storm surge frequency estimates...")
        
        # Historical hurricane impacts on Tampa Bay (simplified model)
        # In production, this would use HURDAT2 database
        
        # Estimate based on regional climate patterns
        base_frequency = 0.15  # 15% annual chance of significant storm surge
        
        projections = []
        current_year = datetime.now().year
        
        for year in range(current_year, 2036):
            # Increase frequency with climate change (conservative estimate)
            years_from_now = year - current_year
            frequency_increase = years_from_now * 0.005  # 0.5% increase per year
            
            surge_frequency = base_frequency + frequency_increase
            
            projections.append({
                'year': year,
                'storm_surge_frequency': min(surge_frequency, 0.5),  # Cap at 50%
                'category_1_freq': surge_frequency * 0.6,
                'category_2_freq': surge_frequency * 0.3,
                'category_3_plus_freq': surge_frequency * 0.1
            })
        
        return pd.DataFrame(projections)
    
    def get_climate_summary(self) -> Dict:
        """
        Generate climate summary for Tampa Bay region
        """
        try:
            station_data = self.get_all_station_data(years_back=10)
            sea_level_proj = self.calculate_sea_level_projections(station_data)
            storm_proj = self.get_storm_surge_frequency()
            
            return {
                'sea_level_projections': sea_level_proj,
                'storm_surge_projections': storm_proj,
                'station_data': station_data,
                'summary': {
                    'avg_sea_level_rise_mm_per_year': sea_level_proj['trend_mm_per_year'].iloc[0],
                    'total_rise_by_2035_mm': sea_level_proj[sea_level_proj['year'] == 2035]['sea_level_rise_mm'].iloc[0],
                    'current_storm_surge_frequency': storm_proj[storm_proj['year'] == datetime.now().year]['storm_surge_frequency'].iloc[0]
                }
            }
        except Exception as e:
            logger.error(f"Failed to generate climate summary: {e}")
            raise