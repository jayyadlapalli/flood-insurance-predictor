"""
NFIP Insurance Data Processing Module
Handles National Flood Insurance Program (NFIP) claims and premium data
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

class NFIPDataProcessor:
    """
    Processes NFIP insurance claims and premium data for flood risk analysis
    """
    
    def __init__(self, data_dir: str = "data/raw/nfip"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        
        # FEMA OpenFEMA API for NFIP data
        self.base_url = "https://www.fema.gov/api/open/v2"
        
        # Florida state code for filtering
        self.florida_state_code = "12"
        
        # Tampa Bay counties FIPS codes
        self.tampa_bay_counties = {
            '12057': 'Hillsborough',
            '12103': 'Pinellas', 
            '12101': 'Pasco',
            '12081': 'Manatee'
        }
        
    def download_nfip_claims(self, limit: int = 10000) -> pd.DataFrame:
        """
        Download NFIP claims data from FEMA OpenFEMA API
        
        Args:
            limit: Maximum number of records to retrieve
            
        Returns:
            DataFrame with NFIP claims data
        """
        cache_file = self.data_dir / "nfip_claims_tampa_bay.csv"
        
        if cache_file.exists():
            logger.info("Loading cached NFIP claims data")
            return pd.read_csv(cache_file, parse_dates=['dateOfLoss'])
        
        logger.info("Downloading NFIP claims data from FEMA OpenFEMA API")
        
        # Filter for Florida counties in Tampa Bay region
        county_filter = " or ".join([f"countyCode eq '{code}'" for code in self.tampa_bay_counties.keys()])
        
        params = {
            '$filter': f"state eq 'FL' and ({county_filter})",
            '$top': limit,
            '$orderby': 'dateOfLoss desc'
        }
        
        try:
            response = requests.get(f"{self.base_url}/FimaNfipClaims", params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if 'FimaNfipClaims' not in data:
                raise ValueError("No NFIP claims data returned from API")
            
            claims_data = data['FimaNfipClaims']
            df = pd.DataFrame(claims_data)
            
            # Convert date columns
            if 'dateOfLoss' in df.columns:
                df['dateOfLoss'] = pd.to_datetime(df['dateOfLoss'], errors='coerce')
            
            # Add county names
            df['county_name'] = df['countyCode'].map(self.tampa_bay_counties)
            
            # Cache the data
            df.to_csv(cache_file, index=False)
            
            logger.info(f"Downloaded {len(df)} NFIP claims records")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Failed to download NFIP claims data: {e}")
            raise
    
    def download_nfip_policies(self, limit: int = 10000) -> pd.DataFrame:
        """
        Download NFIP policy data from FEMA OpenFEMA API
        """
        cache_file = self.data_dir / "nfip_policies_tampa_bay.csv"
        
        if cache_file.exists():
            logger.info("Loading cached NFIP policy data")
            return pd.read_csv(cache_file)
        
        logger.info("Downloading NFIP policy data from FEMA OpenFEMA API")
        
        county_filter = " or ".join([f"countyCode eq '{code}'" for code in self.tampa_bay_counties.keys()])
        
        params = {
            '$filter': f"state eq 'FL' and ({county_filter})",
            '$top': limit,
            '$orderby': 'policyEffectiveDate desc'
        }
        
        try:
            response = requests.get(f"{self.base_url}/FimaNfipPolicies", params=params, timeout=60)
            response.raise_for_status()
            
            data = response.json()
            
            if 'FimaNfipPolicies' not in data:
                raise ValueError("No NFIP policy data returned from API")
            
            policies_data = data['FimaNfipPolicies']
            df = pd.DataFrame(policies_data)
            
            # Add county names
            df['county_name'] = df['countyCode'].map(self.tampa_bay_counties)
            
            # Cache the data
            df.to_csv(cache_file, index=False)
            
            logger.info(f"Downloaded {len(df)} NFIP policy records")
            return df
            
        except requests.RequestException as e:
            logger.error(f"Failed to download NFIP policy data: {e}")
            raise
    
    def calculate_zip_premium_stats(self) -> pd.DataFrame:
        """
        Calculate insurance premium statistics by ZIP code
        """
        logger.info("Calculating insurance premium statistics by ZIP code")
        
        try:
            claims_data = self.download_nfip_claims()
            policies_data = self.download_nfip_policies()
            
            # Process claims data by ZIP
            if not claims_data.empty and 'propertyZipCode' in claims_data.columns:
                claims_by_zip = claims_data.groupby('propertyZipCode').agg({
                    'totalInsuredValue': ['mean', 'sum', 'count'],
                    'amountPaidOnBuildingClaim': ['mean', 'sum'],
                    'amountPaidOnContentsClaim': ['mean', 'sum'],
                    'dateOfLoss': ['min', 'max']
                }).reset_index()
                
                # Flatten column names
                claims_by_zip.columns = [
                    'zip_code', 'avg_insured_value', 'total_insured_value', 'total_claims',
                    'avg_building_payout', 'total_building_payout',
                    'avg_contents_payout', 'total_contents_payout',
                    'first_claim_date', 'last_claim_date'
                ]
            else:
                claims_by_zip = pd.DataFrame()
            
            # Process policy data by ZIP
            if not policies_data.empty and 'propertyZipCode' in policies_data.columns:
                policies_by_zip = policies_data.groupby('propertyZipCode').agg({
                    'totalPremium': ['mean', 'sum', 'count'],
                    'totalCoverage': ['mean', 'sum'],
                    'deductibleAmountInBuildingCoverage': 'mean'
                }).reset_index()
                
                # Flatten column names
                policies_by_zip.columns = [
                    'zip_code', 'avg_premium', 'total_premiums', 'total_policies',
                    'avg_coverage', 'total_coverage', 'avg_deductible'
                ]
            else:
                policies_by_zip = pd.DataFrame()
            
            # Merge claims and policy data
            if not claims_by_zip.empty and not policies_by_zip.empty:
                zip_stats = pd.merge(policies_by_zip, claims_by_zip, on='zip_code', how='outer')
            elif not policies_by_zip.empty:
                zip_stats = policies_by_zip
            elif not claims_by_zip.empty:
                zip_stats = claims_by_zip
            else:
                # Return empty DataFrame with expected columns
                zip_stats = pd.DataFrame(columns=[
                    'zip_code', 'avg_premium', 'total_policies', 'total_claims',
                    'avg_building_payout', 'loss_ratio'
                ])
            
            # Calculate additional metrics
            if not zip_stats.empty:
                # Loss ratio (payouts / premiums)
                if 'total_building_payout' in zip_stats.columns and 'total_premiums' in zip_stats.columns:
                    zip_stats['loss_ratio'] = (
                        zip_stats['total_building_payout'].fillna(0) / 
                        zip_stats['total_premiums'].replace(0, np.nan)
                    ).fillna(0)
                
                # Claims frequency (claims per policy)
                if 'total_claims' in zip_stats.columns and 'total_policies' in zip_stats.columns:
                    zip_stats['claims_frequency'] = (
                        zip_stats['total_claims'].fillna(0) / 
                        zip_stats['total_policies'].replace(0, np.nan)
                    ).fillna(0)
            
            return zip_stats
            
        except Exception as e:
            logger.error(f"Failed to calculate ZIP premium statistics: {e}")
            raise
    
    def predict_premium_changes(self, flood_risk_data: pd.DataFrame) -> pd.DataFrame:
        """
        Predict insurance premium changes based on flood risk projections
        
        Args:
            flood_risk_data: DataFrame with flood risk scores by ZIP
            
        Returns:
            DataFrame with premium predictions
        """
        logger.info("Predicting insurance premium changes")
        
        try:
            current_premiums = self.calculate_zip_premium_stats()
            
            # Merge with flood risk data
            premium_predictions = pd.merge(
                flood_risk_data, 
                current_premiums, 
                on='zip_code', 
                how='left'
            )
            
            # Calculate baseline premium where data is missing
            if not current_premiums.empty and 'avg_premium' in current_premiums.columns:
                baseline_premium = current_premiums['avg_premium'].median()
            else:
                baseline_premium = 1200  # National average NFIP premium
            
            premium_predictions['avg_premium'] = premium_predictions['avg_premium'].fillna(baseline_premium)
            
            # Predict future premiums based on flood risk
            projections = []
            current_year = datetime.now().year
            
            for _, row in premium_predictions.iterrows():
                zip_code = row['zip_code']
                current_premium = row['avg_premium']
                flood_risk_score = row.get('flood_risk_score', 0.1)
                
                for year in range(current_year, 2036):
                    years_from_now = year - current_year
                    
                    # Risk-based premium adjustment
                    risk_multiplier = 1 + (flood_risk_score * 0.1 * years_from_now)
                    
                    # Climate change adjustment (2-4% annual increase)
                    climate_multiplier = (1.03) ** years_from_now
                    
                    # Market adjustment (inflation, regulatory changes)
                    market_multiplier = (1.025) ** years_from_now
                    
                    predicted_premium = current_premium * risk_multiplier * climate_multiplier * market_multiplier
                    
                    projections.append({
                        'zip_code': zip_code,
                        'year': year,
                        'predicted_premium': predicted_premium,
                        'current_premium': current_premium,
                        'premium_increase_pct': ((predicted_premium - current_premium) / current_premium) * 100,
                        'flood_risk_score': flood_risk_score
                    })
            
            return pd.DataFrame(projections)
            
        except Exception as e:
            logger.error(f"Failed to predict premium changes: {e}")
            raise
    
    def get_insurance_summary(self) -> Dict:
        """
        Generate insurance summary statistics for Tampa Bay region
        """
        try:
            claims_data = self.download_nfip_claims()
            policies_data = self.download_nfip_policies()
            zip_stats = self.calculate_zip_premium_stats()
            
            summary = {
                'total_claims': len(claims_data) if not claims_data.empty else 0,
                'total_policies': len(policies_data) if not policies_data.empty else 0,
                'zip_codes_analyzed': len(zip_stats) if not zip_stats.empty else 0
            }
            
            if not zip_stats.empty:
                if 'avg_premium' in zip_stats.columns:
                    summary['avg_regional_premium'] = zip_stats['avg_premium'].median()
                    summary['premium_range'] = {
                        'min': zip_stats['avg_premium'].min(),
                        'max': zip_stats['avg_premium'].max()
                    }
                
                if 'loss_ratio' in zip_stats.columns:
                    summary['avg_loss_ratio'] = zip_stats['loss_ratio'].median()
            
            return summary
            
        except Exception as e:
            logger.error(f"Failed to generate insurance summary: {e}")
            return {'error': str(e)}