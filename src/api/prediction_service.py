"""
FastAPI service for flood risk and insurance premium predictions
"""

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import Dict, List, Optional
import sys
from pathlib import Path
import logging

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from data.fema_data import FEMADataProcessor
from data.noaa_data import NOAADataProcessor
from data.insurance_data import NFIPDataProcessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Tampa Bay Flood Risk Predictor API",
    description="API for predicting flood risk and insurance premium changes",
    version="1.0.0"
)

# Request/Response models
class FloodRiskRequest(BaseModel):
    zip_code: str
    year: Optional[int] = 2030

class PremiumRequest(BaseModel):
    zip_code: str
    property_type: str = "single_family"
    year: Optional[int] = 2030

class FloodRiskResponse(BaseModel):
    zip_code: str
    flood_risk_score: float
    risk_category: str
    high_risk_area_pct: float
    year: int

class PremiumResponse(BaseModel):
    zip_code: str
    current_premium: float
    predicted_premium: float
    premium_increase_pct: float
    year: int

# Initialize data processors
fema_processor = FEMADataProcessor()
noaa_processor = NOAADataProcessor()
nfip_processor = NFIPDataProcessor()

@app.get("/")
async def root():
    return {"message": "Tampa Bay Flood Risk Predictor API"}

@app.get("/health")
async def health_check():
    return {"status": "healthy", "service": "flood-risk-predictor"}

@app.post("/predict/flood-risk", response_model=FloodRiskResponse)
async def predict_flood_risk(request: FloodRiskRequest):
    """
    Predict flood risk for a specific ZIP code and year
    """
    try:
        # Get flood risk data
        flood_risk_data = fema_processor.calculate_zip_flood_risk()
        
        # Find data for requested ZIP code
        zip_data = flood_risk_data[flood_risk_data['zip_code'] == request.zip_code]
        
        if zip_data.empty:
            raise HTTPException(
                status_code=404, 
                detail=f"No flood risk data found for ZIP code {request.zip_code}"
            )
        
        risk_score = float(zip_data['flood_risk_score'].iloc[0])
        high_risk_pct = float(zip_data['high_risk_area_pct'].iloc[0])
        
        # Determine risk category
        if risk_score > 0.7:
            risk_category = "Very High"
        elif risk_score > 0.5:
            risk_category = "High"
        elif risk_score > 0.3:
            risk_category = "Moderate"
        else:
            risk_category = "Low"
        
        return FloodRiskResponse(
            zip_code=request.zip_code,
            flood_risk_score=risk_score,
            risk_category=risk_category,
            high_risk_area_pct=high_risk_pct,
            year=request.year
        )
        
    except Exception as e:
        logger.error(f"Error predicting flood risk: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")

@app.post("/predict/premium", response_model=PremiumResponse)
async def predict_premium(request: PremiumRequest):
    """
    Predict insurance premium for a specific ZIP code, property type, and year
    """
    try:
        # Get flood risk data
        flood_risk_data = fema_processor.calculate_zip_flood_risk()
        
        # Get premium predictions
        premium_projections = nfip_processor.predict_premium_changes(flood_risk_data)
        
        # Filter for requested ZIP and year
        prediction = premium_projections[
            (premium_projections['zip_code'] == request.zip_code) &
            (premium_projections['year'] == request.year)
        ]
        
        if prediction.empty:
            raise HTTPException(
                status_code=404,
                detail=f"No premium data found for ZIP code {request.zip_code} in year {request.year}"
            )
        
        current_premium = float(prediction['current_premium'].iloc[0])
        predicted_premium = float(prediction['predicted_premium'].iloc[0])
        premium_increase_pct = float(prediction['premium_increase_pct'].iloc[0])
        
        return PremiumResponse(
            zip_code=request.zip_code,
            current_premium=current_premium,
            predicted_premium=predicted_premium,
            premium_increase_pct=premium_increase_pct,
            year=request.year
        )
        
    except Exception as e:
        logger.error(f"Error predicting premium: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Prediction error: {str(e)}")

@app.get("/data/climate-summary")
async def get_climate_summary():
    """
    Get climate change summary for Tampa Bay region
    """
    try:
        climate_data = noaa_processor.get_climate_summary()
        return climate_data
    except Exception as e:
        logger.error(f"Error getting climate summary: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data error: {str(e)}")

@app.get("/data/zip-codes")
async def get_available_zip_codes():
    """
    Get list of available ZIP codes with flood risk data
    """
    try:
        flood_risk_data = fema_processor.calculate_zip_flood_risk()
        zip_codes = flood_risk_data['zip_code'].unique().tolist()
        return {"zip_codes": zip_codes, "count": len(zip_codes)}
    except Exception as e:
        logger.error(f"Error getting ZIP codes: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Data error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)