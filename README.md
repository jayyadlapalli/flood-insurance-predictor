# Tampa Bay Flood Risk + Insurance Premium Predictor

A comprehensive geospatial data science system to predict flood risk and insurance premium changes across Tampa Bay ZIP codes over the next 10 years, considering sea level rise, storm surge frequency, and zoning changes.

## Overview

This project combines FEMA flood hazard maps, NOAA sea level data, and NFIP insurance records to forecast how climate change will impact flood insurance costs across the Tampa Bay region through 2035.

## Key Features

- **Flood Risk Forecasting**: Time-series models predicting flood risk changes by ZIP code
- **Insurance Premium Prediction**: ML models forecasting NFIP premium increases
- **Geospatial Analysis**: Integration of FEMA flood zones with demographic data
- **Interactive Dashboard**: Real-time predictions and visualizations
- **API Service**: RESTful API for flood risk and premium queries

## Data Sources

| Source | Data Type | Description |
|--------|-----------|-------------|
| FEMA NFHL | Flood hazard maps | Zone-level flood risk by ZIP/census block |
| NOAA | Sea level trends | Historical and projected sea level data |
| NFIP | Insurance data | Premiums, claims, and payouts by ZIP |
| US Census | Demographics | Population and housing characteristics |
| Zillow/ACS | Property values | Real estate market data |

## Project Structure

```
flood-risk-predictor/
├── data/
│   ├── raw/                  # FEMA maps, NOAA data, insurance tables
│   ├── processed/            # Cleaned and engineered datasets
│   └── external/             # API downloads and cache
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   ├── 02_flood_risk_modeling.ipynb
│   ├── 03_insurance_premium_modeling.ipynb
│   └── 04_geospatial_analysis.ipynb
├── src/
│   ├── data/
│   │   ├── fema_data.py      # FEMA flood map processing
│   │   ├── noaa_data.py      # Sea level and climate data
│   │   └── insurance_data.py # NFIP claims and premium data
│   ├── models/
│   │   ├── flood_risk_model.py
│   │   └── premium_model.py
│   ├── visualization/
│   │   └── dashboard.py
│   └── api/
│       └── prediction_service.py
├── config/
│   └── data_sources.yaml
├── requirements.txt
├── Dockerfile
└── docker-compose.yml
```

## Models

### 1. Flood Risk Forecasting
- **Input Features**: Sea level rise, storm surge frequency, elevation, coastal proximity
- **Model Types**: Prophet, LSTM, or ARIMA for time-series forecasting
- **Output**: Flood risk scores by ZIP code (2025-2035)

### 2. Insurance Premium Prediction
- **Input Features**: Flood risk score, historical claims, property values, structure type
- **Model Types**: XGBoost, Random Forest Regressor
- **Output**: Predicted NFIP premiums by ZIP/year

## Key Features Engineered

| Feature | Description |
|---------|-------------|
| `flood_zone_pct` | Percentage of ZIP area in FEMA flood zones |
| `sea_level_trend_mm_per_year` | NOAA sea level rise trend |
| `avg_insurance_premium` | Historical mean premium per ZIP |
| `claims_per_capita` | Historical flood claims per capita |
| `elevation_m` | Mean elevation above sea level |
| `coastal_distance_km` | Distance to nearest coastline |
| `property_value_index` | Normalized home values |

## Installation

```bash
# Clone repository
git clone https://github.com/yourusername/flood-risk-predictor.git
cd flood-risk-predictor

# Install dependencies
pip install -r requirements.txt

# Set up environment variables
cp .env.example .env
# Add your API keys for NOAA, FEMA, etc.
```

## Usage

### Dashboard
```bash
streamlit run src/visualization/dashboard.py
```

### API Service
```bash
uvicorn src.api.prediction_service:app --reload
```

### Example API Call
```python
import requests

response = requests.post("http://localhost:8000/predict", json={
    "zip_code": "33602",
    "property_type": "single_family",
    "year": 2030
})

print(response.json())
```

## Deliverables

1. **Interactive Dashboard**: ZIP code-level flood risk and premium predictions
2. **Heatmaps**: Visual representation of risk changes over time
3. **Forecast Charts**: Premium trends by ZIP (2025-2035)
4. **API Service**: Real-time predictions for any Tampa Bay ZIP
5. **Model Documentation**: Interpretability analysis and model cards

## Technology Stack

- **Data Processing**: pandas, geopandas, dask
- **Machine Learning**: scikit-learn, XGBoost, Prophet
- **Geospatial**: rasterio, folium, shapely
- **Visualization**: plotly, streamlit, matplotlib
- **API**: FastAPI, uvicorn
- **Deployment**: Docker, docker-compose

## Development Status

- [x] Project structure and documentation
- [ ] Data collection and preprocessing
- [ ] Exploratory data analysis
- [ ] Flood risk modeling
- [ ] Insurance premium modeling
- [ ] Dashboard development
- [ ] API implementation
- [ ] Model validation and testing
- [ ] Deployment and documentation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests and documentation
5. Submit a pull request

## License

MIT License - see LICENSE file for details

## Disclaimer

This project is for research and educational purposes. Predictions should not be used as the sole basis for insurance or real estate decisions.