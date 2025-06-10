"""
Tampa Bay Flood Risk and Insurance Premium Predictor Dashboard
Interactive Streamlit application for flood risk visualization and premium forecasting
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import folium
from streamlit_folium import st_folium
import sys
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from data.fema_data import FEMADataProcessor
from data.noaa_data import NOAADataProcessor
from data.insurance_data import NFIPDataProcessor

# Configure page
st.set_page_config(
    page_title="Tampa Bay Flood Risk Predictor",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
@st.cache_data
def load_data():
    """Load and cache all data sources"""
    try:
        # Initialize data processors
        fema_processor = FEMADataProcessor()
        noaa_processor = NOAADataProcessor()
        nfip_processor = NFIPDataProcessor()
        
        # Load FEMA flood risk data
        flood_risk_data = fema_processor.calculate_zip_flood_risk()
        
        # Load NOAA climate projections
        climate_data = noaa_processor.get_climate_summary()
        
        # Load NFIP insurance data
        insurance_stats = nfip_processor.calculate_zip_premium_stats()
        premium_projections = nfip_processor.predict_premium_changes(flood_risk_data)
        
        return {
            'flood_risk': flood_risk_data,
            'climate': climate_data,
            'insurance': insurance_stats,
            'premium_projections': premium_projections
        }
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

# Title and description
st.title("🌊 Tampa Bay Flood Risk & Insurance Premium Predictor")
st.markdown("**Predicting flood risk and insurance premium changes across Tampa Bay ZIP codes through 2035**")

# Sidebar controls
st.sidebar.header("Analysis Controls")

# Load data
with st.spinner("Loading flood risk and insurance data..."):
    data = load_data()

if data is None:
    st.error("Unable to load data. Please check data sources and API connectivity.")
    st.stop()

# Analysis year selector
current_year = 2024
analysis_year = st.sidebar.slider(
    "Select Analysis Year",
    min_value=current_year,
    max_value=2035,
    value=2030,
    step=1
)

# ZIP code filter
available_zips = sorted(data['flood_risk']['zip_code'].unique()) if not data['flood_risk'].empty else []
selected_zips = st.sidebar.multiselect(
    "Select ZIP Codes",
    options=available_zips,
    default=available_zips[:10] if len(available_zips) > 10 else available_zips
)

# Risk threshold
risk_threshold = st.sidebar.slider(
    "Flood Risk Threshold",
    min_value=0.0,
    max_value=1.0,
    value=0.3,
    step=0.1,
    help="Show only ZIP codes above this risk level"
)

# Main dashboard layout
col1, col2, col3, col4 = st.columns(4)

# Key metrics
if not data['flood_risk'].empty:
    with col1:
        high_risk_zips = len(data['flood_risk'][data['flood_risk']['flood_risk_score'] > 0.5])
        st.metric("High Risk ZIP Codes", high_risk_zips)
    
    with col2:
        avg_risk = data['flood_risk']['flood_risk_score'].mean()
        st.metric("Average Risk Score", f"{avg_risk:.2f}")

if 'climate' in data and data['climate'] and 'summary' in data['climate']:
    with col3:
        sea_level_rise = data['climate']['summary'].get('total_rise_by_2035_mm', 0)
        st.metric("Sea Level Rise by 2035", f"{sea_level_rise:.1f} mm")
    
    with col4:
        storm_freq = data['climate']['summary'].get('current_storm_surge_frequency', 0)
        st.metric("Storm Surge Frequency", f"{storm_freq:.1%}")

# Flood Risk Analysis
st.header("📊 Flood Risk Analysis")

if not data['flood_risk'].empty:
    col1, col2 = st.columns(2)
    
    with col1:
        # Risk distribution
        fig_risk_dist = px.histogram(
            data['flood_risk'],
            x='flood_risk_score',
            nbins=20,
            title='Distribution of Flood Risk Scores',
            labels={'flood_risk_score': 'Flood Risk Score', 'count': 'Number of ZIP Codes'}
        )
        st.plotly_chart(fig_risk_dist, use_container_width=True)
    
    with col2:
        # Top risk ZIP codes
        top_risk = data['flood_risk'].nlargest(10, 'flood_risk_score')
        fig_top_risk = px.bar(
            top_risk,
            x='zip_code',
            y='flood_risk_score',
            title='Top 10 Highest Risk ZIP Codes',
            labels={'flood_risk_score': 'Risk Score', 'zip_code': 'ZIP Code'}
        )
        st.plotly_chart(fig_top_risk, use_container_width=True)

# Climate Projections
st.header("🌡️ Climate Change Projections")

if 'climate' in data and data['climate']:
    col1, col2 = st.columns(2)
    
    if 'sea_level_projections' in data['climate']:
        with col1:
            sea_level_proj = data['climate']['sea_level_projections']
            fig_sea_level = px.line(
                sea_level_proj,
                x='year',
                y='sea_level_rise_mm',
                title='Sea Level Rise Projections',
                labels={'sea_level_rise_mm': 'Sea Level Rise (mm)', 'year': 'Year'}
            )
            # Add uncertainty bounds
            fig_sea_level.add_trace(
                go.Scatter(
                    x=sea_level_proj['year'],
                    y=sea_level_proj['upper_bound_mm'],
                    fill=None,
                    mode='lines',
                    line_color='rgba(0,100,80,0)',
                    showlegend=False
                )
            )
            fig_sea_level.add_trace(
                go.Scatter(
                    x=sea_level_proj['year'],
                    y=sea_level_proj['lower_bound_mm'],
                    fill='tonexty',
                    mode='lines',
                    line_color='rgba(0,100,80,0)',
                    name='Uncertainty Range'
                )
            )
            st.plotly_chart(fig_sea_level, use_container_width=True)
    
    if 'storm_surge_projections' in data['climate']:
        with col2:
            storm_proj = data['climate']['storm_surge_projections']
            fig_storm = px.line(
                storm_proj,
                x='year',
                y='storm_surge_frequency',
                title='Storm Surge Frequency Projections',
                labels={'storm_surge_frequency': 'Annual Probability', 'year': 'Year'}
            )
            st.plotly_chart(fig_storm, use_container_width=True)

# Insurance Premium Predictions
st.header("💰 Insurance Premium Predictions")

if not data['premium_projections'].empty:
    # Filter projections for selected year
    year_projections = data['premium_projections'][
        data['premium_projections']['year'] == analysis_year
    ]
    
    if not year_projections.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            # Premium increase by ZIP
            fig_premium_increase = px.bar(
                year_projections.nlargest(15, 'premium_increase_pct'),
                x='zip_code',
                y='premium_increase_pct',
                title=f'Predicted Premium Increases by {analysis_year}',
                labels={'premium_increase_pct': 'Premium Increase (%)', 'zip_code': 'ZIP Code'}
            )
            st.plotly_chart(fig_premium_increase, use_container_width=True)
        
        with col2:
            # Premium vs risk correlation
            fig_risk_premium = px.scatter(
                year_projections,
                x='flood_risk_score',
                y='predicted_premium',
                hover_data=['zip_code'],
                title='Flood Risk vs Predicted Premium',
                labels={'flood_risk_score': 'Flood Risk Score', 'predicted_premium': 'Predicted Premium ($)'}
            )
            st.plotly_chart(fig_risk_premium, use_container_width=True)

# Geographic Visualization
st.header("🗺️ Geographic Risk Map")

try:
    # Create folium map centered on Tampa Bay
    tampa_center = [27.9506, -82.4572]
    m = folium.Map(location=tampa_center, zoom_start=10)
    
    # Add flood risk data to map
    if not data['flood_risk'].empty:
        for _, row in data['flood_risk'].head(50).iterrows():  # Limit for performance
            # Use ZIP code centroid (approximate)
            # In production, you'd geocode ZIP codes properly
            lat = tampa_center[0] + (hash(str(row['zip_code'])) % 100 - 50) * 0.01
            lon = tampa_center[1] + (hash(str(row['zip_code'])) % 100 - 50) * 0.01
            
            risk_score = row['flood_risk_score']
            color = 'red' if risk_score > 0.7 else 'orange' if risk_score > 0.4 else 'green'
            
            folium.CircleMarker(
                location=[lat, lon],
                radius=risk_score * 20 + 5,
                popup=f"ZIP: {row['zip_code']}<br>Risk Score: {risk_score:.2f}",
                color=color,
                fillColor=color,
                fillOpacity=0.6
            ).add_to(m)
    
    # Display map
    st_folium(m, width=700, height=500)
    
except Exception as e:
    st.warning("Geographic visualization unavailable. Install folium and streamlit-folium for map display.")

# Detailed Data Tables
st.header("📋 Detailed Analysis")

tab1, tab2, tab3 = st.tabs(["Flood Risk Data", "Insurance Statistics", "Premium Projections"])

with tab1:
    st.subheader("Flood Risk by ZIP Code")
    if not data['flood_risk'].empty:
        filtered_risk = data['flood_risk'][data['flood_risk']['flood_risk_score'] > risk_threshold]
        st.dataframe(filtered_risk.sort_values('flood_risk_score', ascending=False))
    else:
        st.info("No flood risk data available")

with tab2:
    st.subheader("Insurance Statistics")
    if not data['insurance'].empty:
        st.dataframe(data['insurance'])
    else:
        st.info("No insurance data available")

with tab3:
    st.subheader("Premium Projections")
    if not data['premium_projections'].empty:
        proj_display = data['premium_projections'][
            (data['premium_projections']['year'] == analysis_year) &
            (data['premium_projections']['zip_code'].isin(selected_zips) if selected_zips else True)
        ]
        st.dataframe(proj_display.sort_values('premium_increase_pct', ascending=False))
    else:
        st.info("No premium projection data available")

# Data Sources and Methodology
st.header("📚 Data Sources & Methodology")

with st.expander("Data Sources"):
    st.markdown("""
    **FEMA National Flood Hazard Layer (NFHL)**
    - Official flood zone designations
    - Base flood elevations
    - Special flood hazard areas
    
    **NOAA Tides and Currents**
    - Historical sea level data
    - Tide gauge measurements
    - Sea level rise trends
    
    **NFIP Claims and Policy Data**
    - Historical insurance claims
    - Policy premiums and coverage
    - Loss ratios by geography
    
    **US Census Bureau**
    - ZIP code boundaries
    - Demographic characteristics
    - Housing data
    """)

with st.expander("Methodology"):
    st.markdown("""
    **Flood Risk Calculation**
    - Overlay FEMA flood zones with ZIP code boundaries
    - Calculate percentage of high-risk area per ZIP
    - Weight by flood zone severity (A, AE, VE zones)
    
    **Sea Level Rise Projections**
    - Linear trend analysis from NOAA tide gauge data
    - Regional climate model adjustments
    - Uncertainty bounds based on IPCC scenarios
    
    **Premium Predictions**
    - Risk-based pricing adjustments
    - Climate change multipliers (2-4% annually)
    - Market and regulatory factors
    """)

# Footer
st.markdown("---")
st.markdown("**Tampa Bay Flood Risk Predictor** | Data sources: FEMA, NOAA, NFIP")
st.caption("This tool is for research purposes. Consult insurance professionals for official premium quotes.")