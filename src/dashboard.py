import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import joblib
import numpy as np

st.set_page_config(page_title="ICU Forecasting", layout="wide")

st.title("🏥 ICU Patient Load Forecasting Dashboard")
st.markdown("---")

# Load data
@st.cache_data
def load_data():
    try:
        df = pd.read_csv('processed_icu_data.csv')
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df
    except:
        return None

# Load model
@st.cache_resource
def load_model():
    try:
        return joblib.load('icu_model.pkl')
    except:
        return None

df = load_data()
model = load_model()

if df is not None:
    # Display metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Current ICU Count", f"{df['icu_count'].iloc[-1]:.0f}")
    
    with col2:
        avg_count = df['icu_count'].mean()
        st.metric("Average ICU Count", f"{avg_count:.1f}")
    
    with col3:
        max_count = df['icu_count'].max()
        st.metric("Maximum ICU Count", f"{max_count:.0f}")
    
    with col4:
        min_count = df['icu_count'].min()
        st.metric("Minimum ICU Count", f"{min_count:.0f}")
    
    # Plot historical data
    st.subheader("📊 Historical ICU Occupancy")
    
    fig = px.line(df, x='datetime', y='icu_count', 
                  title='ICU Patient Count Over Time')
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Hourly pattern
    col1, col2 = st.columns(2)
    
    with col1:
        hourly_avg = df.groupby('hour')['icu_count'].mean()
        fig2 = px.bar(x=hourly_avg.index, y=hourly_avg.values,
                     title='Average ICU Count by Hour of Day',
                     labels={'x': 'Hour', 'y': 'Average Count'})
        st.plotly_chart(fig2, use_container_width=True)
    
    with col2:
        daily_avg = df.groupby('day_of_week')['icu_count'].mean()
        days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
        fig3 = px.bar(x=days, y=daily_avg.values,
                     title='Average ICU Count by Day of Week',
                     labels={'x': 'Day', 'y': 'Average Count'})
        st.plotly_chart(fig3, use_container_width=True)
    
    # Predictions
    if model is not None:
        st.subheader("🔮 Predictions")
        
        # Prepare last data point for prediction
        last_row = df.iloc[-1]
        features = [[
            last_row['hour'],
            last_row['day_of_week'],
            last_row['icu_count'],
            df.iloc[-2]['icu_count'] if len(df) > 1 else last_row['icu_count'],
            df.iloc[-3]['icu_count'] if len(df) > 2 else last_row['icu_count']
        ]]
        
        prediction = model.predict(features)[0]
        
        col1, col2 = st.columns(2)
        with col1:
            st.info(f"🎯 Predicted next ICU count: {prediction:.0f}")
        with col2:
            change = prediction - last_row['icu_count']
            if change > 0:
                st.warning(f"📈 Expected change: +{change:.0f}")
            else:
                st.success(f"📉 Expected change: {change:.0f}")
    
else:
    st.error("No data found. Please run data_loader.py first!")

st.markdown("---")
st.caption("ICU Load Forecasting System | Using MIMIC-IV Dataset")