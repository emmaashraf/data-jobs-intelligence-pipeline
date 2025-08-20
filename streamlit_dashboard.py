"""
Data Jobs Intelligence Dashboard
Real-time visualization of job market data
Author: Eman Elgamal
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import glob
import os
from datetime import datetime, timedelta
import numpy as np
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Page configuration
st.set_page_config(
    page_title="Data Jobs Intelligence Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .sidebar .sidebar-content {
        background-color: #262730;
    }
</style>
""", unsafe_allow_html=True)

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_job_data():
    """Load job data from CSV files"""
    
    # Get all CSV files from data directory
    csv_files = glob.glob("data/production_jobs_*.csv") + glob.glob("data/enhanced_jobs_*.csv")
    
    if not csv_files:
        # Generate sample data if no files exist
        return generate_sample_data()
    
    # Load the most recent file
    latest_file = max(csv_files, key=os.path.getctime)
    
    try:
        df = pd.read_csv(latest_file)
        
        # Data cleaning and preparation
        df['scraped_timestamp'] = pd.to_datetime(df['scraped_timestamp'])
        df['posted_date'] = pd.to_datetime(df['posted_date'])
        
        # Clean category names
        df['category_display'] = df['category'].str.replace('_', ' ').str.title()
        
        # Extract city and state from location
        df['city'] = df['location'].apply(extract_city)
        df['state'] = df['location'].apply(extract_state)
        
        return df
        
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return generate_sample_data()

def extract_city(location):
    """Extract city from location string"""
    if pd.isna(location) or location == "N/A":
        return "Unknown"
    
    parts = location.split(',')
    return parts[0].strip() if parts else "Unknown"

def extract_state(location):
    """Extract state from location string"""
    if pd.isna(location) or location == "N/A":
        return "Unknown"
    
    parts = location.split(',')
    if len(parts) >= 2:
        return parts[1].strip()
    return "Unknown"

def generate_sample_data():
    """Generate sample data for demonstration"""
    
    sample_data = {
        'job_title': [
            'Senior Data Engineer', 'Data Scientist - ML', 'Business Intelligence Analyst',
            'Data Analyst', 'Machine Learning Engineer', 'Data Warehouse Architect',
            'Junior Data Scientist', 'Lead Data Engineer', 'Analytics Manager',
            'Data Platform Engineer', 'Research Scientist', 'BI Developer',
            'Senior Data Analyst', 'ML Research Engineer', 'Data Infrastructure Engineer'
        ] * 4,  # 60 jobs total
        'company': [
            'Tech Innovations Inc', 'AI Solutions Corp', 'DataFlow Analytics',
            'Enterprise Data Systems', 'StartUp Analytics', 'Cloud Data Co',
            'Analytics First', 'Data Driven LLC', 'Intelligence Systems',
            'Big Data Corp', 'Smart Analytics', 'Data Insights Inc',
            'Advanced Analytics', 'Data Science Hub', 'Analytics Pro'
        ] * 4,
        'location': [
            'New York, NY', 'San Francisco, CA', 'Chicago, IL', 'Austin, TX',
            'Boston, MA', 'Seattle, WA', 'Denver, CO', 'Atlanta, GA',
            'Los Angeles, CA', 'Washington, DC', 'Miami, FL', 'Dallas, TX',
            'Portland, OR', 'Minneapolis, MN', 'Phoenix, AZ'
        ] * 4,
        'category': [
            'data_engineer', 'data_scientist', 'data_analyst', 'data_warehouse',
            'machine_learning', 'bi_developer'
        ] * 10,
        'job_type': ['Full-time'] * 50 + ['Contract'] * 5 + ['Part-time'] * 5,
        'work_mode': ['Remote'] * 25 + ['Hybrid'] * 20 + ['On-site'] * 15,
        'experience_level': ['Senior Level'] * 25 + ['Mid Level'] * 20 + ['Entry Level'] * 15,
        'source': ['glassdoor'] * 30 + ['indeed'] * 20 + ['linkedin'] * 10,
        'scraped_timestamp': [datetime.now() - timedelta(hours=i) for i in range(60)],
        'posted_date': [datetime.now().date() - timedelta(days=i%7) for i in range(60)]
    }
    
    df = pd.DataFrame(sample_data)
    df['category_display'] = df['category'].str.replace('_', ' ').str.title()
    df['city'] = df['location'].apply(extract_city)
    df['state'] = df['location'].apply(extract_state)
    
    return df

def create_metrics_cards(df):
    """Create metric cards for key statistics"""
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="📊 Total Jobs",
            value=len(df),
            delta=f"+{len(df[df['scraped_timestamp'] > datetime.now() - timedelta(days=1)])}"
        )
    
    with col2:
        st.metric(
            label="🏢 Companies",
            value=df['company'].nunique(),
            delta=f"{df[df['company'] != 'N/A']['company'].nunique()} unique"
        )
    
    with col3:
        remote_jobs = len(df[df['work_mode'] == 'Remote'])
        remote_pct = (remote_jobs / len(df) * 100) if len(df) > 0 else 0
        st.metric(
            label="🏠 Remote Jobs",
            value=f"{remote_pct:.1f}%",
            delta=f"{remote_jobs} positions"
        )
    
    with col4:
        senior_jobs = len(df[df['experience_level'] == 'Senior Level'])
        st.metric(
            label="⭐ Senior Positions",
            value=senior_jobs,
            delta=f"{senior_jobs/len(df)*100:.1f}% of total"
        )

def create_category_chart(df):
    """Create jobs by category chart"""
    
    category_counts = df.groupby('category_display').size().reset_index(name='count')
    
    fig = px.bar(
        category_counts,
        x='category_display',
        y='count',
        title="📊 Jobs by Category",
        color='count',
        color_continuous_scale='viridis'
    )
    
    fig.update_layout(
        xaxis_title="Job Category",
        yaxis_title="Number of Jobs",
        showlegend=False
    )
    
    return fig

def create_work_mode_chart(df):
    """Create work mode distribution chart"""
    
    work_mode_counts = df['work_mode'].value_counts()
    
    fig = px.pie(
        values=work_mode_counts.values,
        names=work_mode_counts.index,
        title="🏠 Work Mode Distribution",
        color_discrete_sequence=px.colors.qualitative.Set3
    )
    
    fig.update_traces(textposition='inside', textinfo='percent+label')
    
    return fig

def create_experience_level_chart(df):
    """Create experience level distribution chart"""
    
    exp_counts = df['experience_level'].value_counts()
    
    fig = go.Figure(data=[
        go.Bar(
            x=exp_counts.index,
            y=exp_counts.values,
            marker_color=['#ff9999', '#66b3ff', '#99ff99', '#ffcc99']
        )
    ])
    
    fig.update_layout(
        title="👥 Experience Level Distribution",
        xaxis_title="Experience Level",
        yaxis_title="Number of Jobs"
    )
    
    return fig

def create_location_map(df):
    """Create geographical distribution map"""
    
    # State coordinates (simplified)
    state_coords = {
        'NY': {'lat': 40.7128, 'lon': -74.0060, 'name': 'New York'},
        'CA': {'lat': 36.7783, 'lon': -119.4179, 'name': 'California'},
        'IL': {'lat': 40.6331, 'lon': -89.3985, 'name': 'Illinois'},
        'TX': {'lat': 31.9686, 'lon': -99.9018, 'name': 'Texas'},
        'MA': {'lat': 42.4072, 'lon': -71.3824, 'name': 'Massachusetts'},
        'WA': {'lat': 47.7511, 'lon': -120.7401, 'name': 'Washington'},
        'CO': {'lat': 39.5501, 'lon': -105.7821, 'name': 'Colorado'},
        'GA': {'lat': 32.1656, 'lon': -82.9001, 'name': 'Georgia'},
        'FL': {'lat': 27.6648, 'lon': -81.5158, 'name': 'Florida'},
        'DC': {'lat': 38.9072, 'lon': -77.0369, 'name': 'Washington DC'}
    }
    
    state_counts = df.groupby('state').size().reset_index(name='job_count')
    
    # Add coordinates
    state_counts['lat'] = state_counts['state'].map(lambda x: state_coords.get(x, {}).get('lat', 39.8283))
    state_counts['lon'] = state_counts['state'].map(lambda x: state_coords.get(x, {}).get('lon', -98.5795))
    state_counts['state_name'] = state_counts['state'].map(lambda x: state_coords.get(x, {}).get('name', x))
    
    fig = px.scatter_mapbox(
        state_counts,
        lat='lat',
        lon='lon',
        size='job_count',
        hover_name='state_name',
        hover_data={'job_count': True, 'lat': False, 'lon': False},
        color='job_count',
        size_max=50,
        zoom=3,
        title="🗺️ Geographic Distribution of Jobs"
    )
    
    fig.update_layout(
        mapbox_style="open-street-map",
        height=400
    )
    
    return fig

def create_trend_chart(df):
    """Create time trend chart"""
    
    df['date'] = df['scraped_timestamp'].dt.date
    daily_counts = df.groupby('date').size().reset_index(name='jobs_count')
    
    fig = px.line(
        daily_counts,
        x='date',
        y='jobs_count',
        title="📈 Daily Job Posting Trends",
        markers=True
    )
    
    fig.update_layout(
        xaxis_title="Date",
        yaxis_title="Number of Jobs"
    )
    
    return fig

def create_top_companies_chart(df):
    """Create top companies chart"""
    
    company_counts = df[df['company'] != 'N/A']['company'].value_counts().head(10)
    
    if len(company_counts) > 0:
        fig = px.bar(
            x=company_counts.values,
            y=company_counts.index,
            orientation='h',
            title="🏢 Top Hiring Companies",
            color=company_counts.values,
            color_continuous_scale='blues'
        )
        
        fig.update_layout(
            xaxis_title="Number of Job Postings",
            yaxis_title="Company",
            showlegend=False
        )
        
        return fig
    
    return None

def main():
    """Main dashboard function"""
    
    # Header
    st.markdown('<h1 class="main-header">📊 Data Jobs Intelligence Dashboard</h1>', unsafe_allow_html=True)
    st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Load data
    with st.spinner("Loading job market data..."):
        df = load_job_data()
    
    # Sidebar filters
    st.sidebar.header("🔍 Filters")
    
    # Category filter
    categories = st.sidebar.multiselect(
        "Select Job Categories:",
        options=df['category_display'].unique(),
        default=df['category_display'].unique()
    )
    
    # Work mode filter
    work_modes = st.sidebar.multiselect(
        "Select Work Modes:",
        options=df['work_mode'].unique(),
        default=df['work_mode'].unique()
    )
    
    # Experience level filter
    exp_levels = st.sidebar.multiselect(
        "Select Experience Levels:",
        options=df['experience_level'].unique(),
        default=df['experience_level'].unique()
    )
    
    # Date range filter
    min_date = pd.to_datetime(df['posted_date']).dt.date.min()
    max_date = pd.to_datetime(df['posted_date']).dt.date.max()
    
    date_range = st.sidebar.date_input(
        "Select Date Range:",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Convert posted_date to date for comparison
    df['posted_date_only'] = pd.to_datetime(df['posted_date']).dt.date
    
    # Apply filters
    filtered_df = df[
        (df['category_display'].isin(categories)) &
        (df['work_mode'].isin(work_modes)) &
        (df['experience_level'].isin(exp_levels)) &
        (df['posted_date_only'] >= date_range[0]) &
        (df['posted_date_only'] <= date_range[1])
    ]
    
    # Metrics cards
    st.subheader("📈 Key Metrics")
    create_metrics_cards(filtered_df)
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_category_chart(filtered_df), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_work_mode_chart(filtered_df), use_container_width=True)
    
    col3, col4 = st.columns(2)
    
    with col3:
        st.plotly_chart(create_experience_level_chart(filtered_df), use_container_width=True)
    
    with col4:
        top_companies_fig = create_top_companies_chart(filtered_df)
        if top_companies_fig:
            st.plotly_chart(top_companies_fig, use_container_width=True)
        else:
            st.info("No company data available")
    
    # Full-width charts
    st.plotly_chart(create_location_map(filtered_df), use_container_width=True)
    st.plotly_chart(create_trend_chart(filtered_df), use_container_width=True)
    
    # Data table
    st.subheader("📋 Job Listings")
    
    # Display options
    col1, col2, col3 = st.columns(3)
    with col1:
        show_table = st.checkbox("Show detailed table", value=False)
    with col2:
        rows_to_show = st.selectbox("Rows to display:", [10, 25, 50, 100], index=1)
    with col3:
        download_data = st.download_button(
            label="📥 Download CSV",
            data=filtered_df.to_csv(index=False),
            file_name=f"jobs_data_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv"
        )
    
    if show_table:
        st.dataframe(
            filtered_df[['job_title', 'company', 'location', 'category_display', 
                        'job_type', 'work_mode', 'experience_level', 'posted_date']].head(rows_to_show),
            use_container_width=True
        )
    
    # Footer
    st.markdown("---")
    st.markdown(
        "**Data Jobs Intelligence Dashboard** | "
        f"Built with ❤️ by Eman Elgamal | "
        f"Data as of {datetime.now().strftime('%Y-%m-%d')}"
    )

if __name__ == "__main__":
    main()