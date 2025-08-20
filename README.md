# 🚀 Data Jobs Intelligence Pipeline

> **Enterprise-grade automated job market analysis system with real-time scraping, cloud data warehousing, and intelligent reporting**

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.5.1-red.svg)](https://airflow.apache.org)
[![Snowflake](https://img.shields.io/badge/Snowflake-Cloud%20DW-blue.svg)](https://snowflake.com)
[![Streamlit](https://img.shields.io/badge/Streamlit-Dashboard-red.svg)](https://streamlit.io)

## 📊 Overview

An intelligent data pipeline that automatically scrapes job market data from multiple sources, processes it using advanced analytics, stores it in a cloud data warehouse, and delivers actionable insights through interactive dashboards and automated email reports.

## ✨ Key Features

### 🕷️ **Multi-Source Data Collection**
- **Glassdoor** - Primary job listings source
- **Indeed** - Secondary job aggregation  
- **LinkedIn** - Professional network jobs
- Smart anti-detection mechanisms
- Rate limiting and error handling

### 🧠 **Intelligent Processing**
- **Job Classification**: Full-time, Part-time, Contract, Internship
- **Work Mode Detection**: Remote, Hybrid, On-site
- **Experience Level Analysis**: Entry, Mid, Senior, Management
- **Geographic Intelligence**: City/State extraction
- **Company Analytics**: Hiring patterns and trends

### ❄️ **Cloud Data Warehouse**
- **Snowflake** integration for scalable storage
- Structured data modeling with fact/dimension tables
- Historical trend analysis capabilities
- Real-time data quality monitoring

### 📊 **Interactive Visualization**
- **Streamlit** dashboard with real-time filtering
- **Plotly** charts for market analysis
- Geographic distribution mapping
- Trend analysis and forecasting

### 📧 **Automated Intelligence Reports**
- Beautiful HTML email reports with embedded charts
- Executive-level metrics and insights
- Daily automated delivery
- Smart trend identification

## 🏗️ Architecture

```mermaid
graph LR
    A[Data Sources] --> B[Processing Layer]
    B --> C[Intelligence Engine]
    C --> D[Cloud Storage]
    D --> E[Visualization]
    
    A1[Glassdoor] --> B1[Selenium]
    A2[Indeed] --> B1
    A3[LinkedIn] --> B1
    
    B1 --> B2[Pandas]
    B2 --> C1[Classification]
    C1 --> C2[Analytics]
    C2 --> D1[Snowflake]
    D1 --> E1[Streamlit]
    D1 --> E2[Email Reports]
