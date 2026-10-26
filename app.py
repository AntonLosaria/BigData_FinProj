"""
Streaming Data Dashboard Template
STUDENT PROJECT: Big Data Streaming Dashboard

This is a template for students to build a real-time streaming data dashboard.
Students will need to implement the actual data processing, Kafka consumption,
and storage integration.

IMPLEMENT THE TODO SECTIONS
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import time
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from kafka.errors import KafkaError, NoBrokersAvailable
from streamlit_autorefresh import st_autorefresh

# Page configuration
st.set_page_config(
    page_title="Streaming Data Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

def setup_sidebar():
    """
    STUDENT TODO: Configure sidebar settings and controls
    Implement any configuration options students might need
    """
    st.sidebar.title("Dashboard Controls")
    
    # STUDENT TODO: Add configuration options for data sources
    st.sidebar.subheader("Data Source Configuration")
    
    # Placeholder for Kafka configuration
    kafka_broker = st.sidebar.text_input(
        "Kafka Broker", 
        value="localhost:9092",
        help="STUDENT TODO: Configure your Kafka broker address"
    )
    
    kafka_topic = st.sidebar.text_input(
        "Kafka Topic", 
        value="streaming-data",
        help="STUDENT TODO: Specify the Kafka topic to consume from"
    )
    
    # Placeholder for storage configuration
    st.sidebar.subheader("Storage Configuration")
    storage_type = st.sidebar.selectbox(
        "Storage Type",
        ["HDFS", "MongoDB"],
        help="STUDENT TODO: Choose your historical data storage solution"
    )
    
    return {
        "kafka_broker": kafka_broker,
        "kafka_topic": kafka_topic,
        "storage_type": storage_type
    }

def generate_sample_data():
    """
    STUDENT TODO: Replace this with actual data processing
    
    This function generates sample data for demonstration purposes.
    Students should replace this with real data from Kafka and storage systems.
    """
    # Sample data for demonstration - REPLACE WITH REAL DATA
    current_time = datetime.now()
    times = [current_time - timedelta(minutes=i) for i in range(100, 0, -1)]
    
    sample_data = pd.DataFrame({
        'timestamp': times,
        'value': [100 + i * 0.5 + (i % 10) for i in range(100)],
        'metric_type': ['temperature'] * 100,
        'sensor_id': ['sensor_1'] * 100
    })
    
    return sample_data

def consume_kafka_data(config):
    """
    STUDENT TODO: Implement actual Kafka consumer
    """
    kafka_broker = config.get("kafka_broker", "localhost:9092")
    kafka_topic = config.get("kafka_topic", "streaming-data")
    
    # Cache Kafka consumer to avoid recreation
    cache_key = f"kafka_consumer_{kafka_broker}_{kafka_topic}"
    if cache_key not in st.session_state:
        # Connection retry logic for Kafka consumer
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                st.session_state[cache_key] = KafkaConsumer(
                    kafka_topic,
                    bootstrap_servers=[kafka_broker],
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                    consumer_timeout_ms=5000
                )
                break  # Success, break out of retry loop
            except Exception as e:
                if attempt < max_retries - 1:
                    st.warning(f"Kafka connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    st.error(f"Failed to connect to Kafka after {max_retries} attempts: {e}")
                    st.session_state[cache_key] = None
    
    consumer = st.session_state[cache_key]
    
    if consumer:
        try:
            # Poll for messages
            messages = []
            start_time = time.time()
            poll_timeout = 5
            
            while time.time() - start_time < poll_timeout and len(messages) < 10:
                msg_pack = consumer.poll(timeout_ms=1000)
                
                for tp, messages_batch in msg_pack.items():
                    for message in messages_batch:
                        try:
                            data = message.value
                            if all(key in data for key in ['timestamp', 'value', 'metric_type', 'sensor_id']):
                                # Robust timestamp parsing for various ISO 8601 formats
                                timestamp_str = data['timestamp']
                                try:
                                    # Handle common ISO 8601 formats including Zulu time
                                    if timestamp_str.endswith('Z'):
                                        timestamp_str = timestamp_str[:-1] + '+00:00'
                                    # Parse the timestamp
                                    timestamp = datetime.fromisoformat(timestamp_str)
                                    messages.append({
                                        'timestamp': timestamp,
                                        'value': float(data['value']),
                                        'metric_type': data['metric_type'],
                                        'sensor_id': data['sensor_id']
                                    })
                                except ValueError as ve:
                                    st.warning(f"Invalid timestamp format '{timestamp_str}': {ve}")
                            else:
                                st.warning(f"Invalid message format: {data}")
                        except (ValueError, KeyError, TypeError) as e:
                            st.warning(f"Error processing message: {e}")
            
            if messages:
                return pd.DataFrame(messages)
            else:
                st.info("No messages received from Kafka. Using sample data.")
                return generate_sample_data()
                
        except (NoBrokersAvailable, KafkaError, Exception) as e:
            error_type = "Kafka broker unavailable" if isinstance(e, NoBrokersAvailable) else f"Kafka error: {e}" if isinstance(e, KafkaError) else f"Unexpected error: {e}"
            st.error(f"{error_type}. Using sample data.")
            return generate_sample_data()
    else:
        st.error("Unable to connect to Kafka. Using sample data.")
        return generate_sample_data()

def query_historical_data(time_range="1h", metrics=None):
    """
    STUDENT TODO: Implement actual historical data query
    
    This function should:
    1. Connect to HDFS/MongoDB
    2. Query historical data based on time range and selected metrics
    3. Return aggregated historical data
    
    Parameters:
    - time_range: time period to query (e.g., "1h", "24h", "7d")
    - metrics: list of metric types to include
    
    Expected return format:
    pandas DataFrame with historical data
    """
    # STUDENT TODO: Replace with actual storage query
    st.warning("STUDENT TODO: Implement historical data query in query_historical_data() function")
    
    # Return sample data for template demonstration
    return generate_sample_data()


def display_real_time_view(config, refresh_interval):
    """
    Page 1: Real-time Streaming View
    STUDENT TODO: Implement real-time data visualization from Kafka
    """
    st.header("📈 Real-time Streaming Dashboard")
    
    # Refresh status
    refresh_state = st.session_state.refresh_state
    st.info(f"**Auto-refresh:** {'🟢 Enabled' if refresh_state['auto_refresh'] else '🔴 Disabled'} - Updates every {refresh_interval} seconds")
    
    # Loading indicator for data consumption
    with st.spinner("Fetching real-time data from Kafka..."):
        real_time_data = consume_kafka_data(config)
    
    if real_time_data is not None:
        # Data freshness indicator
        data_freshness = datetime.now() - refresh_state['last_refresh']
        freshness_color = "🟢" if data_freshness.total_seconds() < 10 else "🟡" if data_freshness.total_seconds() < 30 else "🔴"
        
        st.success(f"{freshness_color} Data updated {data_freshness.total_seconds():.0f} seconds ago")
        
        # Real-time data metrics
        st.subheader("📊 Live Data Metrics")
        if not real_time_data.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Records Received", len(real_time_data))
            with col2:
                st.metric("Latest Value", f"{real_time_data['value'].iloc[-1]:.2f}")
            with col3:
                st.metric("Data Range", f"{real_time_data['timestamp'].min().strftime('%H:%M')} - {real_time_data['timestamp'].max().strftime('%H:%M')}")
        
        # Real-time chart
        st.subheader("📈 Real-time Trend")
        
        if not real_time_data.empty:
            # STUDENT TODO: Customize this chart for your specific data
            fig = px.line(
                real_time_data,
                x='timestamp',
                y='value',
                title=f"Real-time Data Stream (Last {len(real_time_data)} records)",
                labels={'value': 'Sensor Value', 'timestamp': 'Time'},
                template='plotly_white'
            )
            fig.update_layout(
                xaxis_title="Time",
                yaxis_title="Value",
                hovermode='x unified'
            )
            st.plotly_chart(fig, width='stretch')
            
            # Raw data table with auto-refresh
            with st.expander("📋 View Raw Data"):
                st.dataframe(
                    real_time_data.sort_values('timestamp', ascending=False),
                    width='stretch',
                    height=300
                )
        else:
            st.warning("No real-time data available. STUDENT TODO: Implement Kafka consumer.")
    
    else:
        st.error("STUDENT TODO: Kafka data consumption not implemented")

def display_historical_view(config):
    """
    STUDENT TODO: Implement historical data query and visualization
    """
    st.header("📊 Historical Data Analysis")
    
    with st.expander("ℹ️ Implementation Guide"):
        st.info("""
        **STUDENT TODO:** This page should display historical data queried from HDFS or MongoDB.
        Implement the following:
        - Connection to your chosen storage system (HDFS/MongoDB)
        - Interactive filters and selectors for data exploration
        - Data aggregation and analysis capabilities
        - Historical trend visualization
        """)
    
    # Interactive controls
    st.subheader("Data Filters")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        time_range = st.selectbox(
            "Time Range",
            ["1h", "24h", "7d", "30d"],
            help="STUDENT TODO: Implement time-based filtering in your query"
        )
    
    with col2:
        metric_type = st.selectbox(
            "Metric Type",
            ["temperature", "humidity", "pressure", "all"],
            help="STUDENT TODO: Implement metric filtering in your query"
        )
    
    with col3:
        aggregation = st.selectbox(
            "Aggregation",
            ["raw", "hourly", "daily", "weekly"],
            help="STUDENT TODO: Implement data aggregation in your query"
        )
    
    # STUDENT TODO: Replace with actual historical data query
    historical_data = query_historical_data(time_range, [metric_type] if metric_type != "all" else None)
    
    if historical_data is not None:
        # Display raw data
        st.subheader("Historical Data Table")
        st.info("STUDENT TODO: Customize data display for your specific dataset")
        
        st.dataframe(
            historical_data,
            width='stretch',
            hide_index=True
        )
        
        # Historical trends
        st.subheader("Historical Trends")
        st.info("STUDENT TODO: Implement meaningful historical analysis and visualization")
        
        if not historical_data.empty:
            # STUDENT TODO: Customize this analysis for your data
            fig = px.line(
                historical_data,
                x='timestamp',
                y='value',
                title="STUDENT TODO: Customize historical trend analysis"
            )
            st.plotly_chart(fig, width='stretch')
            
            # Additional analysis
            st.subheader("Data Summary")
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Records", len(historical_data))
                st.metric("Date Range", f"{historical_data['timestamp'].min().strftime('%Y-%m-%d')} to {historical_data['timestamp'].max().strftime('%Y-%m-%d')}")
            
            with col2:
                st.metric("Average Value", f"{historical_data['value'].mean():.2f}")
                st.metric("Data Variability", f"{historical_data['value'].std():.2f}")
    
    else:
        st.error("STUDENT TODO: Historical data query not implemented")

def main():
    """
    STUDENT TODO: Customize the main application flow as needed
    """
    st.title("🚀 Streaming Data Dashboard")
    
    with st.expander("📋 Project Instructions"):
        st.markdown("""
        **STUDENT PROJECT TEMPLATE**
        
        ### Implementation Required:
        - **Real-time Data**: Connect to Kafka and process streaming data
        - **Historical Data**: Query from HDFS/MongoDB
        - **Visualizations**: Create meaningful charts
        - **Error Handling**: Implement robust error handling
        """)
    
    # Initialize session state for refresh management
    if 'refresh_state' not in st.session_state:
        st.session_state.refresh_state = {
            'last_refresh': datetime.now(),
            'auto_refresh': True
        }
    
    # Setup configuration
    config = setup_sidebar()
    
    # Refresh controls in sidebar
    st.sidebar.subheader("Refresh Settings")
    st.session_state.refresh_state['auto_refresh'] = st.sidebar.checkbox(
        "Enable Auto Refresh",
        value=st.session_state.refresh_state['auto_refresh'],
        help="Automatically refresh real-time data"
    )
    
    if st.session_state.refresh_state['auto_refresh']:
        refresh_interval = st.sidebar.slider(
            "Refresh Interval (seconds)",
            min_value=5,
            max_value=60,
            value=15,
            help="Set how often real-time data refreshes"
        )
        
        # Auto-refresh using streamlit-autorefresh package
        st_autorefresh(interval=refresh_interval * 1000, key="auto_refresh")
    
    # Manual refresh button
    if st.sidebar.button("🔄 Manual Refresh"):
        st.session_state.refresh_state['last_refresh'] = datetime.now()
        st.rerun()
    
    # Display refresh status
    st.sidebar.markdown("---")
    st.sidebar.metric("Last Refresh", st.session_state.refresh_state['last_refresh'].strftime("%H:%M:%S"))
    
    # Create tabs for different views
    tab1, tab2 = st.tabs(["📈 Real-time Streaming", "📊 Historical Data"])
    
    with tab1:
        display_real_time_view(config, refresh_interval)
    
    with tab2:
        display_historical_view(config)
    

if __name__ == "__main__":
    main()