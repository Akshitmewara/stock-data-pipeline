import streamlit as st
import pandas as pd
import pyodbc
import plotly.express as px
from streamlit_autorefresh import st_autorefresh

# SQL Server connection parameters
server = 'localhost'
database = 'msdb'
username = 'sa'
password = 'NewStrongPassword123!'
driver = '/opt/homebrew/lib/libmsodbcsql.17.dylib'

def get_stock_data():
    conn_str = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    conn = pyodbc.connect(conn_str)
    query = """
    SELECT TOP 1000 [timestamp], [open], [high], [low], [close], [volume]
    FROM dbo.stock_prices
    ORDER BY [timestamp] DESC
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

st.set_page_config(page_title="Stock Dashboard", layout="wide")

st.title("Real-Time Stock Data Dashboard")

# Sidebar slider for refresh interval in seconds
refresh_rate = st.sidebar.slider("Refresh rate (seconds)", 5, 60, 30)

# Auto-refresh Streamlit app every refresh_rate seconds
st_autorefresh(interval=refresh_rate * 1000, key="dashboard_refresh")

df = get_stock_data()
df['close'] = pd.to_numeric(df['close'], errors='coerce')
df['low'] = pd.to_numeric(df['low'], errors='coerce')
df['high'] = pd.to_numeric(df['high'], errors='coerce')
df['open'] = pd.to_numeric(df['open'], errors='coerce')
df['volume'] = pd.to_numeric(df['volume'], errors='coerce')
df = df.dropna(subset=['volume'])
df = df.sort_values('timestamp')

df_distinct = df.loc[df.groupby('timestamp')['volume'].idxmax()]


if df_distinct.empty:
    st.warning("No stock data available to display yet.")
else:

    col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
    with col_kpi1:
        st.metric("Latest Close", f"{df_distinct['close'].iloc[-1]:.2f}")
    with col_kpi2:
        st.metric("Max Volume", int(df_distinct['volume'].max()))
    with col_kpi3:
        st.metric("Min Price", f"{df_distinct['low'].min():.2f}")




    # Row 1: Close Price and Pie Chart side by side with balanced widths
    col1, col2 = st.columns([3, 2])

    with col1:
        st.subheader("Stock Close Price Over Time")
        fig_close = px.line(df_distinct, x='timestamp', y='close')
        st.plotly_chart(fig_close, use_container_width=True)

    with col2:
        st.subheader("Top 10 Volume Distribution")
        top_volume = df_distinct.nlargest(10, 'volume')
        fig_pie = px.pie(top_volume, values='volume', names='timestamp')
        st.plotly_chart(fig_pie, use_container_width=True)

    # Spacer for visual separation
    st.markdown("---")

    # Row 2: Top 5 Max Volume Table centered within wider column
    st.subheader("Top 5 Max Volume Records")
    st.dataframe(df_distinct.nlargest(5, 'volume'), use_container_width=True)

