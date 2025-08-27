# Stock Data Pipeline Project

This project demonstrates a real-time stock data processing pipeline using Kafka, Spark Structured Streaming, SQL Server, and Streamlit dashboard.

---

## Setup Instructions

### 1. Clone the repository

git clone https://github.com/Akshitmewara/stock-data-pipeline
cd stock-data-pipeline


### 2. Create Python virtual environment and activate it

python3 -m venv venv
source venv/bin/activate  # on Windows use `venv\Scripts\activate`


### 3. Install required Python packages

pip install -r requirements.txt

---

## Running Each Component

### Kafka Producer

cd kafka
python kafka_producer.py

- Runs infinitely fetching stock data every minute and sends it to Kafka topic 'stock_topic'.
- Use `Ctrl+C` to stop.

---

### Spark Streaming Job

cd ../spark 
spark-submit 
–jars /path/to/mssql-jdbc.jar 
–packages org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0 
stream_process.py

- Reads Kafka topic, parses stock data, filters, and writes to SQL Server.
- Runs continuously until stopped.

---

### Streamlit Dashboard

streamlit run streamlit_app.py

- Visualizes live stock data from SQL Server.
- Auto-refreshes with sidebar-controlled interval.

---

## Notes

- Replace `/path/to/mssql-jdbc.jar` with your actual path to Microsoft JDBC driver.
- Ensure Kafka server, SQL Server instance, and all required dependencies are running and reachable.
- Update database credentials inside scripts accordingly.
- For production deployments, consider using environment variables or secrets management to store sensitive information securely.

---

Feel free to open issues or contribute to improve this project!



