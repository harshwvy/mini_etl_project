🌦️ Mizoram Weather & Flooding Data ETL Pipeline
A mini-production grade ETL pipeline using PySpark, designed to process weather and flood data for various places in Mizoram, India. This project simulates monthly data ingestion from multiple source tables, performs aggregations, runs data quality checks, and loads summarized insights into a target table for reporting and analytics.

📌 Project Objective
✅ Process monthly weather and flood readings from source tables.
✅ Perform data quality checks on incoming data.
✅ Calculate aggregated metrics (average temperature, total rainfall, etc.).
✅ Join aggregated data with master data for place details.
✅ Load final summarized data into a target table for each unique place ID along with month date and load status.
✅ Implement logging and configuration-driven design for production-readiness.

📊 Tech Stack
🐍 Python 3.x

⚡ PySpark (Spark SQL, DataFrames)

🐘 PostgreSQL (via pgAdmin for source/target tables)

📦 Config-driven pipeline (JSON config)

📑 Logging framework (Custom Python logger)

📂 Project Structure

etl_mini_project/
├── config/
│   └── etl_config.json             # Pipeline config file (table names, month date, load status etc.)
├── data/
│   └── sample CSVs (if any)
├── src/
│   ├── etl_job.py                  # Main ETL job script
│   ├── db_config.py                # Spark session & table read/write utilities
│   ├── data_quality.py             # Data quality check functions
│   └── logger.py                   # Logging setup
├── logs/
│   └── etl_job.log                 # ETL run logs
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation

📖 How the Pipeline Works
Configuration Load:
Reads pipeline parameters from etl_config.json.

Source Table Load:
Loads place_master, weather_readings, and flood_alerts tables from PostgreSQL using Spark JDBC.

Data Quality Checks:

Null checks for primary columns.

Minimum row count validation.

Uniqueness check for primary keys.

Data Transformation:

Filters records up to the configured month_date.

Aggregates:

average temperature

total rainfall

average humidity

max water level

flood risk level

Joins aggregated stats with place_master.

Final Data Load:

Writes summary records into the mizoram_weather_summary target table via JDBC.

Logging & Monitoring:
Logs every major operation for easy debugging and production tracking.

📈 Example Aggregations
Metric	Source Table	Calculation
Avg Temperature	weather_readings	AVG(temperature)
Total Rainfall	weather_readings	SUM(rainfall)
Avg Humidity	weather_readings	AVG(humidity)
Max Water Level	flood_alerts	MAX(water_level)
Max Flood Risk Level	flood_alerts	MAX(flood_risk_level)

📝 How to Run
Update PostgreSQL connection details and table names in config/etl_config.json.

Install dependencies:

pip install -r requirements.txt
Run the ETL Job:

python src/etl_job.py
📑 Sample Config — etl_config.json

{
  "log_path": "logs/etl_job.log",
  "month_date": "2025-06-01",
  "load_status_id": 1,
  "tables": {
    "place_master": "place_master",
    "weather_readings": "weather_readings",
    "flood_alerts": "flood_alerts",
    "target_summary": "mizoram_weather_summary"
  }
}
📌 Future Enhancements
Automate monthly data load scheduling via Airflow / AWS Glue

Add schema validation (column type checks)

Integrate Delta Lake / Hudi for incremental loads

Build dashboards in Tableau / Metabase for insights

📜 License
This project is for learning, demonstration, and interview preparation purposes. No production use without adaptation and testing.
