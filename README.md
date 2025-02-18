# 🏎️ F1 Data Pipeline 🏁

## 🚀 Overview
The F1 Data Pipeline project aims to collect, process, and analyze Formula 1 data using PySpark. The data is sourced from an API, transformed, and then uploaded to Google Cloud Storage before being loaded into BigQuery for further analysis.

## 📂 Project Structure
```
F1-Data-Pipeline/
│── ingestion/                 # 📥 Data ingestion scripts
│── load/                      # ☁️ Data loading scripts
│── transform/                 # 🔄 Data transformation scripts
│── service-account-file.json  # 🔑 Google Cloud authentication file
│── requirements.txt           # 📜 Dependencies
│── README.md                  # 📖 Project documentation
```

## ⚙️ Setup
✅ Prerequisites

1. Install Python 3.11+
2. Install required dependencies:

    ```
    pip install -r requirements.txt
    ```
3. Set up Spark locally (refer to links below):
    - [Step-by-Step Guide to Install PySpark on Windows](https://www.linkedin.com/pulse/step-by-step-guide-install-pyspark-windows-pc-2024-manav-nayak-wmpbf/?trackingId=lqL38tfLSHu0hGTioW9w5g%3D%3D)
    - [Getting Started with PySpark](https://medium.com/@dipan.saha/getting-started-with-pyspark-day-1-37e5e6fdc14b)
4. Set up Google Cloud credentials for authentication and place the ```service-account-file.json``` in project directory.
5. Download and configure the Google Cloud Storage connector for Spark:
    - Download the GCS connector JAR file.
    - Place the JAR file in the Spark jars directory.
    - Configure Spark to use the connector by adding the following to your Spark session.
    
        ```
        from pyspark.sql import SparkSession
        spark = SparkSession.builder \
        .appName("F1 Data Pipeline") \
        .config("spark.jars", "/path/to/gcs-connector-hadoop3-latest.jar") \
        .getOrCreate()
        ```
* For step 4 and 5 can refer to this [link](https://kashif-sohail.medium.com/read-files-from-google-cloud-storage-bucket-using-local-pyspark-and-jupyter-notebooks-f8bd43f4b42e)

## 📥 Data Ingestion
The ingestion process retrieves data from an API and directly stores it in Google Cloud Storage as JSON files.

```ingestion/```: Fetches data (drivers, constructors, races, circuits, results) and stores it in GCS.

## 🔄 Data Transformation
The processing step involves cleaning and transforming the raw JSON data using PySpark.

```transform/```: Reads raw JSON files from GCS, applies transformations, and prepares the data for storage.

## ☁️ Data Loading
Once the data is processed, it is uploaded to Google Cloud Storage and later loaded into BigQuery.

```load/```: Reads transformed data from GCS and loads it into BigQuery.

## 🎯 Conclusion
This project successfully automates the ingestion, transformation, and loading of F1 data into BigQuery using PySpark.