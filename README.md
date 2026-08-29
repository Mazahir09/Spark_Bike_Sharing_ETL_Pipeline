# Spark_Bike_Sharing_ETL_Pipeline
# Hubway Bikes ETL with PySpark & MongoDB

An end-to-end **Data Engineering ETL project** using **Apache Spark (PySpark)** to process the Hubway Bike Sharing dataset and store the transformed analytical data in **MongoDB**.

This project demonstrates data ingestion, transformation, joining, enrichment, aggregation, and NoSQL data storage using PySpark.

---

## Project Overview

The objective of this project is to process Hubway Bike Sharing data using PySpark and prepare an aggregated dataset for analytical use.

The pipeline works with three datasets:

* Trip data
* Station data
* ZIP code mapping data

The data is loaded into Spark DataFrames, transformed and enriched through joins, aggregated according to the required business dimensions, and finally stored in MongoDB.

---

## ETL Architecture

```text
                 ┌─────────────────────────┐
                 │     Trip Data (CSV)     │
                 └────────────┬────────────┘
                              │
                              │
                 ┌────────────▼────────────┐
                 │   Station Data (CSV)    │
                 └────────────┬────────────┘
                              │
                              │
                 ┌────────────▼────────────┐
                 │ ZIP Code Mapping (CSV)  │
                 └────────────┬────────────┘
                              │
                              ▼
                    ┌──────────────────┐
                    │   PySpark Load   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │ Data Preparation │
                    │ & Column Cleanup │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   Data Joins     │
                    │                  │
                    │ Trips + Stations │
                    │ Trips + ZIP Map  │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │  Transformation  │
                    │                  │
                    │ State            │
                    │ Month_Year       │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │   Aggregation    │
                    │                  │
                    │ Number of Trips  │
                    │ Total Duration   │
                    └────────┬─────────┘
                             │
                             ▼
                    ┌──────────────────┐
                    │     MongoDB      │
                    │    Collection    │
                    └──────────────────┘
```

---

# Technologies Used

* Python
* Apache Spark
* PySpark
* MongoDB
* MongoDB Spark Connector
* Google Colab
* CSV
* NoSQL

---

# Dataset

The project uses the following publicly available datasets.

### Trips Dataset

```text
https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/bike-data/201508_trip_data.csv
```

### Stations Dataset

```text
https://raw.githubusercontent.com/databricks/Spark-The-Definitive-Guide/master/data/bike-data/201508_station_data.csv
```

### ZIP Code Mapping Dataset

```text
https://raw.githubusercontent.com/scpike/us-state-county-zip/master/geo-data.csv
```

---

# Project Structure

```text
Hubway-ETL/
│
├── Spark_ETL_Bike.ipynb
├── requirements.txt
└── README.md
```

The main implementation is contained in the Jupyter Notebook.

---

# 1. Environment Setup

The notebook configures Apache Spark and Java and installs the required PySpark components.

The MongoDB Spark Connector is configured through:

```text
org.mongodb.spark:mongo-spark-connector_2.12:10.3.0
```

The notebook is designed to run in a Google Colab environment.

---

# 2. Spark Initialization

A Spark session is created for the ETL process.

The Spark application is named:

```text
HubwayBikeETL
```

MongoDB read and write connectivity is configured through Spark's MongoDB connector.

For security, the MongoDB connection URI should be supplied privately and should never be committed to the repository.

Example:

```python
.config(
    "spark.mongodb.read.connection.uri",
    "<MONGODB-URI>"
)
```

The repository contains only the placeholder and does not contain MongoDB credentials.

---

# 3. Data Loading

The pipeline loads three datasets into Spark DataFrames.

### Trips DataFrame

The Trips dataset contains fields such as:

```text
Trip_ID
Duration
Start_Date
Start_Station
Start_Terminal
End_Date
End_Station
End_Terminal
Bike_#
Subscriber_Type
Zip_Code
```

### Stations DataFrame

The Stations dataset contains:

```text
station_id
name
lat
long
dockcount
landmark
installation
```

### ZIP Code Mapping

The ZIP-code dataset contains:

```text
state_fips
state
state_abbr
zipcode
county
city
```

---

# 4. Data Preparation

The original dataset contains column names with spaces.

A reusable PySpark function is used to replace spaces with underscores.

For example:

```text
Trip ID
```

becomes:

```text
Trip_ID
```

Similarly:

```text
Start Date
```

becomes:

```text
Start_Date
```

This makes the columns easier to reference during Spark transformations.

---

# 5. Data Transformation

## Starting Landmark

The Trips DataFrame is joined with the Stations DataFrame using the starting terminal.

This adds:

```text
Starting_Landmark
```

to the Trips DataFrame.

---

## Ending Landmark

The Stations DataFrame is joined again using the ending terminal.

This adds:

```text
Ending_Landmark
```

to the Trips DataFrame.

The Stations dataset is therefore used twice with different join conditions to enrich both the starting and ending locations.

---

## State Enrichment

The ZIP Code Mapping dataset is joined with the Trips DataFrame using:

```text
Zip_Code = zipcode
```

The resulting state information is added to the Trips data.

Where a ZIP code cannot be matched, the state is assigned:

```text
Unknown
```

---

## Month-Year Extraction

The `Start_Date` field is converted to a timestamp.

A new column is then generated:

```text
Month_Year
```

with the format:

```text
MM-yyyy
```

Examples:

```text
08-2015
07-2015
06-2015
```

---

# 6. Relevant Output Columns

After transformation, the dataset contains relevant trip, location, subscriber, geographic, and temporal information, including:

```text
Trip_ID
Duration
Start_Date
Start_Station
End_Date
End_Station
Bike_#
Subscriber_Type
Zip_Code
Starting_Landmark
Ending_Landmark
state
county
city
Month_Year
```

---

# 7. Data Aggregation

The final analytical dataset is created using a PySpark `groupBy`.

The aggregation dimensions are:

```text
Starting_Landmark
Ending_Landmark
State
Subscriber_Type
Month_Year
```

Two measures are calculated.

### Number of Trips

The number of unique trips is calculated using:

```python
countDistinct("Trip_ID")
```

The resulting column is:

```text
Number_of_Trips
```

### Total Duration

The total trip duration is calculated by summing the trip duration and converting the result from seconds to minutes.

The resulting column is:

```text
Duration_Total_Minutes
```

The duration is rounded to two decimal places.

---

# 8. Final Dataset

The final aggregated dataset has the following structure:

```text
Starting_Landmark
Ending_Landmark
State
Subscriber_Type
Month_Year
Number_of_Trips
Duration_Total_Minutes
```

Example:

```text
Starting_Landmark | Ending_Landmark | State      | Subscriber_Type | Month_Year | Number_of_Trips | Duration_Total_Minutes
San Jose           | San Jose        | Texas      | Customer        | 06-2015    | 3               | 114.87
Palo Alto          | Palo Alto       | Illinois   | Customer        | 05-2015    | 2               | 150.22
```

---

# 9. MongoDB Storage

The aggregated DataFrame is written to MongoDB using the MongoDB Spark Connector.

The write operation uses:

```text
format: mongodb
mode: overwrite
```

The database and collection names should be configured according to the user's MongoDB environment.

Example:

```python
agg_df.write.format("mongodb") \
    .option("database", "DBname") \
    .option("collection", "col_name") \
    .mode("overwrite") \
    .save()
```

The notebook then reads the MongoDB collection back into Spark to verify that the data was successfully stored.

---

# 10. MongoDB Verification

After writing the aggregated DataFrame to MongoDB, the collection is read back using the MongoDB Spark connector.

This provides a verification step to confirm that the transformed analytical dataset has been successfully persisted.

The retrieved data is displayed using:

```python
df_read.show(20)
```

---

# 11. Running the Notebook

## Google Colab

The notebook was designed to run in Google Colab.

Open the notebook in Google Colab and execute the cells sequentially.

The setup section installs:

* Java
* PySpark
* Findspark

and configures the Spark environment.

---

# 12. MongoDB Configuration

Before running the MongoDB read/write cells, replace the placeholder MongoDB URI:

```text
<MONGODB-URI>
```

with your own MongoDB connection string.

Do **not** commit your real MongoDB URI if it contains credentials.

For example, do not commit:

```text
mongodb+srv://username:password@cluster.mongodb.net/
```

Instead, keep the URI private or provide it through an environment variable/secret-management mechanism.

---

# 13. Security

This repository does not contain MongoDB credentials.

The MongoDB connection values in the notebook are represented using placeholders.

Do not upload:

```text
.env
MongoDB passwords
MongoDB connection strings containing credentials
Cloud credentials
Private keys
Service-account files
```

---

# 14. Project Requirements Coverage

| Requirement                    | Implementation |
| ------------------------------ | -------------- |
| Load Trips data                | ✅              |
| Load Stations data             | ✅              |
| Load ZIP code mapping          | ✅              |
| PySpark DataFrames             | ✅              |
| Correct column headers         | ✅              |
| Starting station join          | ✅              |
| Ending station join            | ✅              |
| ZIP code enrichment            | ✅              |
| State column                   | ✅              |
| Missing state handling         | ✅              |
| Month-Year extraction          | ✅              |
| Relevant column selection      | ✅              |
| Grouping                       | ✅              |
| Distinct trip count            | ✅              |
| Total duration calculation     | ✅              |
| MongoDB storage                | ✅              |
| MongoDB read-back verification | ✅              |

---

# 15. Key Data Engineering Concepts Demonstrated

This project demonstrates practical use of:

* Distributed data processing with PySpark
* Spark DataFrames
* Data ingestion from CSV sources
* Schema inspection
* Column standardization
* DataFrame joins
* Data enrichment
* Handling missing values
* Date/time transformation
* GroupBy operations
* Distinct aggregations
* Numerical aggregation
* MongoDB integration
* Spark MongoDB Connector
* NoSQL data storage
* ETL pipeline design

---

## Author

**Mazahir Hussain**

Data Engineering | Python | SQL | PySpark | Apache Spark | MongoDB | ETL
