---

# Data Analysis with PySpark

## Overview

This project involves data processing and analysis using PySpark. The goal is to load, clean, and analyze data from a dat file to extract meaningful insights. The dataset contains information on households, including identifiers, interview details, and various demographic and socio-economic attributes.

## Prerequisites

- **Apache Spark**: PySpark is part of the Apache Spark project.
- **Python**: Python along with the PySpark library.

## Code Description

### Imports and Setup

```python
import pyspark
from pyspark.sql import SparkSession
from itertools import chain
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import pyspark.sql.functions as F
from pyspark.sql.functions import col, expr, substring, create_map, lit, when
```

- **`pyspark`**: The core library for working with Spark in Python.
- **`SparkSession`**: The entry point for working with Spark.
- **`StructType`, `StructField`**: Define the schema of the DataFrame.
- **`pyspark.sql.functions`**: Functions for DataFrame operations.

### Spark Session Initialization

```python
spark = SparkSession.builder.appName("DataAnalysisAssessment").getOrCreate()
```

Initializes a Spark session named "DataAnalysisAssessment".

### Schema Definition

```python
schema = StructType([
    StructField("HRHHID", StringType(), nullable=True, metadata={"description": "Household Identifier (Part 1)"}),
    ...
])
```
But we not used.

Defines the schema for the DataFrame, specifying column names, types, and descriptions.

### Data Loading and Preprocessing

#### Function: `load_data`

```python
def load_data(data_path: str = data_path) -> pyspark.sql.DataFrame:
    """
    Load data from a CSV file, extract relevant columns, and preprocess.
    ...
    """
    df = spark.read.format("csv").load(data_path)
    df = df.withColumn("HRHHID", substring("_c0", 1, 15))
    ...
    df = df.drop("_c0")
    return strim_cols(df)
```

- **`data_path`**: Path to the CSV file.
- **Operation**: Extracts and renames columns from raw data. The `strim_cols` function is applied to remove whitespace from string columns.

#### Function: `strim_cols`

```python
def strim_cols(df: pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    """
    Trim leading and trailing whitespace from all string columns in the DataFrame.
    ...
    """
    for column in df.columns:
        df = df.withColumn(column, F.trim(column))
    return df
```

- **Operation**: Trims leading and trailing whitespace from all string columns.

### Mapping Functions

#### Function: `Mapper`

```python
def Mapper(df: pyspark.sql.DataFrame, column_name: str, dict_mapper: dict):
    """
    Map values in a DataFrame column based on a dictionary and replace unmatched values.
    ...
    """
    cols = df.columns
    mapping_df = spark.createDataFrame(dict_mapper.items(), ["code", "value"])
    df2 = df.join(mapping_df, df[column_name] == mapping_df["code"], "left")
    df2 = df2.withColumn(column_name, when(col("value").isNull(), 'Others').otherwise(col("value")))
    df2 = df2.select(cols)
    return df2
```

- **Operation**: Maps values in a DataFrame column based on a provided dictionary and replaces unmatched values with 'Others'.

### Tasks and Operations

 **Load Data**:

    ```python
    df = load_data(data_path)
    ```

1. **Income Mapping and Analysis**:
    - **Operation**: Maps income ranges using `income_range_mapping`, groups by income range, counts occurrences, and displays results.

    ```python
    column_name = 'HEFAMINC'
    income_mapped = Mapper(df, column_name, income_range_mapping)
    income_counts = income_mapped.groupBy(column_name).count().orderBy('count', ascending=False)
    income_counts = income_counts.withColumnRenamed('coumt', 'IncomeCount')
    income_counts.show(truncate=False)
    ```

Result
| Household Income Range | Count  |
|------------------------|--------|
| Others                 | 250,020|
| $100,000 - $149,999    | 214,328|
| $75,000 - $99,999      | 194,544|
| $150,000 or more       | 185,309|
| $60,000 - $74,999      | 166,538|
| $50,000 - $59,999      | 124,380|
| $40,000 - $49,999      | 121,333|
| $30,000 - $34,999      | 85,939 |
| $35,000 - $39,999      | 80,927 |
| $20,000 - $24,999      | 73,960 |
| $25,000 - $29,999      | 71,741 |
| $15,000 - $19,999      | 57,047 |
| Less than $5,000       | 36,408 |
| $10,000 - $12,499      | 36,190 |
| $12,500 - $14,999      | 33,342 |
| $7,500 - $9,999        | 28,415 |
| $5,000 - $7,499        | 19,806 |




2. **Division and Race Mapping**:
   
    - **Operation**: Maps division and race codes, groups by division and race, and displays the top counts.

    ```python
    division_race_columns = {"GEDIV": division_mapping, "PTDTRACE": race_mapping}
    division_race = df.select("HRHHID", "PTDTRACE", "GEDIV")
    division_race_counts = division_race.groupBy("GEDIV", "PTDTRACE").count()
    division_race_counts = Mapper(division_race_counts, "PTDTRACE", race_mapping)
    division_race_counts = Mapper(division_race_counts, "GEDIV", division_mapping)
    division_race_counts.orderBy("count", ascending=False).show(10)
    ```

Result


| Region            | Race      | Count  |
|-------------------|-----------|--------|
| South Atlantic    | White Only| 205,956|
| Mountain          | White Only| 171,708|
| Pacific           | White Only| 159,318|
| East North Central| White Only| 138,009|
| West South Central| White Only| 136,677|
| West North Central| White Only| 122,583|
| Middle Atlantic   | White Only| 104,564|
| New England       | White Only| 101,893|
| East South Central| White Only|  82,411|
| South Atlantic    | Black Only|  57,370|


3. **Telephone Access Analysis**:
 
    ```python
    telephone_access_counts = df.filter(
        (col("HETELHHD") == "2") & 
        (col("HETELAVL") == "1") & 
        (col("HEPHONEO") == "1")
    ).count()
    ```
    
   - Result
    Number of responders without telephone at home but can access telephone elsewhere and accept telephone interview: **6725**


4.
   - **Operation**: Counts and prints the number of responders with no home telephone but access to one elsewhere who accept telephone interviews, and those who have a home telephone but do not accept telephone      interviews.
     
  ```python
    telephone_no_interview_counts = df.filter(
        (col("HETELHHD") == "1") & 
        (col("HEPHONEO") == "2")
    ).count()
    print(f"Number of responders who can access a telephone but telephone interview is not accepted: {telephone_no_interview_counts}")
    ```

  - Result
  
  Number of responders who can access a telephone but telephone interview is not accepted: 0

## How to Run

1. **Set up Spark**: Ensure Spark is installed and configured.
2. **Run the Script**: Execute the Python script using a Spark-compatible environment or cluster.

```bash
spark-submit your_script.py
```
or

start jupyter lab/not3ebook
```bash
jupyter lab
```



---
