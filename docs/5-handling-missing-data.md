## Handling Missing Data

Working real data will help you realize that data are rarely perfect. In an ideal world every column will have valid values but in reality we often deal with missing data to avoid skewing the results. Its very important to identify what's missing in your data and how to handle it, whether that's filling in deafaults or dropping rows. 

Let's look at our taxi dataset and see if we have any missing data in some of the key fields that we're working with. 

1. Connect the colab notebook with our google drive.

```python
## Cell 1

# Mount the google drive in the colab notebook as a directory
from google.colab import drive
drive.mount('/content/drive/')
```
```text
Mounted at /content/drive/
```

2. Initial set up code to start using pyspark in our notebook.

```python
## Cell 2

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark
```
```text
SparkSession - in-memory

SparkContext

Spark UI

Version
    v4.1.1
Master
    local[*]
AppName
    pyspark-shell
```

3. Load the taxi data file into a pyspark dataframe using the below code.

```python
## Cell 3

df = spark.read.parquet('/content/drive/MyDrive/pyspark_training/yellow_tripdata_2026-01.parquet')
df.show(5)
```
```text
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|       2| 2026-01-01 00:54:04|  2026-01-01 00:59:37|              1|         0.97|         1|                 N|         239|         238|           1|        7.2|  1.0|    0.5|      3.66|         0.0|                  1.0|       15.86|                 2.5|        0.0|               0.0|
|       1| 2026-01-01 00:34:04|  2026-01-01 00:39:47|              0|          0.9|         1|                 N|         163|         162|           2|        7.9| 4.25|    0.5|       0.0|         0.0|                  1.0|       13.65|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:57:06|  2026-01-01 01:05:59|              0|          1.4|         1|                 N|          43|         237|           1|       10.7| 4.25|    0.5|       2.5|         0.0|                  1.0|       18.95|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:15:22|  2026-01-01 00:58:10|              4|         5.58|         1|                 N|         142|         209|           1|       38.7|  1.0|    0.5|     11.11|         0.0|                  1.0|       55.56|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:27:13|  2026-01-01 00:40:43|              0|         2.16|         1|                 N|          88|         144|           1|       13.5|  1.0|    0.5|      3.85|         0.0|                  1.0|        23.1|                 2.5|        0.0|              0.75|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 5 rows
```

4. Import additional dependencies required for handling missing data.

```python
## Cell 4

from pyspark.sql.functions import col, isnull
```

*Let's keep exploring the column's we've already worked with, like passenger count and fare amount. Check whether there are any records in our dataset where the fare or the number of passengers is not populated as they would skew any calculations around average fare amounts or passenger counts.*

> We can use the isnull function in pyspark to return rows where the specified column is null or empty. Similar to the previous functions this also returns a new dataframe, so we also want to use show operation to view the result.

5. Retrieve the rows where the fare_amount column is null and then show the count of rows returned.

```python
## Cell 5

df.filter(isnull(col('fare_amount'))).count()
```
```text
0
```

In this case it looks like we don't have any null values.

6. Retrieve the rows where the passenger_count column is null and then show the count of rows returned.

```python
## Cell 6

df.filter(isnull(col('passenger_count'))).count()
```
```text
1088058
```

*The above results shows us that there are quite large number of null values in the passenger_count column (over a  million). It's probably safe to assume that every taxi ride should have atleast one passenger and that the data was just not recorded by the driver.*

> Let's back fill those null values with the value 1.

7. Use the fillna function to fill the null values with 1 for the passenger_count column, and also verify the update.

- Fill the null values with 1.

```python
## Cell 7

df1 = df.fillna({'passenger_count': 1})
```

Here, the output has been assigned to df1, so you wont see any output from the above query.

- Verify the update.

```python
## Cell 8

df1.filter(isnull(col('passenger_count'))).count()
```
```python
0
```

This proves that there are no longer any null values in the passenger_count column.

> [!IMPORTANT]
> Keep in mind that the above example is just a very simple one of how the real world data might actually be corrupted. In reality, you will come across much more complex scenarios that require a deeper knowledge of the data. 

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>

