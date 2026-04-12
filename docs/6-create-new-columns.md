## Creating New Dataframes and Columns

Let's assume we want to look a bit more closely to our data at the trip duration in our taxi at taxi ride data. We already have the pickup and drop off times, so we can easily calculate the duration between those twos.

```text
+--------------------+---------------------+
|tpep_pickup_datetime|tpep_dropoff_datetime|
+--------------------+---------------------+
| 2026-01-01 00:54:04|  2026-01-01 00:59:37|
| 2026-01-01 00:34:04|  2026-01-01 00:39:47|
| 2026-01-01 00:57:06|  2026-01-01 01:05:59|
| 2026-01-01 00:15:22|  2026-01-01 00:58:10|
| 2026-01-01 00:27:13|  2026-01-01 00:40:43|
+--------------------+---------------------+
```

> Wouldn't it be handy to have the trip duration as a permanent column in our dataframe rather than calculating each time ?

Let's jump back to our notebook and try this thing out!!

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

4. Import some mathematical functions that helps us with the timestamps.

```python
## Cell 4

from pyspark.sql.functions import unix_timestamp, round
```

5. Now, you can calculate the trip duration and add it to the dataframe as follows.

```python
## Cell 5

df1 = df.withColumn(
    'trip_duration_minutes',
    round(
        (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime')) / 60, 
        1
    )
)
```

*The first argument is the name of the new column and the second is how it's calculated.*

6. Verify the update.

```python
## Cell 6

df.select('tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_duration_minutes').show(5)
```
```text
+--------------------+---------------------+---------------------+
|tpep_pickup_datetime|tpep_dropoff_datetime|trip_duration_minutes|
+--------------------+---------------------+---------------------+
| 2026-01-01 00:54:04|  2026-01-01 00:59:37|                  5.6|
| 2026-01-01 00:34:04|  2026-01-01 00:39:47|                  5.7|
| 2026-01-01 00:57:06|  2026-01-01 01:05:59|                  8.9|
| 2026-01-01 00:15:22|  2026-01-01 00:58:10|                 42.8|
| 2026-01-01 00:27:13|  2026-01-01 00:40:43|                 13.5|
+--------------------+---------------------+---------------------+
only showing top 5 rows
```

*Our dataset contains lots of columns that we don't really care about, and some of those columns are spelled inconsistently. We can utilize the select statement that we already know to select those columns and the withColumnsRenamed method to rename columns for more consistent naming. In our case, the withColumnsRenamed method takes a dictionary as an argument of the type old column name, colon, new column name.*

7. Rename the columns of our dataframe.

```python
## Cell 7

df2 = df1.select(
    'VendorID',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'payment_type',
    'total_amount',
    'trip_duration_minutes'
).withColumnsRenamed({
    'VendorID': 'vendor_id',
    'tpep_pickup_datetime': 'pickup_time',
    'tpep_dropoff_datetime': 'dropoff_time',
    'passenger_count': 'num_passengers',
    'trip_distance': 'distance_km',
    'payment_type': 'payment_method',
    'total_amount': 'total_fare'
})
```

8. Explore the newly created dataframe.

```python
## Cell 8

df2.show(5)
```
```text
+---------+-------------------+-------------------+--------------+-----------+--------------+----------+---------------------+
|vendor_id|        pickup_time|       dropoff_time|num_passengers|distance_km|payment_method|total_fare|trip_duration_minutes|
+---------+-------------------+-------------------+--------------+-----------+--------------+----------+---------------------+
|        2|2026-01-01 00:54:04|2026-01-01 00:59:37|             1|       0.97|             1|     15.86|                  5.6|
|        1|2026-01-01 00:34:04|2026-01-01 00:39:47|             0|        0.9|             2|     13.65|                  5.7|
|        1|2026-01-01 00:57:06|2026-01-01 01:05:59|             0|        1.4|             1|     18.95|                  8.9|
|        2|2026-01-01 00:15:22|2026-01-01 00:58:10|             4|       5.58|             1|     55.56|                 42.8|
|        2|2026-01-01 00:27:13|2026-01-01 00:40:43|             0|       2.16|             1|      23.1|                 13.5|
+---------+-------------------+-------------------+--------------+-----------+--------------+----------+---------------------+
only showing top 5 rows
```

*You can also quickly remove columns from the dataframe. The drop method returns a dataframe that's identical to the original one, but with one or more columns removed.*

9. Drop the the columns vendor_id and RatecodeID.

```python
## Cell 9

df2.drop('vendor_id', 'payment_method').show(5)
```
```text
+-------------------+-------------------+--------------+-----------+----------+---------------------+
|        pickup_time|       dropoff_time|num_passengers|distance_km|total_fare|trip_duration_minutes|
+-------------------+-------------------+--------------+-----------+----------+---------------------+
|2026-01-01 00:54:04|2026-01-01 00:59:37|             1|       0.97|     15.86|                  5.6|
|2026-01-01 00:34:04|2026-01-01 00:39:47|             0|        0.9|     13.65|                  5.7|
|2026-01-01 00:57:06|2026-01-01 01:05:59|             0|        1.4|     18.95|                  8.9|
|2026-01-01 00:15:22|2026-01-01 00:58:10|             4|       5.58|     55.56|                 42.8|
|2026-01-01 00:27:13|2026-01-01 00:40:43|             0|       2.16|      23.1|                 13.5|
+-------------------+-------------------+--------------+-----------+----------+---------------------+
only showing top 5 rows
```

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>