## Data Formats and Loading Data

We've already covered the basics of PySpark dataframes, lets create a new one from our previously downloaded taxi data. You can create a pyspark dataframe by loading data from various sources like:

- Data files from local storage (CSV or Parquet files)
- Cloud storages (AWS S3 Buckets, Google Cloud Storage, Azure Blob Storage)
- Relational Database (tables in HIVE, RDDs in spark)

The example dataset we downloaded earlier (taxi data) was in the parquet format but pyspark can support many different formats. These includes CSV, JSON, ORC, and Avro. The main differences between these file types is the compression and how they handle schemas. But for all purposes, we can use them all in a similar way.

### Overview: Working with PySpark DataFrames

In this chapter, we'll learn:

- What PySpark DataFrames are
- What kinds of data formats can be used in PySpark
- How to explore the schema and data types of a DataFrame
- How to execute basic queries against DataFrames

---

Let's try this hands on and actually load some data.

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

> [!NOTE]
> If you're curious about loading data directly from a relational database into a dataframe, I recommend checking out the [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/index.html).

Now that we've loaded our data file into a dataframe, lets explore the data a little. When starting to explore the data its usually helpful to:

- Understand what data looks like.
- Explore columns names and data types.
- See sample data

4. Explore the data.

- Display the first few rows of the data.

```python
## Cell 4

df.show(10)
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
|       2| 2026-01-01 00:47:11|  2026-01-01 01:00:47|              2|         2.33|         1|                 N|         144|         137|           1|       14.2|  1.0|    0.5|      4.99|         0.0|                  1.0|       24.94|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:17:54|  2026-01-01 00:28:32|              1|          1.3|         1|                 N|         142|          50|           2|       11.4| 4.25|    0.5|       0.0|         0.0|                  1.0|       17.15|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:34:28|  2026-01-01 00:59:05|              0|          2.9|         1|                 N|          50|         234|           1|       22.6| 4.25|    0.5|      5.65|         0.0|                  1.0|        34.0|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:34:14|  2026-01-01 01:11:58|              1|         5.34|         1|                 N|         161|          45|           1|       37.3|  1.0|    0.5|      8.61|         0.0|                  1.0|       51.66|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:41:07|  2026-01-01 00:50:42|              3|         1.83|         1|                 N|         237|         263|           1|       10.7|  1.0|    0.5|      2.36|         0.0|                  1.0|       18.06|                 2.5|        0.0|               0.0|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 10 rows
```

- Find out the total number of rows we have in our dataset.

```python
## Cell 5

df.count()
```
```text
3724889
```

- Find out the schema of our data (column names and their data types).

    - Print column names

    ```python
    ## Cell 6

    df.schema.names
    ```
    ```text
    ['VendorID',
    'tpep_pickup_datetime',
    'tpep_dropoff_datetime',
    'passenger_count',
    'trip_distance',
    'RatecodeID',
    'store_and_fwd_flag',
    'PULocationID',
    'DOLocationID',
    'payment_type',
    'fare_amount',
    'extra',
    'mta_tax',
    'tip_amount',
    'tolls_amount',
    'improvement_surcharge',
    'total_amount',
    'congestion_surcharge',
    'Airport_fee',
    'cbd_congestion_fee']
    ```

    - Print column names along with the data types.

    ```python
    ## Cell 7

    df.printSchema()
    ```
    ```text
    root
     |-- tpep_pickup_datetime: timestamp_ntz (nullable = true)
     |-- VendorID: integer (nullable = true)
     |-- tpep_dropoff_datetime: timestamp_ntz (nullable = true)
     |-- passenger_count: long (nullable = true)
     |-- trip_distance: double (nullable = true)
     |-- RatecodeID: long (nullable = true)
     |-- store_and_fwd_flag: string (nullable = true)
     |-- PULocationID: integer (nullable = true)
     |-- DOLocationID: integer (nullable = true)
     |-- payment_type: long (nullable = true)
     |-- fare_amount: double (nullable = true)
     |-- extra: double (nullable = true)
     |-- mta_tax: double (nullable = true)
     |-- tip_amount: double (nullable = true)
     |-- tolls_amount: double (nullable = true)
     |-- improvement_surcharge: double (nullable = true)
     |-- total_amount: double (nullable = true)
     |-- congestion_surcharge: double (nullable = true)
     |-- Airport_fee: double (nullable = true)
     |-- cbd_congestion_fee: double (nullable = true)
    ```

- Display some descriptive statistics of our data like (number of rows containing data, min, max, mean and standard deviation).

```python
## Cell 8

df.describe(['passenger_count', 'total_amount']).show()
```
```text
+-------+------------------+------------------+
|summary|   passenger_count|      total_amount|
+-------+------------------+------------------+
|  count|           2636831|           3724889|
|   mean| 1.256271258946819|29.178525515824877|
| stddev|0.6702431378098701|22.585529763602587|
|    min|                 0|           -2560.2|
|    max|                 9|            2560.2|
+-------+------------------+------------------+
```

> [!NOTE]
> The describe function makes the most sense to use it on numeric columns. The above code shows the descriptivce statistics for the columns passenger_count and the total_amount columns.

Now that we have explored our data, we're ready to look more closely at our data and aloso actually start querying it for some data anaylsis.

5. Access a column from the spark dataframe.

- Access column using the dot notation.

```python
## Cell 9

df.passenger_count
```
```text
Column<'passenger_count'>
```

*While this is a convenient shorthand notation, it won't work if column name has spaces or uses reserved keywords in it.*

- Access a column using the square brackets.

```python
## Cell 10

df['passenger_count']
```
```text
Column<'passenger_count'>
```

*This is more explicit index syntax and mostly recommended to use instead of dot notation.*

- Access a column using the select function

```python
## Cell 11

df.select('passenger_count')
```
```text
DataFrame[passenger_count: bigint]
```

*This works similar to the SELECT statement in SQL.*

> [!NOTE]
> The only output you see is the datatype of the result, this is because as we discussed in the previous session that pyspark dataframes are immutable in order to ensure consistency across the distributed dataset, meaning that the select command returns a new dataframe which the colab notebook does't automaticall display as a table.

- You can use the show method to display the columns.

```python
## Cell 12

df.select('passenger_count').show()
```
```text
+---------------+
|passenger_count|
+---------------+
|              1|
|              0|
|              0|
|              4|
|              0|
|              2|
|              1|
|              0|
|              1|
|              3|
|              1|
|              1|
|              2|
|              3|
|              1|
|              1|
|              1|
|              1|
|              1|
|              1|
+---------------+
only showing top 20 rows
```

- You can also use multiple columns by simply using commas to seperate column names instead of just a name.

```python
## Cell 13

df.select('passenger_count', 'total_amount').show()
```
```text
+---------------+------------+
|passenger_count|total_amount|
+---------------+------------+
|              1|       15.86|
|              0|       13.65|
|              0|       18.95|
|              4|       55.56|
|              0|        23.1|
|              2|       24.94|
|              1|       17.15|
|              0|        34.0|
|              1|       51.66|
|              3|       18.06|
|              1|       42.42|
|              1|       21.42|
|              2|       27.65|
|              3|       10.15|
|              1|         0.0|
|              1|       22.75|
|              1|       41.58|
|              1|       41.55|
|              1|        63.4|
|              1|       18.04|
+---------------+------------+
only showing top 20 rows
```

6. Sorting our dataframe.

The sort method in pyspark allows us to sort a dataframe by one or multiple columns. This has the same effect as the ORDER BY statement in SQL.

- Sort the dataframe by total_amount in descending order.

```python
## Cell 14

df.sort('total_amount', ascending=False).show(5)
```
```text
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|       2| 2026-01-25 00:43:10|  2026-01-27 11:28:52|              1|        48.65|         1|                 N|          79|         193|           3|     2555.2|  1.0|    0.5|       0.0|         0.0|                  1.0|      2560.2|                 2.5|        0.0|               0.0|
|       1| 2026-01-15 13:56:22|  2026-01-15 13:56:22|              0|          0.0|        99|                 N|         264|         264|           1|     2500.0|  0.0|    0.0|       0.0|         0.0|                  0.0|      2500.0|                 0.0|        0.0|               0.0|
|       2| 2026-01-19 17:09:00|  2026-01-19 17:09:05|              4|          0.0|         5|                 N|          24|          24|           3|      990.0|  0.0|    0.0|       0.0|         0.0|                  1.0|       991.0|                 0.0|        0.0|               0.0|
|       2| 2026-01-26 17:27:28|  2026-01-26 20:22:44|              1|       128.05|         4|                 N|         132|         265|           2|      899.0|  2.5|    0.0|       0.0|        7.46|                  1.0|      911.71|                 0.0|       1.75|               0.0|
|       2| 2026-01-24 00:18:59|  2026-01-24 00:43:52|              1|         9.03|         5|                 N|         147|         146|           4|      900.0|  0.0|    0.0|       0.0|         0.0|                  1.0|      904.25|                 2.5|        0.0|              0.75|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 5 rows
```

- Sort the DataFrame by total_amount in descending order, followed by passenger_count in descending order.

```python
## Cell 15

df.sort(['total_amount', 'passenger_count'], ascending=[False, False]).show(5)
```
```text
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|       2| 2026-01-25 00:43:10|  2026-01-27 11:28:52|              1|        48.65|         1|                 N|          79|         193|           3|     2555.2|  1.0|    0.5|       0.0|         0.0|                  1.0|      2560.2|                 2.5|        0.0|               0.0|
|       1| 2026-01-15 13:56:22|  2026-01-15 13:56:22|              0|          0.0|        99|                 N|         264|         264|           1|     2500.0|  0.0|    0.0|       0.0|         0.0|                  0.0|      2500.0|                 0.0|        0.0|               0.0|
|       2| 2026-01-19 17:09:00|  2026-01-19 17:09:05|              4|          0.0|         5|                 N|          24|          24|           3|      990.0|  0.0|    0.0|       0.0|         0.0|                  1.0|       991.0|                 0.0|        0.0|               0.0|
|       2| 2026-01-26 17:27:28|  2026-01-26 20:22:44|              1|       128.05|         4|                 N|         132|         265|           2|      899.0|  2.5|    0.0|       0.0|        7.46|                  1.0|      911.71|                 0.0|       1.75|               0.0|
|       2| 2026-01-24 00:18:59|  2026-01-24 00:43:52|              1|         9.03|         5|                 N|         147|         146|           4|      900.0|  0.0|    0.0|       0.0|         0.0|                  1.0|      904.25|                 2.5|        0.0|              0.75|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 5 rows
```

7. Filtering our dataframe.

We can use the filter method to show the rows in the dataframe that matches certain conditions. This basically works like a WHERE caluse in SQL.

- Show the trips from our data that went went to the airport (meaning they incurred an airport fee), which is captured in airport_fee column.

```python
## Cell 16

df.filter('Airport_fee > 0').show(5)
```
```text
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|       2| 2026-01-01 00:08:06|  2026-01-01 00:33:41|              1|        10.48|         1|                 N|         138|          97|           1|       43.6|  6.0|    0.5|     10.22|         0.0|                  1.0|       63.07|                 0.0|       1.75|               0.0|
|       1| 2026-01-01 00:20:18|  2026-01-01 00:31:51|              1|          3.9|         1|                 N|         138|           7|           1|       17.0| 7.75|    0.5|      6.55|         0.0|                  1.0|        32.8|                 0.0|       1.75|               0.0|
|       2| 2026-01-01 00:01:27|  2026-01-01 00:44:30|              1|        18.47|         2|                 N|         132|         246|           1|       70.0|  0.0|    0.5|     16.34|        6.94|                  1.0|       99.78|                 2.5|       1.75|              0.75|
|       1| 2026-01-01 00:22:21|  2026-01-01 01:00:28|              1|         19.9|         1|                 N|         138|          14|           2|       74.4| 7.75|    0.5|       0.0|         0.0|                  1.0|       83.65|                 0.0|       1.75|               0.0|
|       2| 2026-01-01 00:07:54|  2026-01-01 00:19:37|              1|         5.02|         1|                 N|         138|         226|           2|       21.9|  6.0|    0.5|       0.0|         0.0|                  1.0|       31.15|                 0.0|       1.75|               0.0|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 5 rows
```

- Find airport rides with more than 2 passengers. 

```python
## Cell 17

df.filter('Airport_fee > 0 and passenger_count > 2').show(5)
```
```text
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|       2| 2026-01-01 00:08:07|  2026-01-01 00:40:39|              3|        17.18|         2|                 N|         132|         141|           1|       70.0|  0.0|    0.5|       5.0|         0.0|                  1.0|       80.75|                 2.5|       1.75|               0.0|
|       2| 2026-01-01 00:01:26|  2026-01-01 00:19:36|              4|        10.97|         1|                 N|         138|         170|           1|       42.2|  6.0|    0.5|      10.0|        6.94|                  1.0|       71.64|                 2.5|       1.75|              0.75|
|       2| 2026-01-01 00:09:10|  2026-01-01 00:43:32|              3|        15.94|         1|                 N|         132|          89|           1|       64.6|  1.0|    0.5|     13.42|         0.0|                  1.0|       82.27|                 0.0|       1.75|               0.0|
|       2| 2026-01-01 00:18:07|  2026-01-01 01:09:06|              4|        21.48|         2|                 N|         132|          48|           1|       70.0|  0.0|    0.5|       0.0|         0.0|                  1.0|        76.5|                 2.5|       1.75|              0.75|
|       2| 2026-01-01 00:38:55|  2026-01-01 00:50:07|              3|         4.94|         1|                 N|         132|         215|           2|       21.9|  1.0|    0.5|       0.0|         0.0|                  1.0|       26.15|                 0.0|       1.75|               0.0|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 5 rows
```

- Chain the above query with select to show only specific columns of our result.

```python
## Cell 18

df.filter('Airport_fee > 0 and passenger_count > 2')\
.select('VendorID', 'passenger_count', 'total_amount').show(5)
```
```text
+--------+---------------+------------+
|VendorID|passenger_count|total_amount|
+--------+---------------+------------+
|       2|              3|       80.75|
|       2|              4|       71.64|
|       2|              3|       82.27|
|       2|              4|        76.5|
|       2|              3|       26.15|
+--------+---------------+------------+
only showing top 5 rows
```

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>