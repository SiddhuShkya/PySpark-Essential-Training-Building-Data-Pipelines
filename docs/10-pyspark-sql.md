## PySpark SQL

In this chapter, we'll learn:

- What PySpark SQL is
- How to create temporary views in PySpark SQL
- How to use SQL to query data in PySpark SQL

Until now, we've mostly used the dataframe API in PySpark to query our dataframes. The API is great, but some of us might feel a lot more confident using SQL to write complex queries for data analysis. Luckily, pyspark comes with a pyspark sql module, that allows us to use SQL syntax directly in our python code.

> What makes PySpark SQL especially powerful is that it works seamlessly with dataframe operations. This means we dont have to choose between using SQL or python to work with our data as we are able to blend both.

*For example, it might be easier to load and transform data using python methods and then to switching to use SQL for more complex queries to analyze the data.*

> [!NOTE]
> PySpark under the hood, uses the same query execution engine making them interchangeable.

Let's go through a simple walkthrough to understand more about pyspark sql.

1. In order to to use PySpark SQL, we need to first create a temporary view from our data. This way our spark seesion can have access to the data.

*Initial setup: Loading the data from our taxi file into a new dataframe called 'taxi'.*

```python
## Cell 1

# Mount the google drive in the colab notebook as a directory
from google.colab import drive
drive.mount('/content/drive/')
```
```text
Mounted at /content/drive/
```

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

```python
## Cell 3

taxi = spark.read.parquet('/content/drive/MyDrive/pyspark_training/yellow_tripdata_2026-01.parquet')
```

2. Now, that we have loaded our data into 'taxi', next we can register a temporary view with the code below.

```python
## Cell 4

taxi.createOrReplaceTempView('taxi')
```

*This statement registers our dataframe with the Spark SQL catalog as a temporary view that's also called 'taxi'. We are using the same name 'taxi' for consistency, but you also use a different name.*

> [!NOTE]
> Note that the above statement doesn't make a copy of the data, as it simply registers the original dataframe. The temporary view also doesn't persist across spark sessions, hence the temporary.

3. After registering our dataframe as tempoerary view, our spark session now knows about the data in the view and we can use PySpark SQL to query it.

- Write a simple select statement to find all rides, where total ride amount is more than 50 US dollars.

```python
## Cell 5

spark.sql('SELECT * FROM taxi WHERE total_amount > 50;').show()
```

```text
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|VendorID|tpep_pickup_datetime|tpep_dropoff_datetime|passenger_count|trip_distance|RatecodeID|store_and_fwd_flag|PULocationID|DOLocationID|payment_type|fare_amount|extra|mta_tax|tip_amount|tolls_amount|improvement_surcharge|total_amount|congestion_surcharge|Airport_fee|cbd_congestion_fee|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
|       2| 2026-01-01 00:15:22|  2026-01-01 00:58:10|              4|         5.58|         1|                 N|         142|         209|           1|       38.7|  1.0|    0.5|     11.11|         0.0|                  1.0|       55.56|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:34:14|  2026-01-01 01:11:58|              1|         5.34|         1|                 N|         161|          45|           1|       37.3|  1.0|    0.5|      8.61|         0.0|                  1.0|       51.66|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:04:59|  2026-01-01 00:46:56|              1|         10.2|         1|                 N|          90|          41|           1|       47.1| 4.25|    0.5|     10.55|         0.0|                  1.0|        63.4|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:04:15|  2026-01-01 00:42:26|              1|         6.45|         1|                 N|         148|         238|           1|       38.0|  1.0|    0.5|     10.94|         0.0|                  1.0|       54.69|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:08:06|  2026-01-01 00:33:41|              1|        10.48|         1|                 N|         138|          97|           1|       43.6|  6.0|    0.5|     10.22|         0.0|                  1.0|       63.07|                 0.0|       1.75|               0.0|
|       1| 2026-01-01 00:49:04|  2026-01-01 01:36:01|              1|         19.1|         4|                 N|         140|         265|           2|       80.0| 4.25|    0.5|       0.0|         0.0|                  1.0|       85.75|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:55:56|  2026-01-01 01:46:40|              1|          6.8|         1|                 N|         234|          41|           1|       41.5| 4.25|    0.5|      11.8|         0.0|                  1.0|       59.05|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:01:27|  2026-01-01 00:44:30|              1|        18.47|         2|                 N|         132|         246|           1|       70.0|  0.0|    0.5|     16.34|        6.94|                  1.0|       99.78|                 2.5|       1.75|              0.75|
|       2| 2026-01-01 00:53:35|  2026-01-01 01:47:24|              1|         8.38|         1|                 N|         246|         145|           1|       52.7|  1.0|    0.5|      5.84|         0.0|                  1.0|       64.29|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:22:21|  2026-01-01 01:00:28|              1|         19.9|         1|                 N|         138|          14|           2|       74.4| 7.75|    0.5|       0.0|         0.0|                  1.0|       83.65|                 0.0|       1.75|               0.0|
|       2| 2026-01-01 00:51:26|  2026-01-01 01:17:35|              1|         5.89|         1|                 N|         170|          37|           1|       30.3|  1.0|    0.5|       8.6|        6.94|                  1.0|       51.59|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:29:56|  2026-01-01 00:53:41|              1|        15.42|         1|                 N|         132|         255|           1|       56.9|  1.0|    0.5|     14.85|         0.0|                  1.0|        76.0|                 0.0|       1.75|               0.0|
|       1| 2026-01-01 00:27:23|  2026-01-01 01:23:50|              1|          6.6|         1|                 N|         249|          41|           1|       40.8| 4.25|    0.5|     13.95|         0.0|                  1.0|        60.5|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:25:24|  2026-01-01 01:03:47|              1|        10.52|         1|                 N|         239|          97|           1|       49.9|  1.0|    0.5|     16.47|         0.0|                  1.0|       71.37|                 2.5|        0.0|               0.0|
|       1| 2026-01-01 00:32:16|  2026-01-01 01:47:51|              1|         11.1|         1|                 N|         137|         133|           1|       64.6| 4.25|    0.5|     15.45|        6.94|                  1.0|       92.74|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:24:53|  2026-01-01 01:06:57|              2|          7.0|         1|                 N|         239|          87|           1|       37.3| 4.25|    0.5|     10.75|         0.0|                  1.0|        53.8|                 2.5|        0.0|              0.75|
|       2| 2026-01-01 00:00:22|  2026-01-01 00:27:41|              2|        19.15|         2|                 N|         132|          75|           2|       70.0|  0.0|    0.5|       0.0|        6.94|                  1.0|       80.19|                 0.0|       1.75|               0.0|
|       2| 2026-01-01 00:45:31|  2026-01-01 01:26:01|              1|         9.01|         1|                 N|         249|         133|           1|       47.1|  1.0|    0.5|     13.21|         0.0|                  1.0|       66.06|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:39:07|  2026-01-01 00:43:38|              1|          0.5|         1|                 N|          79|         107|           1|        5.8| 4.25|    0.5|     100.0|         0.0|                  1.0|      111.55|                 2.5|        0.0|              0.75|
|       1| 2026-01-01 00:05:14|  2026-01-01 00:55:39|              1|         16.4|         1|                 N|         161|          64|           2|       69.5| 4.25|    0.5|       0.0|         0.0|                  1.0|       75.25|                 2.5|        0.0|              0.75|
+--------+--------------------+---------------------+---------------+-------------+----------+------------------+------------+------------+------------+-----------+-----+-------+----------+------------+---------------------+------------+--------------------+-----------+------------------+
only showing top 20 rows
```

> [!IMPORTANT]
> Notice a few things in the above statement, first of all instead of using a method on a dataframe, we use the spark sql method directly on the spark session instance called that we created at the very beggining of this notebook. Second, the SQL query is just a regular string in quotes. Third, we can simply access the taxi view by its name (no quotation marks needed here) and finally, the SQL method returns a new dataframe, which is why we need to use the show method again to display the result.

- Because, the above statement returns a new dataframe, we can now start shading methods onto the SQL statement like this.

```python
## Cell 6

spark.sql('SELECT * FROM taxi WHERE total_amount > 50;')\
    .filter('passenger_count > 2')\
    .select('payment_type', 'passenger_count', 'total_amount')\
    .show(5)
```
```text
+------------+---------------+------------+
|payment_type|passenger_count|total_amount|
+------------+---------------+------------+
|           1|              4|       55.56|
|           1|              4|       54.25|
|           1|              4|      150.81|
|           1|              4|       56.65|
|           1|              3|       80.75|
+------------+---------------+------------+
only showing top 5 rows
```

*The above statement is a great example of how we can mix and match different syntax style using PySpark SQL. So, if you're more comfortable writing complex SQL than python, you can do that as well and then fine tune the result using python.*

- You can format the the query strings in SQL inside python code, especially when using longer and more complex SQL statements. You might want to assign the query to a seperate string variable before executing it, making the query much easier to read and modify.

> Let's demonstrate this using the similar problem as the above one, but this time using only the SQL statement for transformation.

```python
## Cell 7

query = '''
SELECT 
    payment_type,
    passenger_count,
    total_amount
FROM taxi
WHERE
    total_amount > 50 AND passenger_count > 2;
'''
```

> [!NOTE]
> The triple quoted string syntax in python lets you use indentation, which makes it a lot easier to read a long query. After all, even if we have computers executing these statements, its still mostly humans that will be reading the code and modifying it.

Now you can easily execute the above query like this.

```python
## Cell 8

spark.sql(query).show()
```
```text
+------------+---------------+------------+
|payment_type|passenger_count|total_amount|
+------------+---------------+------------+
|           1|              4|       55.56|
|           1|              4|       54.25|
|           1|              4|      150.81|
|           1|              4|       56.65|
|           1|              3|       80.75|
|           1|              4|       71.64|
|           1|              3|        52.5|
|           1|              4|       62.08|
|           1|              3|       82.27|
|           1|              3|      103.67|
|           1|              3|        51.6|
|           1|              3|       54.11|
|           1|              4|        76.5|
|           1|              3|      255.31|
|           1|              4|        53.1|
|           1|              3|       61.65|
|           1|              4|       55.86|
|           1|              3|       68.35|
|           2|              4|       52.85|
|           2|              3|       81.75|
+------------+---------------+------------+
only showing top 20 rows
```

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>