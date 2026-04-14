## Writing Dataframes to Files

Until now we've only work with dataframes in memory in our notebook, but in the real-world application, we usually want to write some data back into a database or a file to have a permamnent record of it. Writing data to a file is pretty starightforward in pyspark as the dataframe class already has a write method we can use.

Let's create a new output file with the results of a group-by calculation.

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

4. Use group by to see the average fare amount spend using different payment types.

```python
## Cell 4

from pyspark.sql.functions import avg

df.groupBy('payment_type').agg(avg('total_amount')).show()
```
```text
+------------+------------------+
|payment_type| avg(total_amount)|
+------------+------------------+
|           1| 29.61055066192971|
|           3| 8.927757947238725|
|           2|23.411882226317573|
|           4|1.6987643617021269|
|           0| 31.68379475177417|
+------------+------------------+
```

*To write the above result dataframe to file:*

- First, we need to assign the result of the group by to a new datframe for convenience.
- Then we write the dataframe to a specific file format (csv in this case).

5. Assign the result to a new dataframe.

```python
## Cell 5

avg_fare = df.groupBy('payment_type').agg(avg('total_amount'))
```

6. Write the resulting dataframe to csv using the write method.

```python
## Cell 6

avg_fare.write.csv('/content/drive/MyDrive/pyspark_training/avg_fare', header=True, mode='overwrite')
```

*The above query tells pyspark to write the output to a directory called avg_fare, include the header rows and if necessary override any existing files.*

7. You can verify the output by running the below command directly from your google colab cell.

```sh
## Cell 7

!ls /content/drive/MyDrive/pyspark_training/avg_fare
```

```text
part-00000-a3c708ff-d019-4468-8a8c-b976ba33cb0a-c000.csv  _SUCCESS
```

*The output will have a single csv file that starts with part-.*

> [!NOTE]
> By default, PySpark creates one output file per partition of your data. This is one of the main difference between how PySpark handles data and how pandas handles data. PySpark always assumes that your data will be distributed.

> In case you're curious about how to write a dataframe to a database table or a data warehouse instead of a file, it is recommended to check out the PySpark documentation to learn more.

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>
