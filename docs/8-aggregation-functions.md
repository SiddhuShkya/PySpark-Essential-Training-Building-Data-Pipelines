## Aggregate Functions

Let's assume we want to get some basic analytics across our taxi rides dataframe. For example, how often do customers pay with different payment methods and what's the average fare for our rides and how does it differ by payment method.

> Aggregate functions in pyspark lets you compute values across the entire dataframe, such as the count, sum, min and max, average and standard deviation. We aggregate and compute usin the below steps:

- Group By: Choose a field in dataframe to group by.
- Aggregate: Decide which aggregate functions to use.

Lets try this with our taxi data.

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

4. Calculate how often a customer uses a specific payment methods like cash or credit card.

- Group By: payment_type
- Aggregate function: count

```python
## Cell 4

df.groupBy('payment_type').count().sort('payment_type').show()
```
```text
+------------+-------+
|payment_type|  count|
+------------+-------+
|           0|1088058|
|           1|2249747|
|           2| 314043|
|           3|  16641|
|           4|  56400|
+------------+-------+
```

> Data dictionary (using the taxi data website) to look up the payment type values.

- 0 = Flex Fare Trip
- 1 = Credit Card Payment
- 2 = Cash Payment
- 3 = No Charge
- 4 = Dispute
- 5 = Unknown
- 6 = Voided Trip

5. Check if there's a difference between the amount of the fare and the payment type.

- Group By: payment_type
- Aggregate functions: average(total_amount)

```python
## Cell 5

df.groupBy('payment_type').avg('total_amount').show()
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

*There is an alternative way of writing aggregate functions in pyspark which might come in quite handy for some use cases, instead of calling the average method directly on the result of the group by, you can use the agg method instead and pass the average function as an argument.*

Let's see how we can do it.

6. Import the avg function from PySpark.

```python
## Cell 6

from pyspark.sql.functions import avg
```

7. Use the agg method and do the solve the same problem as in step 5.

```python
## Cell 7

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

> [!NOTE]
> The above syntax gives us more control as we can also give the resulting average column a more clearer, more human-readable name using the alias method.

7. Use alias for the same query in step 7.

```python
## Cell 8

df.groupBy('payment_type').agg(avg('total_amount').alias('avg_amount')).show()
```
```text
+------------+------------------+
|payment_type|        avg_amount|
+------------+------------------+
|           1| 29.61055066192971|
|           3| 8.927757947238725|
|           2|23.411882226317573|
|           4|1.6987643617021269|
|           0| 31.68379475177417|
+------------+------------------+
```

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>