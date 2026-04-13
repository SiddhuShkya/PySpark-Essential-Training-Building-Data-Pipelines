## Unions and Joins

If you've used SQL in the past, you might be familiar with the concept of unions and joins.

- Union: Combines 2 dataframes with the same columns by appending rows.
- Join: A join add adds columns from one dataframe to another based on a matching condition.

Let's try this out in pyspark.

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

3. Load the taxi data files into a pyspark dataframe using the below code.

```python
## Cell 3

df_jan = spark.read.parquet('/content/drive/MyDrive/pyspark_training/yellow_tripdata_2026-01.parquet')
df_feb = spark.read.parquet('/content/drive/MyDrive/pyspark_training/yellow_tripdata_2026-02.parquet')
```

4. Verify if the columns in jan matches the one in feb

```python
## Cell 4

df_jan.schema.names == df_feb.schema.names
```
```text
True
```

5. Create a new dataframe called df_2026, by appending all rows from df_feb onto the rows in the df_jan dataframe.

```python
## Cell 5

df_2026 = df_jan.union(df_feb)
```

6. Count the rows in the newly created df_2026 dataframe.

```python
## Cell 6

df_2026.count()
```
```text
7124755
```

*This is the sum of the january and feburary taxi rides in 2026.*

> [!NOTE]
> Note that the union does not remove duplicates, if you combine the same data more than once, you will end up with duplicate records. 

*Now, lets move onto joins in PySpark, this works similar to SQL. You have two or more than two dataframes and specify which columns you want to join on. This appends the columns of one dataframe onto another. Depending on the type of join it might also create new rows, but more often than not we just want to add columns with additional information to existing rows.*

7. In our dataframe PULocationID and DOLocationID, represents pickup and dropoff locations. 

*These IDs are just numbers, and to know what real location these numbers refer to, we'll have to join the trip dada with a lookup table that maps location IDs.*

- Load our lookup table.

```python
## Cell 7

taxi_zone_lookup = spark.read.option('header', 'true').csv('/content/drive/MyDrive/pyspark_training/taxi_zone_lookup.csv')
taxi_zone_lookup.show(5)
```
```text
+----------+-------------+--------------------+------------+
|LocationID|      Borough|                Zone|service_zone|
+----------+-------------+--------------------+------------+
|         1|          EWR|      Newark Airport|         EWR|
|         2|       Queens|         Jamaica Bay|   Boro Zone|
|         3|        Bronx|Allerton/Pelham G...|   Boro Zone|
|         4|    Manhattan|       Alphabet City| Yellow Zone|
|         5|Staten Island|       Arden Heights|   Boro Zone|
+----------+-------------+--------------------+------------+
only showing top 5 rows
```

- Add Borough column for each pickup location to our existing dataframe with the taxi ride data.

```python
## Cell 8

df_joined = df_2026.join(taxi_zone_lookup, df_2026.PULocationID == taxi_zone_lookup.LocationID, 'left')
df_joined.select('PULocationID', 'Borough', 'Zone', 'service_zone').show(5)
```
```text
+------------+---------+--------------------+------------+
|PULocationID|  Borough|                Zone|service_zone|
+------------+---------+--------------------+------------+
|         239|Manhattan|Upper West Side S...| Yellow Zone|
|         163|Manhattan|       Midtown North| Yellow Zone|
|          43|Manhattan|        Central Park| Yellow Zone|
|         142|Manhattan| Lincoln Square East| Yellow Zone|
|          88|Manhattan|Financial Distric...| Yellow Zone|
+------------+---------+--------------------+------------+
only showing top 5 rows
```

---

# <div align="center">Thank You for Going Through This Guide! 🙏✨</div>
