## PySpark Course Challenges


### Challenge 1

Write a query combining sort, select, and filter that shows all non-airport rides with exactly one passenger.

- Select only the trip_distacne and total_amount columns.
- Sort the resulting dataframe by the trip distance in descending order.

> Solution:

```python
df.filter('Airport_fee == 0 and passenger_count == 1')\
.select('trip_distance', 'total_amount')\
.sort('trip_distance', asceding=False)\
.show(5)
```

> Output

```text
+-------------+------------+
|trip_distance|total_amount|
+-------------+------------+
|          0.0|        7.75|
|          0.0|        8.75|
|          0.0|        13.5|
|          0.0|       29.95|
|          0.0|         7.0|
+-------------+------------+
only showing top 5 rows
```

*You might wonder, does the order of these statements matter, should we write the filter first or the select? Remember that PySpark uses lazy evaluation, meanining that it parses out all the transformation first and then decides the best order to actually execute them. Therefore even if we don't write them in the perfect order, spark will automatically optimize it for us behind the scenes.*

--- 

### Challenge 2

Write some pyspark code that does some data cleansing and  apply several of the methods you have learned before.

1. Create 2 new dataframes called df_jan_2026 and df_feb_2026 from the corresponding datafiles. 

> Solution:

```python
df_jan_2026 = spark.read.parquet('/content/drive/MyDrive/pyspark_training/yellow_tripdata_2026-01.parquet')
df_feb_2026 = spark.read.parquet('/content/drive/MyDrive/pyspark_training/yellow_tripdata_2026-02.parquet')
```

2. Create a new dataframe called df_2026_combines as union of those 2 files.

> Solution:

```python
df_2026_combined = df_jan_2026.union(df_feb_2026)
```

3. Only select the following columns from this combined dataframe and rename columns as indicated in parenthesis. Reassign the result to df_2026_combined.

<img src="../images/challenge-3.png"
     alt="pyspark-images"
     style="border:1px solid white; padding:1px; background:#fff;" />

> Solution:

```python
df_2026_combined = df_2026_combined.\
select('tpep_pickup_datetime', 'tpep_dropoff_datetime',\
        'PULocationID', 'DOLocationID', 'passenger_count', 'fare_amount',\
        'Airport_fee', 'total_amount', 'payment_type').\
withColumnsRenamed({
    'tpep_pickup_datetime': 'pu_datetime',
    'tpep_dropoff_datetime': 'do_datetime',
    'PULocationID': 'pu_location_id',
    'DOLocationID': 'do_location_id',
    'Airport_fee': 'airport_fee',
})
```

4. Create a new dataframe called taxi_zones from the taxi_zone_lookup.csv file. 

> Solution:

```python
taxi_zones = spark.read.option('header', 'true')\
     .csv('/content/drive/MyDrive/pyspark_training/taxi_zone_lookup.csv')
```

5. Join the taxi_zones dataframe onto the df_2026_combined dataframe using the do_location_id, and LocationID columns. Reassign the result of this join to df_2026_combined. 

> Solution:

```python
df_2026_combined = df_2026_combined.join(taxi_zones, df_2026_combined.do_location_id == taxi_zones.LocationID, 'left')
```

6. Final cleanup: Drop the superfluos LocationID, Zone, and service_zone columns, and rename the Bororugh column to pu_boro. Reassign the result to df_2026_combined.

> Solution:

```python
df_2026_combined = df_2026_combined.drop('LocationID', 'Zone', 'service_zone')\
     .withColumnsRenamed({'Borough': 'pu_boro'})
```

7. Display the resulting dataframe df_2026_combined in your notebook.

> Solution:

```python
df_2026_combined.show()
```

> Output:

```text
+-------------------+-------------------+--------------+--------------+---------------+-----------+-----------+------------+------------+---------+
|        pu_datetime|        do_datetime|pu_location_id|do_location_id|passenger_count|fare_amount|airport_fee|total_amount|payment_type|  pu_boro|
+-------------------+-------------------+--------------+--------------+---------------+-----------+-----------+------------+------------+---------+
|2026-01-01 00:54:04|2026-01-01 00:59:37|           239|           238|              1|        7.2|        0.0|       15.86|           1|Manhattan|
|2026-01-01 00:34:04|2026-01-01 00:39:47|           163|           162|              0|        7.9|        0.0|       13.65|           2|Manhattan|
|2026-01-01 00:57:06|2026-01-01 01:05:59|            43|           237|              0|       10.7|        0.0|       18.95|           1|Manhattan|
|2026-01-01 00:15:22|2026-01-01 00:58:10|           142|           209|              4|       38.7|        0.0|       55.56|           1|Manhattan|
|2026-01-01 00:27:13|2026-01-01 00:40:43|            88|           144|              0|       13.5|        0.0|        23.1|           1|Manhattan|
|2026-01-01 00:47:11|2026-01-01 01:00:47|           144|           137|              2|       14.2|        0.0|       24.94|           1|Manhattan|
|2026-01-01 00:17:54|2026-01-01 00:28:32|           142|            50|              1|       11.4|        0.0|       17.15|           2|Manhattan|
|2026-01-01 00:34:28|2026-01-01 00:59:05|            50|           234|              0|       22.6|        0.0|        34.0|           1|Manhattan|
|2026-01-01 00:34:14|2026-01-01 01:11:58|           161|            45|              1|       37.3|        0.0|       51.66|           1|Manhattan|
|2026-01-01 00:41:07|2026-01-01 00:50:42|           237|           263|              3|       10.7|        0.0|       18.06|           1|Manhattan|
|2026-01-01 00:12:46|2026-01-01 00:48:58|           161|           144|              1|       29.6|        0.0|       42.42|           1|Manhattan|
|2026-01-01 00:50:40|2026-01-01 01:03:09|           144|            79|              1|       12.1|        0.0|       21.42|           1|Manhattan|
|2026-01-01 00:21:33|2026-01-01 00:49:14|           163|            48|              2|       21.9|        0.0|       27.65|           2|Manhattan|
|2026-01-01 00:42:31|2026-01-01 00:44:28|           234|            90|              3|        4.4|        0.0|       10.15|           2|Manhattan|
|2026-01-01 00:40:35|2026-01-01 00:45:12|           161|           229|              1|        0.0|        0.0|         0.0|           4|Manhattan|
|2026-01-01 00:58:23|2026-01-01 01:20:19|           162|           170|              1|       17.0|        0.0|       22.75|           2|Manhattan|
|2026-01-01 00:45:15|2026-01-01 00:45:15|           142|            68|              1|       28.9|        0.0|       41.58|           1|Manhattan|
|2026-01-01 00:38:26|2026-01-01 01:04:42|            90|            87|              1|       28.9|        0.0|       41.55|           1|Manhattan|
|2026-01-01 00:04:59|2026-01-01 00:46:56|            90|            41|              1|       47.1|        0.0|        63.4|           1|Manhattan|
|2026-01-01 00:21:45|2026-01-01 00:31:43|           239|           263|              1|       11.4|        0.0|       18.04|           1|Manhattan|
+-------------------+-------------------+--------------+--------------+---------------+-----------+-----------+------------+------------+---------+
only showing top 20 rows
```
