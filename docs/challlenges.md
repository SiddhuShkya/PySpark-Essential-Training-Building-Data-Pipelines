## PySPpark Course Challenges


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
