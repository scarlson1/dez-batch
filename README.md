# Data Engineering Zoomcamp - Batch (Module 4)

## Question 1: Install Spark and PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

`4.1.1`

## Question 2: Yellow November 2025

Read the November 2025 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

- [ ] 6MB
- [ ] 25MB
- [ ] 75MB
- [ ] 100MB

## Question 3: Count records

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- [ ] 62,610
- [ ] 102,340
- [ ] 162,604
- [ ] 225,768

```python
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

df_yellow.registerTempTable('yellow')
q = "SELECT COUNT(*) FROM yellow WHERE pickup_datetime >= '2025-10-15' AND pickup_datetime < '2025-10-16';"

spark.sql(q).show()

# 128,893
```

## Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- [ ] 22.7
- [ ] 58.2
- [ ] 90.6
- [ ] 134.5

```python
longest_trip_q = "\
    SELECT \
        MAX( \
            EXTRACT(HOURS FROM dropoff_datetime-pickup_datetime) \
            + (EXTRACT(DAYS FROM dropoff_datetime-pickup_datetime) * 24) \
        ) AS max_dur \
    FROM yellow;
"

spark.sql(longest_trip_q).show()
```

## Question 5: User Interface

Spark's User Interface which shows the application's dashboard runs on which local port?

- [ ] 80
- [ ] 443
- [ ] 4040
- [ ] 8080

## Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow November 2025 data, what is the name of the LEAST frequent pickup location Zone?

- [ ] Governor's Island/Ellis Island/Liberty Island
- [ ] Arden Heights
- [ ] Rikers Island
- [ ] Jamaica Bay

```python
df_least_pickup = spark.sql("""
SELECT
    PULocationID AS LocationId,
    COUNT(1) AS number_records
FROM yellow
GROUP BY 1
ORDER BY number_records ASC
LIMIT 10;
""")

df_join = df_least_pickup.join(df_zone, on=['LocationID'], how='inner')
df_join.show()
```

---

**Homework was update, below are the questions from early Feb. (updated 2026 cohort questions above)**

---

[Link](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2026/06-batch/homework.md)

[homework.ipynb](./homework.ipynb)

### Question 1: Install spark & PySpark

- Install Spark
- Run PySpark
- Create a local spark session
- Execute spark.version.

What's the output?

_'4.1.1'_

### Question 2: Yellow October 2024

Read the October 2024 Yellow into a Spark Dataframe.

Repartition the Dataframe to 4 partitions and save it to parquet.

What is the average size of the Parquet (ending with .parquet extension) Files that were created (in MB)? Select the answer which most closely matches.

_"Output Size" in spark dashboard is 22.4 MiB - assuming that's per partition?_

- [ ] 6MB
- [x] 25MB
- [ ] 75MB
- [ ] 100MB

### Question 3: Count Records

How many taxi trips were there on the 15th of October?

Consider only trips that started on the 15th of October.

- [ ] 85,567
- [ ] 105,567
- [x] 125,567
- [ ] 145,567

```python
df_yellow = df_yellow \
    .withColumnRenamed('tpep_pickup_datetime', 'pickup_datetime') \
    .withColumnRenamed('tpep_dropoff_datetime', 'dropoff_datetime')

df_yellow.registerTempTable('yellow')
q = "SELECT COUNT(*) FROM yellow WHERE pickup_datetime >= '2024-10-15' AND pickup_datetime < '2024-10-16';"

spark.sql(q).show()

# 128,893
```

### Question 4: Longest trip

What is the length of the longest trip in the dataset in hours?

- [ ] 122
- [ ] 142
- [x] 162
- [ ] 182

```python
longest_trip_q = "\
    SELECT \
        MAX( \
            EXTRACT(HOURS FROM dropoff_datetime-pickup_datetime) \
            + (EXTRACT(DAYS FROM dropoff_datetime-pickup_datetime) * 24) \
        ) AS max_dur \
    FROM yellow;
"

spark.sql(longest_trip_q).show()
```

### Question 5: User Interface

Spark’s User Interface which shows the application's dashboard runs on which local port?

- [ ] 80
- [ ] 443
- [x] 4040
- [ ] 8080

### Question 6: Least frequent pickup location zone

Load the zone lookup data into a temp view in Spark:

```bash
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
```

Using the zone lookup data and the Yellow October 2024 data, what is the name of the LEAST frequent pickup location Zone?

- [ ] Governor's Island/Ellis Island/Liberty Island
- [ ] Arden Heights
- [ ] Rikers Island
- [x] Jamaica Bay

```python
df_least_pickup = spark.sql("""
SELECT
    PULocationID AS LocationId,
    COUNT(1) AS number_records
FROM yellow
GROUP BY 1
ORDER BY number_records ASC
LIMIT 10;
""")

df_join = df_least_pickup.join(df_zone, on=['LocationID'], how='inner')
df_join.show()

```

## Submitting the solutions

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2026/homework/hw5
