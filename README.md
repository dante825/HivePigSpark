# Hive Pig Spark comparison

## Simple cleansing and aggregation on a dataset

### Hive
Apache Hive is a data warehouse thus it does not support updating (not using transactional table).
However, it do can display the cleansed data without creating a table.

Aggregation in Hive is simpler and less time consuming compare to Pig

### Pig
Pig can read the data directly from the file and process the data through the pipeline and produce the cleansed data.
The cleansed data is saved and aggregation is performed.

The syntax of Pig is harder because it is a new language while Hive's language is similar to SQL.

### Spark
Apache Spark is supported by Scala and Python. There are 2 shells, Scala and Python.
The same operations seems to be faster in Spark.
