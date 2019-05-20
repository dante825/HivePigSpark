from pyspark.sql.functions import when
from pyspark import SparkConf, SparkContext, SQLContext

if __name__ == "__main__":
    click_csv = "hdfs:///user/dante/clickdata_header.csv"
    
    # Creating Spark context with Spark configuration
    conf = SparkConf().setAppName("Data cleansing and aggregation - Python").set("spark.hadoop.yarn.resourcemanager.address", "localhost:9000")
    sc = SparkContext(conf = conf)
    sql_sc = SQLContext(sc)

    # Reading the file
    df = sql_sc.read.format("com.databricks.spark.csv").option("header", "true").option("delimited", ",").load(click_csv)
    #df = spark.read.format(click_csv, format='com.databricks.spark.csv', header='true', inferSchema='true').select("text")
    #df = spark.read.format('csv').option('header', 'true').load('hdfs:///user/dante/clickdata_header.csv')

    # cleansing the data
    df2 = df.withColumn("browserid", when (df['browserid'] == 'Firefox', 'Mozilla Firefox').otherwise(df['browserid']))
    df2 = df2.withColumn('browserid', when (df2['browserid'] == 'Mozilla', 'Mozilla Firefox').otherwise(df2['browserid']))
    df2 = df2.withColumn('browserid', when (df2['browserid'] == 'InternetExplorer', 'Internet Explorer').otherwise(df2['browserid']))
    df2 = df2.withColumn('browserid', when (df2['browserid'] == 'IE', 'Internet Explorer').otherwise(df2['browserid']))
    df2 = df2.withColumn('browserid', when (df2['browserid'] == 'Chrome', 'Google Chrome').otherwise(df2['browserid']))

    # aggregating the browserid
    browserGroup = df2.groupBy('browserid').count()
    browserGroup.orderBy('count', ascending = False).show()
