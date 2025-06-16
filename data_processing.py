from pyspark.sql import SparkSession

from pyspark.sql.functions import col, count

def process_data(spark, df):
    print(" ****** THIS JUST  TO SHOW DEDUPED TO DF CONTENT and not required for demonstrauon only  ")

    df1 =  df.createOrReplaceTempView("host_os")
    sql="select dnsHostName, os, count(*) as host_count from host_os where dnsHostName != 'NULL' group by dnsHostName, os"
    df0 = spark.sql(sql)
    df0.show( truncate=False)

    sql="select dnsHostName, os, last_seen, count(*) as host_count from host_os where "\
         " dnsHostName != 'NULL'  group by dnsHostName, os, last_seen"
    spark.sql(sql).show(truncate=False)

    print(" ****** done  ****** ")

    #df1 =  df.dropDuplicates(["dnsHostName"])

    return df