from pyspark.sql import SparkSession
from config import Config
import requests
import json
import pandas as pd

def fetch_data(spark):
  
    qualis_raw = requests.post(Config.QUALIS_URL,
    headers={
        "accept": "application/json",
        "token":  Config.TOKEN  #"dkroy07@gmail.com_83a334d3-6a2f-4422-a1bb-a01c62422d4d"
    },
    cookies={},
    auth=(),
)
    if (qualis_raw.status_code != 200):
        print (f"Error: {qualis_raw.status_code}")
        print (response.text)
        return None
        print (response.text)

    crowd_raw = requests.post(Config.CROWD_URL,
    headers={
        "accept": "application/json",
        "token": Config.TOKEN  #"dkroy07@gmail.com_83a334d3-6a2f-4422-a1bb-a01c62422d4d"
    },
    cookies={},
    auth=(),
)
    if (crowd_raw.status_code != 200):
        print (f"Error: {crowd_raw.status_code}")
        print (response.text)
        return None
        print (response.text)


    qualis_df = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize([qualis_raw.text]))
    crowd_df = spark.read.option("multiline", "true").json(spark.sparkContext.parallelize([crowd_raw.text]))


    qualis_df = qualis_df.select("_id","dnsHostName", "os" ,"isDockerHost", "lastSystemBoot")
    crowd_df = crowd_df.select("_id" ,"hostname",  "last_seen")

    # Combine the two DataFrames
    df0=qualis_df.join(crowd_df, qualis_df.dnsHostName  == crowd_df.hostname, "outer")
    
    #df1 =  df0.createOrReplaceTempView("host_os")
    #sql="select dnsHostName, os, count(*) as host_count from host_os where dnsHostName != 'NULL' group by dnsHostName, os"
    #df = spark.sql(sql)
    #df.show(10, truncate=False)
   # df.show()
    
    print("Data fetched successfully")
    return df0