from pyspark.sql import SparkSession
from data_ingestion import fetch_data
from data_processing import process_data
from data_storage import write_data
from config import Config
from pyspark.sql.functions import col, count

def main():

    spark = SparkSession.builder.appName("Silk Pipeline").getOrCreate()
    df = fetch_data(spark)
    processed_df = process_data(spark,df)

    #processed_df.show(truncate=False)

    data = processed_df.toPandas().to_dict(orient='records')

    # Convert the DataFrame to a list of dictionaries

    print("Data processed successfully")
    # Write the data to MongoDB

    print("Writing data to MongoDB")

    write_data(data)
    spark.stop()

if __name__ == "__main__":
    main()