from pyspark.sql import SparkSession

if __name__ == '__main__':
    # spark session creation
    # local[*]
    # here, local is referred as spark is running on single machine
    #        * is no. of worker threads.
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # Create RDD using external file
