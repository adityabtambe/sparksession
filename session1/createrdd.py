from pyspark.sql import SparkSession

if __name__ == '__main__':
    # spark session creation
    # local[*]
    # here, local is referred as spark is running on single machine
    #        * is no. of worker threads.
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # There are primary 3 ways to create RDD
    # 1. Using existing collection

    # python collection
    input_data = [1, 2, 3, 4, 5]
    # rdd creation using existing collection
    rdd = spark.sparkContext.parallelize(input_data)
    # Calling action collect() - To display data
    print(f"RDD using existing collection: {rdd.collect()}")

    # 2. using external files
    input_file = r"D:\BWT Class 2023\sparksession\input_data\inputdata.txt"
    rdd = spark.sparkContext.textFile(input_file)
    print(f"RDD Using external file: {rdd.collect()}")

    # 3. Using existing RDD
    rdd = rdd.map(lambda x:(x,1))
    print(f"RDD Using existing RDD : {rdd.collect()}")