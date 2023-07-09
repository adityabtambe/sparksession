from pyspark.sql import SparkSession

if __name__ == '__main__':
    # spark session creation
    # local[*]
    # here, local is referred as spark is running on single machine
    #        * is no. of worker threads.
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    # Create RDD using external file
    input_rdd = spark.sparkContext.textFile(r"D:\BWT Class 2023\sparksession\input_data\inputdata.txt")

    # split rdd using single space, As our file is also having word separated based on spaces
    split_rdd = input_rdd.flatMap(lambda x: x.split(" "))

    # Attach 1 to each word
    map_rdd = split_rdd.map(lambda x: (x, 1))

    # get count for each word
    res_rdd = map_rdd.reduceByKey(lambda x,y: x + y)

    # display result
    print("Word count example result: ")
    for element in res_rdd.collect():
        print(f"{element}")