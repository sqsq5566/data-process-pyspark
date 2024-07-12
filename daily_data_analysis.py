from pyspark.sql import SparkSession

spark=SparkSession.builder\
.appName("SparkByExample")\
.config("spark.dynamicAllocation.enabled", "true") \
.config("spark.dynamicAllocation.initialExecutors", "1") \
.config("spark.dynamicAllocation.minExecutors", "1") \
.config("spark.dynamicAllocation.maxExecutors", "5") \
.getOrCreate()

spark._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")

df=spark.read.csv("s3a://47-data-test/data",header=True,inferSchema=True)

df.createOrReplaceTempView("data_Q1_2019")
df_result=spark.sql("select date,count(serial_number) as drive_count ,sum(failure) as drive_failures from data_Q1_2019 group by date order by date")

#df_result.write.mode("overwrite").format("csv").option("header","true").save(f"/Users/shiqi.yuan/pyspark/answer1.csv")
s3_destination_path = "s3a://47-data-test/daily-analysis.csv"
df_result.write.mode("overwrite").format("csv").option("header","true").save(s3_destination_path)

