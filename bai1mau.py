from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("InstacartAnalysis").getOrCreate()

file1 = spark.read.csv("D:/dataset/aisles.csv",header=True,inferSchema=True)
file2 = spark.read.csv("D:/dataset/departments.csv",header=True,inferSchema=True)
file3 = spark.read.csv("D:/dataset/order_products__prior.csv",header=True,inferSchema=True)
file4 = spark.read.csv("D:/dataset/orders.csv",header=True,inferSchema=True)
file5 = spark.read.csv("D:/dataset/products.csv",header=True,inferSchema=True)

print("file 1 có schema là: ")
file1.printSchema()

print("file 2 có schema là: ")
file2.printSchema()