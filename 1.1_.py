#1.1. Đọc các file vào Spark ; hiển thị schema và số bản ghi của từng file 
from pyspark.sql import SparkSession

# Khởi tạo Spark session
spark = SparkSession.builder.appName("InstacartAnalysis").getOrCreate()

# Đọc các file CSV
file1 = spark.read.csv("D:/dataset/aisles.csv", header=True, inferSchema=True)
file2 = spark.read.csv("D:/dataset/departments.csv", header=True, inferSchema=True)
file3 = spark.read.csv("D:/dataset/order_products__prior.csv", header=True, inferSchema=True)
file4 = spark.read.csv("D:/dataset/order_products__train.csv", header=True, inferSchema=True)
file5 = spark.read.csv("D:/dataset/orders.csv", header=True, inferSchema=True)
file6 = spark.read.csv("D:/dataset/products.csv", header=True, inferSchema=True)


# Hiển thị schema
print("Schema của aisles:")
file1.printSchema()
print("Schema của departments:")
file2.printSchema()
print("Schema của order_products__prior:")
file3.printSchema()
print("Schema của order_products__train:")
file4.printSchema()
print("Schema của orders:")
file5.printSchema()
print("Schema của products:")
file6.printSchema()

# Số bản ghi
print(f"Số bản ghi aisle: {file1.count()}")
print(f"Số bản ghi department: {file2.count()}")
print(f"Số bản ghi order_products__prior: {file3.count()}")
print(f"Số bản ghi order_products__train: {file4.count()}")
print(f"Số bản ghi orders: {file5.count()}")
print(f"Số bản ghi products: {file6.count()}")