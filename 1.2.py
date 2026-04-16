#1.2 Tính tổng số người dùng, đơn hàng, sản phẩm, aisle, department
from pyspark.sql import SparkSession

spark=SparkSession.builder.appName("InstacartAnalysis").getOrCreate()

# Đọc các file CSV
file1 = spark.read.csv("D:/dataset/aisles.csv", header=True, inferSchema=True)
file2 = spark.read.csv("D:/dataset/departments.csv", header=True, inferSchema=True)
file3 = spark.read.csv("D:/dataset/order_products__prior.csv", header=True, inferSchema=True)
file4 = spark.read.csv("D:/dataset/order_products__train.csv", header=True, inferSchema=True)
file5 = spark.read.csv("D:/dataset/orders.csv", header=True, inferSchema=True)
file6 = spark.read.csv("D:/dataset/products.csv", header=True, inferSchema=True)

count_file1=file1.count();
count_file2=file2.count();
count_file3=file3.count();
count_file4=file4.count();
count_file5=file5.count();
count_file6=file6.count();

print(f"tổng aisles: {count_file1}")
print(f"tổng departments: {count_file1}")
print(f"tổng order_products__prior: {count_file1}")
print(f"tổng order_products__train: {count_file4}")
print(f"tổng orders: {count_file5}")
print(f"tổng products: {count_file6}")
