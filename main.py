#Import thư viện cần thiết :
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# 1. Khởi tạo SparkSession 
# Chạy local nên ta dùng master("local[*]") để tận dụng tối đa số core của CPU
spark = SparkSession.builder \
    .appName("DeTai5_Instacart_Analysis") \
    .master("local[*]") \
    .getOrCreate()

# Tắt bớt log INFO dài dòng của Spark trên terminal để dễ nhìn kết quả
spark.sparkContext.setLogLevel("ERROR")

data_path = "E:/Big Data Final Course/05_archive/"

print(" --- ĐANG ĐỌC DỮ LIỆU ---")
#2. Đọc các file CSV vào DataFrame 
df_orders = spark.read.csv(data_path + "orders.csv", header=True , inferSchema=True, escape='"')
df_products = spark.read.csv(data_path + "products.csv", header=True, inferSchema=True, escape='"')
df_aisles= spark.read.csv(data_path + "aisles.csv", header=True, inferSchema=True, escape='"')
df_departments = spark.read.csv(data_path + "departments.csv", header=True, inferSchema=True, escape='"')
df_order_products = spark.read.csv(data_path + "order_products__prior.csv", header=True, inferSchema=True, escape='"')

#3. Hiển thị Schema và số bản ghi
datasets = {
    "Orders": df_orders,
    "Products": df_products,
    "Aisles": df_aisles,
    "Departments": df_departments,
    "Order_Products_Prior": df_order_products
}

for name, df in datasets.items():
    print(f"\n=============== {name} =============== ")
    # Hiển thị cấu trúc cột 
    df.printSchema()
    # Hiển thị số lượng bản ghi 
    print(f"Tổng số bản ghi: {df.count()}")

#1.2. Tính tống số ngườ dùng , tổng số đơn hàng , tổng số đơn hàng , tổng sô Aisles và tổng số department 
print("\n--- KẾT QUẢ CÂU 1.2: THỐNG KÊ TỔNG QUAN ---")
 # 1. Tính tổng số người dùng
total_users = df_orders.select(F.countDistinct("user_id")).collect()[0][0]
 
 # 2. TÍnh tổng số đơn hàng 
total_orders = df_orders.select(F.countDistinct("order_id")).collect()[0][0]

 # 3. Tính tổng số sản phẩm :
total_products = df_products.select(F.countDistinct("product_id")).collect()[0][0]

 # 4. TÍnh tổng số Aisle :
total_aisles = df_aisles.select(F.countDistinct("aisle_id")).collect()[0][0]

 # 5. Tính tổng số Department :
total_departments = df_departments.select(F.countDistinct("department_id")).collect()[0][0]

 # In kết quả ra màn hình : 
print(f"Tổng số người dùng : {total_users:,}")
print(f"Tổng số đơn hàng:    {total_orders:,}")
print(f"Tổng số sản phẩm:    {total_products:,}")
print(f"Tổng số Aisle:       {total_aisles:,}")
print(f"Tổng số Department:  {total_departments:,}")

#1.3.Join products.csv, aisles.csv, departments.csv; liệt kê 20 aisle (quầy hàng) có số lượng sản phẩm nhiều nhất.

print("\n--- KẾT QUẢ CÂU 1.3: TOP 20 AISLE CÓ NHIỀU SẢN PHẨM NHẤT ---")

df_joined =  df_products \
     .join(df_aisles, "aisle_id", "inner") \
     .join(df_departments, "department_id", "inner")

# Gom nhóm , đếm số lượng, sắp xếp và lấy top 20 
df_top_aisles = df_joined \
     .groupBy("aisle_id", "aisle", "department") \
     .agg(F.count("product_id").alias("total_products")) \
     .orderBy(F.desc("total_products")) \
     .limit(20)

df_top_aisles.show(truncate=False)

#1.4. Join orders, order_products__prior.csv , tìm 20 sản phẩm được mua nhiều nhất :

#1.5. Tính số đơn hàng trung bình trên mỗi người dùng 

#1.6. Thống kế số đơn hàng theo order_hour_of_day , tìm 3 khung giờ có số đơn nhiều nhất : 

#1.7. Tìm 20 người có số đơn hàng nhiều nhất , đồng thời tính tổng số sản phẩm họ đã mua 

#1.8. Tìm 10 phòng ban (department) 