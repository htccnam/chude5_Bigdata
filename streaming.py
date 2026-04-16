#Câu 2 : Sử dụng orders.csv và order_products_prior.csv để thực hành streaming Top 10 sản phẩm có tần suất mua lại (reordered) cao nhất theo giờ;
#  hiển thị product_id, hour_window, reorder_count, reorder_ratio.
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder \
     .appName("Instacart_Streaming") \
     .master("local[*]") \
     .config("spark.sql.shuffle.partitions", "4") \
     .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
data_path = "E:/Big Data/05_archive/"
streaming_path = "E:/Big Data/streaming_input/"

print("-- KHỞI ĐỘNG HỆ THỐNG STREAMING ---")

df_orders_static = spark.read.csv(data_path + "orders.csv", header=True, inferSchema=True)

# Định nghĩa Schema cho bảng order_product___prior
op_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("add_to_cart_order", IntegerType(), True),
    StructField("reordered", IntegerType(), True)
])

#
df_stream_op = spark.readStream \
    .schema(op_schema) \
    .option("header", True) \
    .csv(streaming_path)

# 
df_joined = df_stream_op.join(df_orders_static, "order_id", "inner") 

# Gom nhóm sản phẩm và khung giờ
df_grouped = df_joined \
    .groupBy(
        F.col("product_id"),
        F.col("order_hour_of_day").alias("hour_window")
    ) \
    .agg(F.sum("reordered").alias("reorder_count"),
         F.count("reordered").alias("total_purchases")
    )
    
# Thêm cột reorder_ratio = reorder_count / total_purchases
df_calculated = df_grouped.withColumn(
    "reorder_ratio",
    F.round(F.col("reorder_count") / F.col("total_purchases"), 4)
)

# Sắp xếp "lấy" top 10 sản phẩm có reorder_count cao nhất 
df_final = df_calculated \
   .select("product_id", "hour_window", "reorder_count", "reorder_ratio") \
   .orderBy(F.desc("reorder_count")) \
   .limit(10)

print("Đang chờ dữ liệu chảy vào (Đợi khoảng 1-2 phút lần đầu tiên)...")
query = df_final.writeStream \
   .outputMode("complete") \
   .format("console") \
   .option("truncate", "false") \
   .start()

query.awaitTermination()