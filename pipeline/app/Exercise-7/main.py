from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def process_hard_drive_data():
    # Khởi tạo SparkSession
    spark = SparkSession.builder.appName("Exercise-7").getOrCreate()
    
    # 1. Đọc file zip CSV
    # File được giữ nguyên dưới dạng .zip, Spark tự động xử lý file nén
    df = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("data/hard-drive-2022-01-01-failures.csv.zip")
    
    # 2. Thêm cột source_file với tên file
    df = df.withColumn("source_file", F.lit("hard-drive-2022-01-01-failures.csv.zip"))
    
    # 3. Trích xuất ngày từ cột source_file và chuyển thành kiểu date
    # Sử dụng regexp_extract để lấy phần ngày (yyyy-mm-dd) và to_date để chuyển thành kiểu date
    df = df.withColumn("file_date", 
                       F.to_date(F.regexp_extract("source_file", r"hard-drive-(\d{4}-\d{2}-\d{2})", 1)))
    
    # 4. Tạo cột brand từ cột model
    # Nếu model chứa khoảng trắng, lấy phần trước khoảng trắng làm brand, nếu không thì đặt là "unknown"
    df = df.withColumn("brand", 
                       F.when(F.col("model").contains(" "), 
                              F.split(F.col("model"), " ").getItem(0)) \
                        .otherwise(F.lit("unknown")))
    
    # 5. Tạo cột storage_ranking dựa trên capacity_bytes
    # Tạo DataFrame phụ tính trung bình capacity_bytes theo model
    model_capacity_df = df.groupBy("model") \
                         .agg(F.avg("capacity_bytes").alias("avg_capacity"))
    
    # Sử dụng Window để xếp hạng theo avg_capacity (giảm dần)
    window_spec = Window.orderBy(F.desc("avg_capacity"))
    model_capacity_df = model_capacity_df.withColumn("storage_ranking", F.rank().over(window_spec))
    
    # Join lại với DataFrame chính để thêm cột storage_ranking
    df = df.join(model_capacity_df.select("model", "storage_ranking"), on="model", how="left")
    
    # 6. Tạo cột primary_key bằng cách hash các cột tạo nên bản ghi duy nhất
    # Giả sử date, serial_number, và model tạo thành bản ghi duy nhất
    df = df.withColumn("primary_key", 
                       F.sha2(F.concat_ws("_", F.col("date"), F.col("serial_number"), F.col("model")), 256))
    
    # 7. Hiển thị kết quả (chỉ một số cột để kiểm tra)
    df.select("source_file", "file_date", "brand", "model", "capacity_bytes", "storage_ranking", "primary_key") \
      .show(10, truncate=False)
    
    return df

# Chạy hàm xử lý
if __name__ == "__main__":
    df_result = process_hard_drive_data()
    print("Processing completed successfully!")