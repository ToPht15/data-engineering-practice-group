import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta

# 1. CẤU HÌNH HADOOP CHO WINDOWS
os.environ["HADOOP_HOME"] = "C:\\hadoop\\hadoop-3.0.0"
os.environ["PATH"] = f"{os.environ['PATH']};C:\\hadoop\\hadoop-3.0.0\\bin"
os.environ["hadoop.home.dir"] = "C:\\hadoop\\hadoop-3.0.0"

# 2. KHỞI TẠO SPARK
def initialize_spark():
    spark = SparkSession.builder \
        .appName("Exercise6") \
        .config("spark.sql.repl.eagerEval.enabled", True) \
        .config("spark.sql.session.timeZone", "UTC") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

# 3. ĐỌC DỮ LIỆU
def read_data(spark, data_path):
    try:
        # Kiểm tra file tồn tại
        required_files = ["Divvy_Trips_2019_Q4.zip", "Divvy_Trips_2020_Q1.zip"]
        for file in required_files:
            file_path = os.path.join(data_path, file)
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Thiếu file bắt buộc: {file} (tìm ở: {file_path})")


        # Schema dữ liệu
        schema = StructType([  
            StructField("trip_id", StringType()),
            StructField("start_time", TimestampType()),
            StructField("end_time", TimestampType()),
            StructField("bikeid", StringType()),
            StructField("tripduration", DoubleType()),
            StructField("from_station_id", StringType()),
            StructField("from_station_name", StringType()),
            StructField("to_station_id", StringType()),
            StructField("to_station_name", StringType()),
            StructField("usertype", StringType()),
            StructField("gender", StringType()),
            StructField("birthyear", IntegerType())
        ])

        # Đọc dữ liệu từ các file
        df1 = spark.read \
            .option("header", True) \
            .schema(schema) \
            .csv(f"file:///{os.path.abspath(data_path)}/Divvy_Trips_2019_Q4.zip")
        
        df2 = spark.read \
            .option("header", True) \
            .schema(schema) \
            .csv(f"file:///{os.path.abspath(data_path)}/Divvy_Trips_2020_Q1.zip")
        
        return df1.union(df2)
        
    except Exception as e:
        print(f"LỖI ĐỌC DỮ LIỆU: {str(e)}", file=sys.stderr)
        raise

# 4. CÁC HÀM XỬ LÝ BÁO CÁO
def create_reports_dir():
    """Tạo thư mục reports nếu chưa tồn tại"""
    reports_dir = get_reports_dir()
    os.makedirs(reports_dir, exist_ok=True)
    print(f"Thư mục báo cáo: {reports_dir}")

def get_reports_dir():
    """Lấy đường dẫn tuyệt đối đến thư mục reports"""
    base_dir = os.getcwd()  # Lấy thư mục hiện tại nơi bạn chạy script
    reports_dir = os.path.join(base_dir, "Exercises", "Exercise-6", "reports")
    return reports_dir

def write_report(df, report_name):
    """Ghi báo cáo ra file CSV"""
    reports_dir = get_reports_dir()
    report_path = os.path.join(reports_dir, report_name)
    print(f"Đang ghi: {report_path}")
    
    df.write \
        .mode("overwrite") \
        .option("header", True) \
        .csv(report_path)

def analyze_data(df):
    """Thực hiện tất cả phân tích"""
    # 1. Thời gian trung bình mỗi ngày
    avg_duration = df.withColumn("date", to_date("start_time")) \
        .groupBy("date") \
        .agg(avg("tripduration").alias("avg_duration_seconds")) \
        .orderBy("date")
    write_report(avg_duration, "average_duration_per_day")

    # 2. Số chuyến đi mỗi ngày
    trips_count = df.withColumn("date", to_date("start_time")) \
        .groupBy("date") \
        .count() \
        .orderBy("date")
    write_report(trips_count, "trips_per_day")

    # 3. Trạm phổ biến nhất theo tháng
    popular_stations = df.withColumn("month", date_format("start_time", "yyyy-MM")) \
        .groupBy("month", "from_station_name") \
        .count() \
        .orderBy("month", desc("count")) \
        .groupBy("month") \
        .agg(
            first("from_station_name").alias("most_popular_station"),
            max("count").alias("trip_count")
        )
    write_report(popular_stations, "popular_stations_per_month")

    # 4. Top 3 trạm 2 tuần cuối
    max_date_row = df.agg(max(to_date("start_time")).alias("max_date")).first()
    max_date = max_date_row["max_date"]

    if max_date is None:
        print("⚠️ Không tìm thấy ngày tối đa trong dữ liệu (start_time trống?). Bỏ qua phần phân tích top 3 trạm 2 tuần cuối.")
    else:
        start_date = max_date - timedelta(days=14)
        top3_stations = df.filter(to_date("start_time") >= start_date) \
            .withColumn("date", to_date("start_time")) \
            .groupBy("date", "from_station_name") \
            .count() \
            .orderBy("date", desc("count")) \
            .groupBy("date") \
            .agg(
                collect_list("from_station_name").alias("top3_stations"),
                collect_list("count").alias("trip_counts")
            )
        write_report(top3_stations, "top3_stations_last_two_weeks")


    # 5. So sánh theo giới tính
    gender_comparison = df.filter(col("gender").isin(["Male", "Female"])) \
        .groupBy("gender") \
        .agg(
            avg("tripduration").alias("avg_duration"),
            count("*").alias("trip_count")
        )
    write_report(gender_comparison, "gender_comparison")

    # 6. Phân tích theo độ tuổi
    current_year = datetime.now().year
    age_analysis = df.withColumn("age", current_year - col("birthyear")) \
        .groupBy("age") \
        .agg(
            avg("tripduration").alias("avg_duration"),
            count("*").alias("trip_count")
        )
    
    # Top 10 độ tuổi có thời gian đi dài nhất
    write_report(age_analysis.orderBy(desc("avg_duration")).limit(10), "top10_longest_trips_by_age")
    
    # Top 10 độ tuổi có thời gian đi ngắn nhất
    write_report(age_analysis.orderBy("avg_duration").limit(10), "top10_shortest_trips_by_age")

# 5. HÀM CHÍNH
def main():
    try:
        print("===== KHỞI TẠO SPARK =====")
        spark = initialize_spark()
        create_reports_dir()

        print("\n===== ĐỌC DỮ LIỆU =====")
        data_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "data"))
        print(f"Đường dẫn dữ liệu: {data_path}")
        df = read_data(spark, data_path)
        
        print("\n===== THỐNG KÊ DỮ LIỆU =====")
        print(f"Tổng số bản ghi: {df.count():,}")
        print("5 bản ghi mẫu:")
        df.show(5, truncate=False)
        df.printSchema()

        print("\n===== BẮT ĐẦU PHÂN TÍCH =====")
        analyze_data(df)

        print("\n===== HOÀN TẤT =====")
        print(f"Báo cáo đã được lưu tại: {get_reports_dir()}")

    except Exception as e:
        print(f"\nLỖI: {str(e)}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
        
    finally:
        spark.stop()
        print("Đã đóng Spark Session")

if __name__ == "__main__":
    import traceback
    main()
