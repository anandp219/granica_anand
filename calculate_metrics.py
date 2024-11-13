from pyspark.sql import SparkSession

# Initialize Spark session with Delta support
spark = SparkSession.builder \
    .appName("UserEngagementAnalytics") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define S3 paths
delta_path = "s3://raw-access-log-data-granica/transfomed_data/"  # replace with your actual Delta table path
metrics_path = "s3://raw-access-log-data-granica/metrics/"    # base path for saving metrics

# Load Delta table as a temporary SQL view
spark.read.format("delta").load(delta_path).createOrReplaceTempView("user_logs")

# 1. Top 5 IP addresses by request count - Daily
top_ips_daily = spark.sql("""
    SELECT date, 
           collect_list(ip_address) AS top_ips, 
           collect_list(request_count) AS ip_counts
    FROM (
        SELECT date_format(timestamp, 'yyyy-MM-dd') AS date,
               ip_address,
               COUNT(*) AS request_count,
               ROW_NUMBER() OVER (PARTITION BY date_format(timestamp, 'yyyy-MM-dd') 
                                  ORDER BY COUNT(*) DESC) AS rank
        FROM user_logs
        GROUP BY date_format(timestamp, 'yyyy-MM-dd'), ip_address
    ) 
    WHERE rank <= 5
    GROUP BY date
    ORDER BY date
""")

# 2. Top 5 IP addresses by request count - Weekly
top_ips_weekly = spark.sql("""
    SELECT week, 
           collect_list(ip_address) AS top_ips, 
           collect_list(request_count) AS ip_counts
    FROM (
        SELECT date_format(timestamp, 'yyyy-ww') AS week,
               ip_address,
               COUNT(*) AS request_count,
               ROW_NUMBER() OVER (PARTITION BY date_format(timestamp, 'yyyy-ww') 
                                  ORDER BY COUNT(*) DESC) AS rank
        FROM user_logs
        GROUP BY date_format(timestamp, 'yyyy-ww'), ip_address
    ) 
    WHERE rank <= 5
    GROUP BY week
    ORDER BY week
""")

# 3. Top 5 Devices by request count - Daily
top_devices_daily = spark.sql("""
    SELECT date, 
           collect_list(device) AS top_devices, 
           collect_list(device_count) AS device_counts
    FROM (
        SELECT date_format(timestamp, 'yyyy-MM-dd') AS date,
               device,
               COUNT(*) AS device_count,
               ROW_NUMBER() OVER (PARTITION BY date_format(timestamp, 'yyyy-MM-dd') 
                                  ORDER BY COUNT(*) DESC) AS rank
        FROM user_logs
        GROUP BY date_format(timestamp, 'yyyy-MM-dd'), device
    ) 
    WHERE rank <= 5
    GROUP BY date
    ORDER BY date
""")

# 4. Top 5 Devices by request count - Weekly
top_devices_weekly = spark.sql("""
    SELECT week, 
           collect_list(device) AS top_devices, 
           collect_list(device_count) AS device_counts
    FROM (
        SELECT date_format(timestamp, 'yyyy-ww') AS week,
               device,
               COUNT(*) AS device_count,
               ROW_NUMBER() OVER (PARTITION BY date_format(timestamp, 'yyyy-ww') 
                                  ORDER BY COUNT(*) DESC) AS rank
        FROM user_logs
        GROUP BY date_format(timestamp, 'yyyy-ww'), device
    ) 
    WHERE rank <= 5
    GROUP BY week
    ORDER BY week
""")

# Save each metric in Delta format under the specified S3 path

# Save daily IP metrics
top_ips_daily.write.format("delta").mode("overwrite").partitionBy("date").save(f"{metrics_path}top_ips_daily")

# Save weekly IP metrics
top_ips_weekly.write.format("delta").mode("overwrite").partitionBy("week").save(f"{metrics_path}top_ips_weekly")

# Save daily device metrics
top_devices_daily.write.format("delta").mode("overwrite").partitionBy("date").save(f"{metrics_path}top_devices_daily")

# Save weekly device metrics
top_devices_weekly.write.format("delta").mode("overwrite").partitionBy("week").save(f"{metrics_path}top_devices_weekly")

# Stop the Spark session
spark.stop()
