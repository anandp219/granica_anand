import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def main(spark_session, input_path, output_path):
    try:

        df = spark_session.read.format("delta").load(input_path)
        df.createOrReplaceTempView("user_logs")

        top_ips_daily = spark.sql(
            """
            SELECT date, 
                   ip_address,
                   COUNT(*) AS value
            FROM user_logs
            GROUP BY date, ip_address
            ORDER BY date, value DESC
        """
        ).filter("value > 0")

        window_spec_ips_daily = Window.partitionBy("date").orderBy(F.desc("value"))
        top_ips_daily = top_ips_daily.withColumn(
            "rank", F.row_number().over(window_spec_ips_daily)
        ).filter("rank <= 5")

        top_ips_daily.write.format("delta").partitionBy("date").mode(
            "overwrite"
        ).option("overwriteSchema", "true").save(f"{output_path}/top_ips_daily")

        top_ips_weekly = spark.sql(
            """
        WITH data_with_weekly_date_array AS (
            SELECT 
                SEQUENCE(cast(date as date), DATE_ADD(cast(date as date), 6)) AS weekly_date_array,
                date,
                ip_address,
                http_verb,
                path,
                device,
                status
            FROM user_logs
        ),
        data_with_weekly_date_spine AS (
        SELECT 
                timestamp,
                date,
                ip_address,
                http_verb,
                path,
                device,
                status
            FROM data_with_weekly_date_array
            LATERAL VIEW EXPLODE(weekly_date_array) t AS timestamp
        )
        SELECT 
            ip_address, timestamp, COUNT(*) as value
            FROM data_with_weekly_date_spine
            GROUP BY ip_address, timestamp
            ORDER BY timestamp, value DESC
        """
        )

        window_spec_ips_weekly = Window.partitionBy("timestamp").orderBy(
            F.desc("value")
        )
        top_ips_rolling_weekly = top_ips_weekly.withColumn(
            "rank", F.row_number().over(window_spec_ips_weekly)
        ).filter("rank <= 5")

        top_ips_rolling_weekly.write.format("delta").partitionBy("timestamp").mode(
            "overwrite"
        ).option("overwriteSchema", "true").save(f"{output_path}/top_ips_weekly")

        top_devices_daily = spark.sql(
            """
            SELECT date, 
                   device,
                   COUNT(*) AS value
            FROM user_logs
            GROUP BY date, device
            ORDER BY date, value DESC
        """
        ).filter("value > 0")

        window_spec_devices_daily = Window.partitionBy("date").orderBy(F.desc("value"))
        top_devices_daily = top_devices_daily.withColumn(
            "rank", F.row_number().over(window_spec_devices_daily)
        ).filter("rank <= 5")

        top_devices_daily.write.format("delta").partitionBy("date").mode(
            "overwrite"
        ).option("overwriteSchema", "true").save(f"{output_path}/top_devices_daily")

        top_devices_weekly = spark.sql(
            """
        WITH data_with_weekly_date_array AS (
            SELECT 
                SEQUENCE(cast(date as date), DATE_ADD(cast(date as date), 6)) AS weekly_date_array,
                date,
                ip_address,
                http_verb,
                path,
                device,
                status
            FROM user_logs
        ),
        data_with_weekly_date_spine AS (
        SELECT 
                timestamp,
                date,
                ip_address,
                http_verb,
                path,
                device,
                status
            FROM data_with_weekly_date_array
            LATERAL VIEW EXPLODE(weekly_date_array) t AS timestamp
        )
        SELECT 
            device, timestamp, COUNT(*) as value
            FROM data_with_weekly_date_spine
            GROUP BY device, timestamp
            ORDER BY timestamp, value DESC
        """
        )

        window_spec_devices_weekly = Window.partitionBy("timestamp").orderBy(
            F.desc("value")
        )
        top_devices_rolling_weekly = top_devices_weekly.withColumn(
            "rank", F.row_number().over(window_spec_devices_weekly)
        ).filter("rank <= 5")

        top_devices_rolling_weekly.write.format("delta").partitionBy("timestamp").mode(
            "overwrite"
        ).option("overwriteSchema", "true").save(f"{output_path}/top_devices_weekly")

    except Exception as e:
        print(f"Error processing logs: {str(e)}")
        raise


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Error: Two arguments are required.")
        sys.exit(1)

    input_s3_path = sys.argv[1]
    output_s3_path = sys.argv[2]

    print(f"Input S3 path: {input_s3_path}")
    print(f"Output S3 path: {output_s3_path}")

    spark = SparkSession.builder.appName("calculate_metrics").getOrCreate()

    main(spark, input_s3_path, output_s3_path)
