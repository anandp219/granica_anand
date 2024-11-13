import re
import sys
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType, StructField, StructType


def transform_line(log_line):
    try:
        log_pattern = (
            r"(?P<ip>\S+)\s+"  # IP Address
            r"(?P<identity>\S+)\s+"  # Remote logname
            r"(?P<user>\S+)\s+"  # User ID
            r"\[(?P<timestamp>[^\]]+)\]\s+"  # Timestamp
            r'"(?P<request>[^"]*)"\s+'  # Request method and URL
            r"(?P<status>\d{3})\s+"  # Status code
            r"(?P<size>\S+)\s+"  # Size of response
            r'"(?P<referrer>[^"]*)"\s+'  # Referrer
            r'"(?P<user_agent>[^"]*)"'  # User-Agent
        )

        match = re.match(log_pattern, log_line)
        if not match:
            return None  # Log line didn't match expected pattern

        log_data = match.groupdict()

        # Extract fields from the matched data
        ip_address = log_data.get("ip")
        timestamp = log_data.get("timestamp")
        request = log_data.get("request")
        status = log_data.get("status")
        user_agent = log_data.get("user_agent")

        # Process the timestamp
        try:
            date_obj = datetime.strptime(timestamp, "%d/%b/%Y:%H:%M:%S %z")
            formatted_date = date_obj.strftime("%Y-%m-%d")
        except ValueError:
            formatted_date = "other"

        # Split the request into method and path
        request_parts = request.split()
        http_verb = request_parts[0] if len(request_parts) > 0 else "other"
        path = request_parts[1] if len(request_parts) > 1 else "other"

        # Detect device type based on user-agent
        user_agent_lower = user_agent.lower()
        if "mobile" in user_agent_lower:
            device = "mobile"
        elif "tablet" in user_agent_lower:
            device = "tablet"
        elif "web" in user_agent_lower:
            device = "desktop"
        else:
            device = "other"

        return ip_address, http_verb, path, formatted_date, device, status
    except ValueError as e:
        return "other", "other", "other", "other", "other", "other"


def main(spark_session, input_path, output_path):
    try:

        df = spark_session.read.text(input_path)

        # Register the UDF with return schema
        transform_line_udf = udf(
            transform_line,
            StructType(
                [
                    StructField("ip_address", StringType(), True),
                    StructField("http_verb", StringType(), True),
                    StructField("path", StringType(), True),
                    StructField("date", StringType(), True),
                    StructField("device", StringType(), True),
                    StructField("status", StringType(), True),
                ]
            ),
        )

        df = df.withColumn("parsed", transform_line_udf("value"))
        df = df.withColumn("ip_address", df["parsed.ip_address"])
        df = df.withColumn("http_verb", df["parsed.http_verb"])
        df = df.withColumn("path", df["parsed.path"])
        df = df.withColumn("date", df["parsed.date"])
        df = df.withColumn("device", df["parsed.device"])
        df = df.withColumn("status", df["parsed.status"])

        df = df.select("ip_address", "http_verb", "path", "date", "device", "status")
        df = df.filter(~(f.col("date").isNull()) & (f.col("date") != "other"))
        df.write.format("delta").partitionBy("date").mode("overwrite").option(
            "overwriteSchema", "true"
        ).save(output_path)

        print(f"Successfully wrote transformed data to {output_path}")

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

    spark = SparkSession.builder.appName("clean_and_transform_data").getOrCreate()

    main(spark, input_s3_path, output_s3_path)
