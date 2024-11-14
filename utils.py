import gzip
import os
import subprocess
from io import BytesIO
from typing import Tuple
from urllib.parse import urlparse

import boto3
from botocore.exceptions import NoCredentialsError
from dotenv import load_dotenv

load_dotenv()


def check_eks_cluster_status():
    # Initialize a session using Amazon EKS
    cluster_name = os.getenv("CLUSTER_NAME")
    eks_client = boto3.client("eks")

    try:
        # Describe the EKS cluster
        response = eks_client.describe_cluster(name=cluster_name)
        cluster_status = response["cluster"]["status"]
        cluster_id = response["cluster"]["name"]

        if cluster_status == "ACTIVE":
            print(f"Cluster {cluster_name} is running with cluster ID: {cluster_id}")
            return cluster_id
        else:
            raise RuntimeError("Cluster is not running")

    except eks_client.exceptions.ResourceNotFoundException:
        print(f"Cluster {cluster_name} does not exist.")
        raise RuntimeError("Cluster does not exist")

    except Exception as e:
        print(f"Error checking cluster status: {e}")
        raise RuntimeError("Error while finding cluster status")


def get_s3_uri(s3_path: str) -> Tuple[str, str]:
    parsed = urlparse(s3_path)
    return parsed.netloc, parsed.path.lstrip("/")


def read_gz_files_from_s3():
    """
    Reads and prints the content of all .gz files in the specified S3 bucket and prefix.
    """
    s3_bucket, s3_prefix = get_s3_uri(os.getenv("BUCKET_PATH"))
    s3_client = boto3.client("s3")
    try:
        # List objects within the specified prefix
        response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)

        # Check if the prefix contains any objects
        if "Contents" not in response:
            print("No objects found in the specified prefix.")
            return

        # Loop through each object in the prefix
        for obj in response["Contents"]:
            key = obj["Key"]

            # Only process .gz files
            if key.endswith(".gz"):
                print(f"Reading {key}...")

                # Get the object
                gz_obj = s3_client.get_object(Bucket=s3_bucket, Key=key)
                gz_body = gz_obj["Body"].read()

                # Decompress and read the .gz file content
                with gzip.GzipFile(fileobj=BytesIO(gz_body)) as gz_file:
                    file_content = gz_file.read().decode(
                        "utf-8"
                    )  # Adjust encoding if needed
                    print(file_content)  # Or process the text as needed

    finally:
        s3_client.close()


def upload_file_to_s3(local_file_path):
    bucket_name = os.getenv("BUCKET_NAME")
    region = os.getenv("AWS_REGION")
    s3 = boto3.client("s3", region_name=region)
    try:
        s3.upload_file(local_file_path, bucket_name, f"scripts/{local_file_path}")
        print(
            f"File {local_file_path} uploaded successfully to {bucket_name}/{local_file_path}"
        )
    except FileNotFoundError:
        raise RuntimeError(f"File {local_file_path} not found.")
    except NoCredentialsError:
        raise RuntimeError(f"AWS credentials not available.")
    except Exception as e:
        raise RuntimeError(f"Error uploading file to S3: {e}")


def get_temp_credentials():
    # Create an STS client
    session = boto3.Session()

    # Get the current AWS credentials from the session
    credentials = session.get_credentials()

    # Access the Access Key ID and Secret Access Key (if available in session)
    access_key = credentials.access_key
    secret_key = credentials.secret_key

    return access_key, secret_key


def get_eks_cluster_endpoint():
    cluster_name = os.getenv("CLUSTER_NAME")
    # Create an EKS client
    eks_client = boto3.client("eks")

    try:
        # Describe the EKS cluster
        response = eks_client.describe_cluster(name=cluster_name)

        # Extract the endpoint from the response
        cluster_endpoint = response["cluster"]["endpoint"]

        print(f"Cluster Endpoint: {cluster_endpoint}")
        return cluster_endpoint

    except Exception as e:
        print(f"Error retrieving cluster endpoint: {e}")
        raise RuntimeError("Unable to resolve cluster endpoint")


def run_spark_submit(
    job_name, credentials, script_path, context, input_path, output_path
):
    region = os.getenv("AWS_REGION")

    command = [
        "spark-submit",
        "--master",
        f"k8s://{get_eks_cluster_endpoint()}:443",
        "--deploy-mode",
        "cluster",
        "--name",
        job_name,
        "--conf",
        "spark.executor.instances=3",
        "--conf",
        "spark.executor.cores=1",
        "--conf",
        "spark.executor.memory=2g",
        "--conf",
        "spark.kubernetes.container.image=spark:3.5.1",
        "--conf",
        "spark.kubernetes.file.upload.path=/tmp",
        "--conf",
        "spark.jars.ivy=/tmp/ivy-cache",
        "--conf",
        "spark.kubernetes.namespace=default",
        "--conf",
        "spark.ssl.verifyServerCertificate=false",
        "--conf",
        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf",
        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf",
        "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf",
        "spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
        "--conf",
        "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "--conf",
        f"spark.hadoop.fs.s3a.endpoint=s3.{region}.amazonaws.com",
        "--conf",
        "spark.driver.extraJavaOptions=-Dcom.sun.net.ssl.checkRevocation=false -Dcom.sun.security.ssl.allowUnsafeRenegotiation=true -Djavax.net.ssl.trustAll=true -Dlog4j.logger.org.apache.spark=DEBUG",
        "--conf",
        "spark.executor.extraJavaOptions=-Dcom.sun.net.ssl.checkRevocation=false -Dcom.sun.security.ssl.allowUnsafeRenegotiation=true -Djavax.net.ssl.trustAll=true",
        "--conf",
        "spark.jars.packages=io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-catalyst_2.12:3.5.1,org.apache.spark:spark-sql_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4",
        "--conf",
        f"spark.hadoop.fs.s3a.access.key={credentials[0]}",
        "--conf",
        f"spark.hadoop.fs.s3a.secret.key={credentials[1]}",
        script_path,
        input_path,
        output_path,
    ]

    try:
        process = subprocess.Popen(
            command, shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        _, stderr = process.communicate()
        if (
            stderr is not None
            and stderr != ""
            and "termination reason: Error" in str(stderr)
        ):
            raise RuntimeError(f"Error running Spark job: {stderr}")
    except subprocess.CalledProcessError as e:
        context.log.error(f"Error running Spark job: {e.stderr}")
        raise RuntimeError(f"Spark job {job_name} failed")
