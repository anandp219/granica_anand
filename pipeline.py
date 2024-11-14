import os

from dagster import (
    DagsterType,
    GraphDefinition,
    In,
    JobDefinition,
    OpExecutionContext,
    Out,
    graph,
    op,
    repository,
)
from dotenv import load_dotenv

from utils import (
    check_eks_cluster_status,
    get_temp_credentials,
    run_spark_submit,
    upload_file_to_s3,
)

ChainedOutputType = DagsterType(
    name="Chained Output Type", type_check_fn=lambda _, value: True
)

load_dotenv()


@op(
    out=Out(ChainedOutputType),
)
def clean_and_transform_data(context: OpExecutionContext) -> None:
    bucket_name = os.getenv("BUCKET_NAME")
    context.log.info("clean_and_transform_data started")
    cluster_id = check_eks_cluster_status()

    input_path = os.getenv("RAW_DATA_PATH")
    output_path = os.getenv("TRANSFORMED_DATA_PATH")
    if cluster_id is None:
        raise RuntimeError("Cluster is not running")
    context.log.info(f"cluster resolved to {cluster_id}")

    upload_file_to_s3("clean_and_transform_data.py")
    script_s3_path = f"s3://{bucket_name}/scripts/clean_and_transform_data.py"
    credentials = get_temp_credentials()

    context.log.info("Submitting spark job")
    run_spark_submit(
        "clean_and_transform_data",
        credentials,
        script_s3_path,
        context,
        input_path,
        output_path,
    )
    context.log.info("Spark job ran successfully")

    context.log.info("clean_and_transform_data ended")


@op(
    required_resource_keys={"io_manager"},
    ins={
        "_clean_and_transform_data": In(input_manager_key="io_manager"),
    },
)
def calculate_metrics(context: OpExecutionContext, _clean_and_transform_data):
    bucket_name = os.getenv("BUCKET_NAME")
    context.log.info("calculate_metrics started")

    input_path = os.getenv("TRANSFORMED_DATA_PATH")
    output_path = os.getenv("METRICS_PATH")
    cluster_id = check_eks_cluster_status()
    if cluster_id is None:
        raise RuntimeError("Cluster is not running")
    context.log.info(f"cluster resolved to {cluster_id}")

    upload_file_to_s3("calculate_metrics.py")
    script_s3_path = f"s3://{bucket_name}/scripts/calculate_metrics.py"
    credentials = get_temp_credentials()

    context.log.info("Submitting spark job")
    run_spark_submit(
        "calculate_metrics",
        credentials,
        script_s3_path,
        context,
        input_path,
        output_path,
    )
    context.log.info("Spark job ran successfully")

    context.log.info("calculate_metrics ended")


def base_pipeline_function() -> None:
    chain = clean_and_transform_data()
    _ = calculate_metrics(chain)


def build_job(graph_job: GraphDefinition) -> JobDefinition:
    return graph_job.to_job(name="data_pipeline")


@repository
def data_pipeline_repo():
    """Data pipeline Repository."""
    return [build_job(graph_job=graph(name="data_pipeline")(base_pipeline_function))]
