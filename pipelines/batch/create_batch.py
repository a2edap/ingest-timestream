from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from typing import Any, Dict, List
import boto3
from botocore.config import Config
from datetime import datetime
import logging
import os
import pandas as pd


def build_parser():
    """Build argument parser.

    :return:  argument parser
    :rtype:  ArgumentParser
    """
    desc = "\n\tRun "

    parser = ArgumentParser(
        description=desc,
        formatter_class=ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("input_bucket")
    parser.add_argument("base_path")
    parser.add_argument(
        "--date_time", nargs="?", help="Optional date and time subdirectory"
    )
    return parser


def configure_boto3_client(service_name, region_name):
    return boto3.client(
        service_name,
        region_name=region_name,
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )


def parse_data_model(input_bucket, input_prefix) -> Dict[str, Any]:
    s3 = boto3.resource("s3")

    column_names = []
    column_models = []

    for object_summary in s3.Bucket(input_bucket).objects.filter(Prefix=input_prefix):
        if object_summary.key.endswith(".csv"):
            csv_object = s3.Object(input_bucket, object_summary.key)
            csv_body = csv_object.get()["Body"]
            df = pd.read_csv(csv_body, nrows=1)
            column_names = df.columns.tolist()
            break

    if column_names:
        multi_measure_columns = column_names[2:]

        for name in multi_measure_columns:
            column_models.append(
                {
                    "SourceColumn": f"{name}",
                    f"TargetMultiMeasureAttributeName": f"{name}",
                    "MeasureValueType": "DOUBLE",
                }
            )

    data_model = {
        "DataModel": {
            "TimeColumn": "time",
            "TimeUnit": "SECONDS",
            "DimensionMappings": [
                {"SourceColumn": "location", "DestinationColumn": "location"},
            ],
            "MultiMeasureMappings": {
                "TargetMultiMeasureName": "ID",  # TODO: call this the folder name (e.g., lidar, met, etc)
                "MultiMeasureAttributeMappings": column_models,
            },
        }
    }
    return data_model


def create_batch_load_task(
    client: str,
    table: str,
    input_prefix: str,
    input_bucket_name: str,
    input_object_key_prefix: str,
    report_bucket_name: str,
    report_object_key_prefix: str,
):
    database = "awaken"
    data_model_configuration = parse_data_model(input_bucket, input_prefix)
    data_source_configuration = {
        "DataSourceS3Configuration": {
            "BucketName": input_bucket_name,
            "ObjectKeyPrefix": input_object_key_prefix,
        },
        "DataFormat": "CSV",
    }
    report_configuration = {
        "ReportS3Configuration": {
            "BucketName": report_bucket_name,
            "ObjectKeyPrefix": report_object_key_prefix,
            "EncryptionOption": "SSE_S3",
        }
    }
    try:
        result = client.create_batch_load_task(
            TargetDatabaseName=database,
            TargetTableName=table,
            DataModelConfiguration=data_model_configuration,
            DataSourceConfiguration=data_source_configuration,
            ReportConfiguration=report_configuration,
        )
        task_id = result["TaskId"]
        logging.info("Batch load task created successfully.", task_id)
        return task_id
    except Exception as err:
        logging.error("Create batch load task job failed:", err)


from utils.timestream import TimestreamPipeline


class BatchPipeline(TimestreamPipeline):
    def run(self, inputs: List[str]) -> None:
        # inputs is a list of paths timestream/jobs/yyyymmdd.HH0000
        main()
        return super().run(inputs)


def main(input_bucket: str, base_path: str, date_time: str):
    database = "awaken"
    REPORT_BUCKET_NAME = os.getenv("REPORT_BUCKET_NAME", "a2e-athena-test")
    REPORT_OBJECT_KEY_PREFIX = os.getenv("REPORT_OBJECT_KEY_PREFIX", "timestream/logs/")

    input_prefix = "{}/{}/awaken/".format(base_path, date_time)

    boto3.setup_default_session(profile_name="dev", region_name="us-west-2")
    write_client = configure_boto3_client("timestream-write")
    s3_client = configure_boto3_client("s3")

    response = s3_client.list_objects_v2(
        Bucket=input_bucket, Prefix=input_prefix, Delimiter="/"
    )

    folders = set()
    total_size = 0
    for common_prefix in response.get("CommonPrefixes", []):
        folder = common_prefix["Prefix"].split("/")[-2]
        folders.add(folder)

    for folder in folders:
        within_folder = s3_client.list_objects_v2(
            Bucket=input_bucket, Prefix=input_prefix + folder + "/"
        )
        file_count = len(within_folder.get("Contents", []))

        if file_count < 100:
            print(f"Folder '{folder}': less than 100")
            table = f"{database}_{folder.replace('.', '_')}"
            try:
                print("batch created")
                # task_id = create_batch_load_task(
                #     write_client,
                #     database,
                #     table,  # do we want to extract the table name from the folder?
                #     input_prefix,
                #     input_bucket,
                #     input_prefix,
                #     REPORT_BUCKET_NAME,
                #     REPORT_OBJECT_KEY_PREFIX,
                # )
            except ValueError as e:
                print(f"Error: {e}")
        else:
            # TODO: Iterate through in groups of 100 files and create a batch for each
            error_message = f"Folder '{folder}': more than 100"
            logging.error(error_message)
            raise ValueError(error_message)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()

    input_bucket = args.input_bucket
    base_path = args.base_path

    if args.date_time:
        date_time = args.date_time
    else:
        current_date = datetime.now().strftime("%Y%m%d.%H0000")
        date_time = current_date

    main(input_bucket, base_path, date_time)
