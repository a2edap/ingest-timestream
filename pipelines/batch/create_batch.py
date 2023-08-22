from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
import boto3
from botocore.config import Config
from datetime import datetime
import logging

boto3.setup_default_session(profile_name="dev", region_name="us-west-2")


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


def create_batch_load_task(
    client,
    database,
    table,
    input_bucket_name,
    input_object_key_prefix,
    report_bucket_name,
    report_object_key_prefix,
):
    try:
        result = client.create_batch_load_task(
            TargetDatabaseName=database,
            TargetTableName=table,
            DataModelConfiguration={
                "DataModel": {
                    "TimeColumn": "time",
                    "TimeUnit": "SECONDS",
                    "DimensionMappings": [
                        {"SourceColumn": "location", "DestinationColumn": "location"},
                    ],
                    "MultiMeasureMappings": {
                        "TargetMultiMeasureName": "megawatts",  # folder name where its coming from 
                        "MultiMeasureAttributeMappings": [
                            {
                                "SourceColumn": "wind_speed",
                                "TargetMultiMeasureAttributeName": "wind_speed",
                                "MeasureValueType": "DOUBLE",
                            },
                            {
                                "SourceColumn": "wind_direction",
                                "TargetMultiMeasureAttributeName": "wind_direction",
                                "MeasureValueType": "DOUBLE",
                            },
                        ],
                    },
                }
            },
            DataSourceConfiguration={
                "DataSourceS3Configuration": {
                    "BucketName": input_bucket_name,
                    "ObjectKeyPrefix": input_object_key_prefix,
                },
                "DataFormat": "CSV",
            },
            ReportConfiguration={
                "ReportS3Configuration": {
                    "BucketName": report_bucket_name,
                    "ObjectKeyPrefix": report_object_key_prefix,
                    "EncryptionOption": "SSE_S3",
                }
            },
        )

        task_id = result["TaskId"]
        logging.info("Batch load task created successfully.", task_id)
        return task_id
    except Exception as err:
        logging.error("Create batch load task job failed:", err)
        return None


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

    database = "awaken"
    REGION = "us-west-2"
    REPORT_BUCKET_NAME = "a2e-athena-test"  # do we want reporting bucket?
    REPORT_OBJECT_KEY_PREFIX = "timestream/logs/"

    input_prefix = "{}/{}/awaken/".format(base_path, date_time)

    session = boto3.Session()
    write_client = configure_boto3_client("timestream-write", REGION)
    s3_client = configure_boto3_client("s3", REGION)

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
                #     input_bucket,
                #     input_prefix,
                #     REPORT_BUCKET_NAME,
                #     REPORT_OBJECT_KEY_PREFIX,
                # )
            except ValueError as e:
                print(f"Error: {e}")
        else:
            error_message = f"Folder '{folder}': more than 100"
            logging.error(error_message)
            raise ValueError(error_message)
