from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
import boto3, os, subprocess
from botocore.config import Config


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
    parser.add_argument("main_directory")
    parser.add_argument("database")
    parser.add_argument("table")
    return parser


def create_batch_load_task(
    client,
    database_name,
    table_name,
    input_bucket_name,
    input_object_key_prefix,
    report_bucket_name,
    report_object_key_prefix,
):
    try:
        result = client.create_batch_load_task(
            TargetDatabaseName=database_name,
            TargetTableName=table_name,
            DataModelConfiguration={
                "DataModel": {
                    "TimeColumn": "time",
                    "TimeUnit": "SECONDS",
                    "DimensionMappings": [
                        {"SourceColumn": "plant_id", "DestinationColumn": "plant_id"},
                        {"SourceColumn": "tech_id", "DestinationColumn": "tech_id"},
                    ],
                    "MultiMeasureMappings": {
                        "TargetMultiMeasureName": "megawatts",
                        "MultiMeasureAttributeMappings": [
                            {
                                "SourceColumn": "wind_pw",
                                "TargetMultiMeasureAttributeName": "wind_pw",
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
        print("Successfully created batch load task: ", task_id)
        return task_id
    except Exception as err:
        print("Create batch load task job failed:", err)
        return None


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    main_directory = args.main_directory
    DATABASE_NAME = args.database
    TABLE_NAME = args.table

    REGION = "us-west-2"
    HT_TTL_HOURS = 24
    CT_TTL_DAYS = 7
    REPORT_BUCKET_NAME = "a2e-athena-test"
    REPORT_OBJECT_KEY_PREFIX = "timestream/logs/"

    session = boto3.Session()

    write_client = session.client(
        "timestream-write",
        region_name=REGION,
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )

    for subdir in os.listdir(main_directory):
        subfolder_path = os.path.join(main_directory, subdir)

        if os.path.isdir(subfolder_path):
            INPUT_BUCKET_NAME = "a2e-athena-test"
            INPUT_OBJECT_KEY_PREFIX = f"timestream/data/{subdir}"
            subprocess.run(
                [
                    "aws",
                    "s3",
                    "sync",
                    subfolder_path,
                    f"s3://{INPUT_BUCKET_NAME}/{INPUT_OBJECT_KEY_PREFIX}",
                ]
            )
            task_id = create_batch_load_task(
                write_client,
                DATABASE_NAME,
                TABLE_NAME,
                INPUT_BUCKET_NAME,
                INPUT_OBJECT_KEY_PREFIX,
                REPORT_BUCKET_NAME,
                REPORT_OBJECT_KEY_PREFIX,
            )
