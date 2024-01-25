import boto3
from botocore.config import Config
from datetime import datetime, timedelta
import re
from create_table import create_table


def create_batch_load_task(
    client, database_name, table_name, input_bucket_name, input_object_key_prefix
):
    report_bucket_name = "a2e-athena-test"
    report_object_key_prefix = "timestream/logs/"
    try:
        result = client.create_batch_load_task(
            TargetDatabaseName=database_name,
            TargetTableName=table_name,
            DataModelConfiguration={
                "DataModel": {
                    "TimeColumn": "time",
                    "TimeUnit": "MILLISECONDS",
                    "DimensionMappings": [
                        {"SourceColumn": "location", "DestinationColumn": "location"},
                    ],
                    "MultiMeasureMappings": {
                        "TargetMultiMeasureName": "data",
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
        print("Successfully created batch load task: ", task_id)
        return task_id
    except Exception as err:
        print("Create batch load task job failed:", err)
        return None


def filter_keys_by_date(keys):
    current_datetime = datetime.now()
    current_date = current_datetime.strftime("%Y%m%d")
    previous_hour_time = (current_datetime - timedelta(hours=1)).strftime("%H0000")
    target_format = current_date + "." + previous_hour_time

    target_format = "20240119.13000"

    matching_keys = [key for key in keys if target_format in key]
    return matching_keys[0] if matching_keys else ""


def extract_names(input_string):
    pattern = r"[^/]+/[^/]+/\d{8}\.\d{6}/([^/]+)/([^/]+)/"
    match = re.match(pattern, input_string)

    if match:
        database_name = match.group(1)
        raw_table_suffix = match.group(2)
        updated_table_suffix = raw_table_suffix.replace(".", "_")
        table_name = f"{database_name}_{updated_table_suffix}"
        return database_name, table_name
    else:
        return None


def table_exists(client, database_name, table_name):
    response = client.list_tables(DatabaseName=database_name)

    for table in response["Tables"]:
        if table["TableName"] == table_name:
            return True

    return False


def database_exists(client, database_name):
    response = client.list_databases()

    # Check if the desired database exists
    for database in response["Databases"]:
        if database["DatabaseName"] == database_name:
            return True

    return False


if __name__ == "__main__":
    session = boto3.Session()
    s3 = boto3.client("s3")

    write_client = session.client(
        "timestream-write",
        region_name="us-west-2",
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )

    INPUT_BUCKET_NAME = "a2e-athena-test"
    INPUT_OBJECT_KEY_PREFIX = f"timestream/jobs/"

    # get all the nested folders in INPUT_OBJECT_KEY_PREFIX
    response = s3.list_objects_v2(
        Bucket=INPUT_BUCKET_NAME, Prefix=INPUT_OBJECT_KEY_PREFIX, Delimiter="/"
    )
    allDatetimeKeys = [
        prefix.get("Prefix") for prefix in response.get("CommonPrefixes", [])
    ]
    print("allDatetimeKeys", allDatetimeKeys)

    relevantDatetimeKey = filter_keys_by_date(allDatetimeKeys)
    print("relevant datetime key:", relevantDatetimeKey)

    # get all the project folders (Only awaken at the moment)
    nestedProjects = s3.list_objects_v2(
        Bucket=INPUT_BUCKET_NAME, Prefix=relevantDatetimeKey, Delimiter="/"
    )
    nestProjectKeys = [
        prefix.get("Prefix") for prefix in nestedProjects.get("CommonPrefixes", [])
    ]
    print("nestProjectKeys", nestProjectKeys)

    for key in nestProjectKeys:
        newFolderswithData = s3.list_objects_v2(
            Bucket=INPUT_BUCKET_NAME, Prefix=key, Delimiter="/"
        )

        create_batch_keys = [
            prefix.get("Prefix")
            for prefix in newFolderswithData.get("CommonPrefixes", [])
        ]

        for batch_key in create_batch_keys:
            print("batch_key", batch_key)

            database_name, table_name = extract_names(batch_key)

            table_in_ts = table_exists(
                client=write_client, database_name=database_name, table_name=table_name
            )
            database_in_ts = database_exists(
                client=write_client, database_name=database_name
            )

            if table_in_ts and database_in_ts:
                create_batch_load_task(
                    write_client,
                    database_name,
                    table_name,
                    INPUT_BUCKET_NAME,
                    input_object_key_prefix=batch_key,
                )
                print("batch created")
            elif not table_in_ts and database_in_ts:
                create_table(write_client, database_name, table_name)
                create_batch_load_task(
                    write_client,
                    database_name,
                    table_name,
                    INPUT_BUCKET_NAME,
                    input_object_key_prefix=batch_key,
                )
                print("created table and then create batch")
            else:
                print("database does not exist")
