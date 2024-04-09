import boto3
from botocore.config import Config
import argparse
from datetime import datetime, timedelta
import re
from create_table import create_table
from listOfBatchLoads import count_batch_load_tasks_in_progress
import logging
import math
import time

logging.basicConfig(filename="error.log", level=logging.ERROR)


def log_error(message):
    logging.error(message)


def create_batch_load_task(
    client, database_name, table_name, input_bucket_name, input_object_key_prefix
):
    report_bucket_name = input_bucket_name
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
        error_message = f"Create batch load task job failed: {err}"
        print(error_message)
        log_error(error_message)
        return None


def filter_keys_by_date(stage, keys, target_date_folder=None):
    if stage == "test" or (stage == "prod" and target_date_folder is not None):
        if target_date_folder is None:
            raise ValueError("target_date_folder is required for stage 'test'")
        matching_keys = [key for key in keys if target_date_folder in key]
        return matching_keys[0] if matching_keys else ""
    else:
        current_datetime = datetime.now()
        current_date = current_datetime.strftime("%Y%m%d")
        previous_hour_time = (current_datetime - timedelta(hours=1)).strftime("%H0000")
        target_format = current_date + "." + previous_hour_time

        matching_keys = [key for key in keys if target_format in key]
        return matching_keys[0] if matching_keys else ""


def extract_names(stage, input_string):
    pattern = r"[^/]+/[^/]+/\d{8}\.\d{6}/([^/]+)/([^/]+)/"
    match = re.match(pattern, input_string)

    if match:
        database_name = match.group(1)
        raw_table_suffix = match.group(2)
        updated_table_suffix = raw_table_suffix.replace(".", "_")
        table_name = f"{database_name}_{updated_table_suffix}"
        if stage == "test":
            database_name_updated = database_name + "_test"
            return database_name_updated, table_name
        else:
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


def copy_files_in_chunks(files, batch_key):
    total_files = len(files)
    num_chunks = math.ceil(total_files / 100)
    chunk_keys = []
    # Iterate through each chunk
    for i in range(num_chunks):
        # Create chunk key
        chunk_key = f"{batch_key}chunk{i + 1}"
        chunk_keys.append(chunk_key)
        # Copy files to the chunk
        chunk_files = files[
            i * 100 : min((i + 1) * 100, total_files)
        ]  # Get files for the current chunk, ensuring not to go beyond the total number of files
        for file in chunk_files:
            new_file_key = file["Key"].split("/")[-1]
            copy_source = {
                "Bucket": INPUT_BUCKET_NAME,
                "Key": file["Key"],
            }
            s3.copy_object(
                CopySource=copy_source,
                Bucket=INPUT_BUCKET_NAME,
                Key=f"{chunk_key}/{new_file_key}",
            )

    return chunk_keys


def check_and_create_batch_task(write_client, database_name, table_name, batch_key):
    MAX_CONCURRENT_ACCOUNT_TASKS = 10
    MAX_CONCURRENT_TABLE_TASKS = 5
    SLEEP_INTERVAL = 15
    while True:
        num_in_progress, table_counts = count_batch_load_tasks_in_progress()
        current_table_count = table_counts.get(table_name, 0)
        print("Num_in_progress", num_in_progress)
        if num_in_progress < MAX_CONCURRENT_ACCOUNT_TASKS:
            if current_table_count < MAX_CONCURRENT_TABLE_TASKS:
                print("before create batch table count: ", current_table_count)
                create_batch_load_task(
                    write_client,
                    database_name,
                    table_name,
                    INPUT_BUCKET_NAME,
                    input_object_key_prefix=batch_key,
                )
                break
            else:
                print(
                    f"Table {table_name} already has 5 or more batch tasks in progress. Waiting..."
                )
                time.sleep(SLEEP_INTERVAL)
        else:
            print("Maximum concurrent tasks reached. Waiting...")
            time.sleep(SLEEP_INTERVAL)


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description="Ingest Timestream")
        parser.add_argument("s3_bucket", type=str, help="Name of S3 Bucket")
        parser.add_argument(
            "stage", type=str, help="Specify being run for test or production"
        )
        parser.add_argument(
            "--target_date_folder",
            type=str,
            help="Target Date folder in format YYYYMMDD.HHMMSS",
        )
        args = parser.parse_args()

        if args.stage == "test" and args.target_date_folder is None:
            parser.error("target_date_folder is required for stage 'test'")

        session = boto3.Session(region_name="us-west-2")
        s3 = boto3.client("s3")

        write_client = session.client(
            "timestream-write",
            region_name="us-west-2",
            config=Config(
                read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
            ),
        )

        INPUT_BUCKET_NAME = args.s3_bucket
        INPUT_OBJECT_KEY_PREFIX = "timestream/jobs/"

        # get all the nested folders in INPUT_OBJECT_KEY_PREFIX
        response = s3.list_objects_v2(
            Bucket=INPUT_BUCKET_NAME, Prefix=INPUT_OBJECT_KEY_PREFIX, Delimiter="/"
        )
        allDatetimeKeys = [
            prefix.get("Prefix") for prefix in response.get("CommonPrefixes", [])
        ]

        relevantDatetimeKey = filter_keys_by_date(
            args.stage, allDatetimeKeys, args.target_date_folder
        )
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
                list_of_chunk_keys = None
                database_name, table_name = extract_names(args.stage, batch_key)
                # if more than 100 files in the folder, then it should continue and not create batch
                response = s3.list_objects_v2(
                    Bucket=INPUT_BUCKET_NAME, Prefix=batch_key
                )
                files = response.get("Contents", [])
                objects_in_folder = len(response.get("Contents", []))
                files = response.get("Contents", [])
                if objects_in_folder > 100:
                    print("inside over 100 files if statement")
                    list_of_chunk_keys = copy_files_in_chunks(files, batch_key)

                    if list_of_chunk_keys is not None:
                        for key in list_of_chunk_keys:
                            table_in_ts = table_exists(
                                client=write_client,
                                database_name=database_name,
                                table_name=table_name,
                            )

                            database_in_ts = database_exists(
                                client=write_client, database_name=database_name
                            )

                            if table_in_ts and database_in_ts:
                                check_and_create_batch_task(
                                    write_client, database_name, table_name, key
                                )
                                print("batch created")
                            elif not table_in_ts and database_in_ts:
                                create_table(write_client, database_name, table_name)
                                check_and_create_batch_task(
                                    write_client, database_name, table_name, key
                                )
                                print(
                                    f"created {table_name} table and then create batch"
                                )
                            else:
                                print("database does not exist")
                else:
                    table_in_ts = table_exists(
                        client=write_client,
                        database_name=database_name,
                        table_name=table_name,
                    )
                    database_in_ts = database_exists(
                        client=write_client, database_name=database_name
                    )

                    if table_in_ts and database_in_ts:
                        check_and_create_batch_task(
                            write_client, database_name, table_name, batch_key
                        )
                        print("batch created")
                    elif not table_in_ts and database_in_ts:
                        create_table(write_client, database_name, table_name)
                        check_and_create_batch_task(
                            write_client, database_name, table_name, batch_key
                        )
                        print(f"created {table_name} table and then create batch")
                    else:
                        print("database does not exist")

    except Exception as e:
        log_error(f"An error occurred: {str(e)}")
