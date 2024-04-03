import boto3
from botocore.config import Config
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser


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
    parser.add_argument("database_name")
    parser.add_argument("table_name")

    return parser


def create_table(client, database_name, table_name):
    print("Creating table")
    retention_properties = {
        "MemoryStoreRetentionPeriodInHours": 24,
        "MagneticStoreRetentionPeriodInDays": 7300,
    }
    magnetic_store_write_properties = {"EnableMagneticStoreWrites": True}
    try:
        client.create_table(
            DatabaseName=database_name,
            TableName=table_name,
            RetentionProperties=retention_properties,
            MagneticStoreWriteProperties=magnetic_store_write_properties,
        )
        print("Table [%s] successfully created." % table_name)
    except client.exceptions.ConflictException:
        print(
            "Table [%s] exists on database [%s]. Skipping table creation"
            % (table_name, database_name)
        )
    except Exception as err:
        print("Create table failed:", err)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    database_name = args.database_name
    table_name = args.table_name

    boto3.setup_default_session(region_name="us-west-2")
    write_client = boto3.client(
        "timestream-write",
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )

    create_table(write_client, database_name, table_name)
