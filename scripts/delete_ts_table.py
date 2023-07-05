import boto3
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
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
    parser.add_argument("database_name")
    parser.add_argument("table_name")

    return parser


def delete_table(client, database_name, table_name):
    print("Deleting Table")
    try:
        result = client.delete_table(DatabaseName=database_name, TableName=table_name)
        print("Delete table status [%s]" % result["ResponseMetadata"]["HTTPStatusCode"])
    except client.exceptions.ResourceNotFoundException:
        print("Table [%s] doesn't exist" % table_name)
    except Exception as err:
        print("Delete table failed:", err)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    database_name = args.database_name
    table_name = args.table_name

    boto3.setup_default_session(profile_name="dev", region_name="us-west-2")
    write_client = boto3.client(
        "timestream-write",
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )

    delete_table(write_client, database_name, table_name)
