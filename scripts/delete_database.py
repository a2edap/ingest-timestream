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

    return parser


def delete_database(client, database_name):
    print(f"Deleting Database: {database_name}")
    try:
        result = client.delete_database(DatabaseName=database_name)
        if result and result["ResponseMetadata"]:
            print(
                "Delete database status [%s]"
                % result["ResponseMetadata"]["HTTPStatusCode"]
            )
    except client.exceptions.ResourceNotFoundException:
        print("database [%s] doesn't exist" % database_name)
    except Exception as err:
        # Not raising an exception here as we want other cleanup to continue
        print("Delete database failed:", err)


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    database_name = args.database_name

    boto3.setup_default_session(profile_name="dev", region_name="us-west-2")
    client = boto3.client(
        "timestream-write",
        config=Config(
            read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
        ),
    )

    delete_database(client, database_name)
