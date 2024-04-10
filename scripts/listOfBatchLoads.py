import boto3
import logging

logging.basicConfig(filename="error.log", level=logging.ERROR)


def log_error(message):
    logging.error(message)


client = boto3.client("timestream-write", region_name="us-west-2")


def count_batch_load_tasks_in_progress():
    try:
        response_in_progress = client.list_batch_load_tasks(TaskStatus="IN_PROGRESS")
        response_created = client.list_batch_load_tasks(TaskStatus="CREATED")

        num_in_progress = len(response_in_progress.get("BatchLoadTasks", []))
        num_created = len(response_created.get("BatchLoadTasks", []))
        num_total = num_in_progress + num_created

        temp = []

        for task in response_in_progress.get("BatchLoadTasks", []):
            table_name = task.get("TableName")
            temp.append(table_name)

        for task in response_created.get("BatchLoadTasks", []):
            table_name = task.get("TableName")
            temp.append(table_name)

        table_counts = {}
        for table in temp:
            if table in table_counts:
                table_counts[table] += 1
            else:
                table_counts[table] = 1

        logging.info("TABLE_COUNTS: %s", table_counts)

        return num_total, table_counts

    except Exception as e:
        logging.exception("An error occurred: %s", e)

    return 0, {}
