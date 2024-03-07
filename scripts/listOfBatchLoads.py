import boto3

client = boto3.client("timestream-write", region_name="us-west-2")


def count_batch_load_tasks_in_progress():
    try:
        response = client.list_batch_load_tasks(TaskStatus="IN_PROGRESS")

        temp = []

        num_in_progress = len(response.get("BatchLoadTasks", []))

        for task in response.get("BatchLoadTasks", []):
            database_name = task.get("DatabaseName")
            table_name = task.get("TableName")
            temp.append(table_name)

        table_counts = {}
        for table in temp:
            if table in table_counts:
                table_counts[table] += 1
            else:
                table_counts[table] = 1

        print("TABLE_COUNTS", table_counts)

        return num_in_progress, table_counts

    except client.exceptions.InternalServerException as e:
        print(f"Internal Server Error: {e}")
    except client.exceptions.AccessDeniedException as e:
        print(f"Access Denied: {e}")
    except client.exceptions.ThrottlingException as e:
        print(f"Throttling Error: {e}")
    except client.exceptions.ValidationException as e:
        print(f"Validation Error: {e}")
    except client.exceptions.InvalidEndpointException as e:
        print(f"Invalid Endpoint: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")

    return 0, {}  # Return 0 if an error occurs
