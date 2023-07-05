# create_table.py

**Inputs**  
`database_name`: Name of the database  
`table_name`: Name of the table

**Configuration**  
Memory store duration: 24 hours  
Magnetic store duration: 7300 days  
Magnetic writes: Enabled

**Outputs**  
If the table does not exist: Table {table_name} successfully created.  
If the table already exists: Table {table_name} exists on database {database_name}. Skipping table creation.  
If table creation fails: Create table failed: {error_message}.

# delete_table.py

**Inputs**  
`database_name`: Name of the database  
`table_name`: Name of the table

**Outputs**  
If the table does exist: Delete table status {HTTPStatusCode}
If the table does not exist: Table {table_name} doesn't exist
If table deletion fails: Delete table failed {error_message}

# generate_model_files.py

**Supporting Files**
name_code_nameplate.py: Includes the data such as plant_name, plant_code, unique_id, ba_id, and nameplate to generate model files.

**Inputs**  
`input_directory`: Directory that holds all the files.
`output_directory`: Directory where all the files will be saved.
`time_resolution`: Resolution of time for the data.
`time_increment`: Time, measured in milliseconds, by which will be increased with each iteration of the loop.

**Outputs**  
The files are categorized based on turbine category (turbineA, turbineB, and turbineC). Each balancing authority/turbine category has its own separate file.

# create_batch.py

**Inputs**  
`main_directory`: Name of the main folder containing all the files.
`database`: Name of the database.
`table`: Name of the table.

**Configuration**  
Region = us-west-2
Memory Store Retention Period in Hours = 24
Magnetic Store Retention Period in Days = 7
INPUT_BUCKET_NAME: a2e-athena-test
INPUT_OBJECT_KEY_PREFIX: timestream/data/{subdir}
REPORT_BUCKET_NAME = "a2e-athena-test"
REPORT_OBJECT_KEY_PREFIX = "timestream/logs/"

**Outputs**  
Successfully created batch load task: {task_id}
Create batch load task job failed: {err}

# model_metedata.py

**Supporting Files**
turbine_specs.py: Includes the specifications for each type of turbine

**Inputs**  
`main_directory`: Directory that holds all the files.
`output_directory`: Directory where all the files will be saved.
`time_resolution`: Resolution of time used for the data in the file names.

**Outputs**  
The files containing metadata are generated along with an improved data structure to enhance readability.
