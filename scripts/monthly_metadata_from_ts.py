import boto3
from botocore.config import Config
import pandas as pd
from datetime import datetime
import numpy as np

ONE_GB_IN_BYTES = 1073741824
TABLE_NAME = 'eia_monthly_generate_data'
DATABASE_NAME = 'windpower'

# 'synthesized_weather_model_data'
raw_data = []

def run_query(query_string):
    try:
        page_iterator = paginator.paginate(QueryString=query_string)
        for page in page_iterator:
            _parse_query_result(page)
    except Exception as err:
        print("Exception while running query:", err)

def _parse_query_result(query_result):
    query_status = query_result["QueryStatus"]

    progress_percentage = query_status["ProgressPercentage"]
    print(f"Query progress so far: {progress_percentage}%")

    bytes_scanned = float(query_status["CumulativeBytesScanned"]) // ONE_GB_IN_BYTES
    print(f"Data scanned so far: {bytes_scanned} GB")

    bytes_metered = float(query_status["CumulativeBytesMetered"]) / ONE_GB_IN_BYTES
    print(f"Data metered so far: {bytes_metered} GB")

    column_info = query_result['ColumnInfo']

    print("Metadata: %s" % column_info)
    print("Data: ")

    for row in query_result['Rows']:
        raw_data.append(_parse_row(column_info, row))
    
        
def _parse_row(column_info, row):
        data = row['Data']
        row_output = []
        for j in range(len(data)):
            info = column_info[j]
            datum = data[j]
            row_output.append(_parse_datum(info, datum))

        return "{%s}" % str(row_output)
        
def _parse_datum(info, datum):
    if datum.get('NullValue', False):
        return "%s=NULL" % info['Name'],

    column_type = info['Type']

    # If the column is of TimeSeries Type
    if 'TimeSeriesMeasureValueColumnInfo' in column_type:
        return _parse_time_series(info, datum)

    # If the column is of Array Type
    elif 'ArrayColumnInfo' in column_type:
        array_values = datum['ArrayValue']
        return "%s=%s" % (info['Name'], _parse_array(info['Type']['ArrayColumnInfo'], array_values))

    # If the column is of Row Type
    elif 'RowColumnInfo' in column_type:
        row_column_info = info['Type']['RowColumnInfo']
        row_values = datum['RowValue']
        return _parse_row(row_column_info, row_values)

    # If the column is of Scalar Type
    else:
        return _parse_column_name(info) + datum['ScalarValue']

def _parse_time_series(info, datum):
    time_series_output = []
    for data_point in datum['TimeSeriesValue']:
        time_series_output.append("{time=%s, value=%s}"
                                    % (data_point['Time'],
                                        _parse_datum(info['Type']['TimeSeriesMeasureValueColumnInfo'],
                                                        data_point['Value'])))
    return "[%s]" % str(time_series_output)

def _parse_array( array_column_info, array_values):
    array_output = []
    for datum in array_values:
        array_output.append(_parse_datum(array_column_info, datum))

    return "[%s]" % str(array_output)
        
@staticmethod
def _parse_column_name(info):
        if 'Name' in info:
            return info['Name'] + "="
        else:
            return ""


if __name__ == '__main__':

    boto3.setup_default_session(profile_name='dev',region_name='us-west-2')
    client = boto3.client('timestream-query')

    paginator = client.get_paginator('query')

    SELECT_ALL = f"SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}"

    QUERY = f"""
            SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}
            where time >= '2020-01-01' AND time < '2021-01-01'
            order by time asc
            """

    run_query(QUERY)

    data = []
    columns = []

    for each_row in raw_data: 
        each_row = each_row.replace("{['", '').replace("']}", '') 
        pairs = each_row.split("', '") 
        values = [pair.split('=')[1] for pair in pairs] 
        data.append(values)

    # for columns
    first_row = raw_data[0].replace("{['", '').replace("']}", '')
    pairs = first_row.split("', '") 
    keys = [pair.split('=')[0] for pair in pairs] 
    columns = list(keys) 

    df= pd.DataFrame(data, columns = columns)
    original = pd.read_csv('AllData\MonthlyGenerate2018-2020.csv')

    # filename
    year = '2020'
    filename = f'eia923.mon.a1.{year}.csv'

    # metadata
    url = 'https://www.eia.gov/opendata/browser/electricity/electric-power-operational-data'
    time_resolution = 'Monthly'
    date = datetime.utcnow()
    current_date_time = date.strftime("%Y-%m-%d %H:%M:%S")
    units = (df['measure_name'][0]).capitalize()

    df = df.drop(['measure_name'], axis=1)
    df['time'] = df['time'].str.slice(stop=19) #cutting down the time to seconds
    # ----------------------------------------
    df_pivoted = df.pivot(index='time', columns='plant_id', values='wind_mwh_gross')
    plant_name_dict = dict(zip(original['plantCode'], original['plantName']))
    for col in df_pivoted.columns:
        plant_name = plant_name_dict.get(int(col), 'unknown')
        new_col_name = col + '(' + plant_name + ')'
        df_pivoted.rename(columns={col: new_col_name}, inplace=True)

    df_pivoted.to_csv(filename, index=True, header=True)

    with open(filename, 'r+', encoding='utf-8') as f:
        content = f.read()
        f.seek(0, 0)
        # write the metadata columns
        f.write('Headers=10\n')
        f.write(f'InputSource={url}\n')
        f.write(f'TimeResolution={time_resolution}\n')
        f.write(f'ProcessedDate={current_date_time}\n')
        f.write('ProcessedBy=WindDataHub\n')
        f.write('MeasurementName=GrossGeneration\n')
        f.write(f'MeasurementValue={units}\n')
        f.write('Column1=Time(UTC) YYYY-MM-DD HH:MM:SS\n')
        f.write('Column2=PlantCode(PlantName)\n')
        f.write('MissingValue=-9999.0\n')
        f.write(content)
