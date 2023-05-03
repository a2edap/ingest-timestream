import boto3
from botocore.config import Config
import pandas as pd
from datetime import datetime
import numpy as np
import sys
sys.path.append('scripts\supporting_files') # add the directory containing my_list.py to the system path
from ba_id_list import ba_id_list



ONE_GB_IN_BYTES = 1073741824
TABLE_NAME = 'eia_hourly_data'
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

    for i in ba_id_list:
        QUERY = f"""
                SELECT * FROM {DATABASE_NAME}.{TABLE_NAME}
                where time >= '2018-01-01' AND time < '2019-01-01'
                and ba_id = '{i}'
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
        print(df)
        print(df.columns)
        # filename
        year = '2018'
        ba = df['ba_id'][0]
        filename = f'eia930.hr.{year}.{ba.lower()}.a1.csv'

        # metadata
        url = 'https://www.eia.gov/opendata/browser/electricity/electric-power-operational-data'
        time_resolution = 'Hourly' 
        date = datetime.utcnow()
        current_date_time = date.strftime("%Y-%m-%d %H:%M:%S")

        units = "Megawatts"

        df['time'] = df['time'].str.slice(stop=19) #cutting down the time to seconds
        df_pivoted = pd.pivot_table(df, values='megawatts', index=['ba_id', 'time'],
                                    columns=['measure_name']).reset_index()
        df_pivoted.columns = ['ba_id', 'time', 'load_mw', 'wind_mw']
        df_pivoted = df_pivoted.drop(['ba_id'], axis=1)
        df_pivoted = df_pivoted.replace(np.nan, -9999)
        df_pivoted.to_csv(filename, index=False, header=True)

        with open(filename, 'r+', encoding='utf-8') as f:
            content = f.read()
            f.seek(0, 0)
            # write the metadata columns
            f.write('Headers=11\n')
            f.write(f'BalancingAuthority={ba}\n')
            f.write(f'InputSource={url}\n')
            f.write(f'TimeResolution={time_resolution}\n')
            f.write(f'MeasurementValue={units}\n')
            f.write(f'ProcessedDate={current_date_time}\n')
            f.write('ProcessedBy=WindDataHub\n')
            f.write('Column1=Time(UTC) YYYY-MM-DD HH:MM:SS\n')
            f.write('Column2=Demand(MW)\n')
            f.write('Column3=Generation(MW)\n')
            f.write('MissingValue=-9999.0\n')
            f.write(content)
