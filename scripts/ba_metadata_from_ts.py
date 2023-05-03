import boto3
from botocore.config import Config
import pandas as pd
from datetime import datetime
import numpy as np

ONE_GB_IN_BYTES = 1073741824
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

    QUERY = f"""
            SELECT * FROM windpower.testtable
            where ba_id='CISO'
            and time >= '2020-01-01' AND time < '2021-01-01'
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

    # for columns-----------------
    first_row = raw_data[0].replace("{['", '').replace("']}", '')
    pairs = first_row.split("', '") 
    keys = [pair.split('=')[0] for pair in pairs] 
    columns = list(keys) 

    #create data frame from the rows-----------------
    df= pd.DataFrame(data, columns = columns)
    original = pd.read_csv('AllData\MonthlyGenerate2018-2020.csv')

    # filename-----------------
    ba = df['ba_id'][0]
    year = '2020'
    filename = f'ba.5m.a1.{year}.{ba.lower()}.csv'

    # metadata-----------------
    url = 'http://www.caiso.com/informed/Pages/ManagingOversupply.aspx'
    time_resolution = '5Mins'
    date = datetime.utcnow()
    current_date_time = date.strftime("%Y-%m-%d %H:%M:%S")
    units = (df['measure_name'][0]).capitalize()

    df = df.drop(['measure_name'], axis=1)
    df = df.drop(['ba_id'], axis=1)
    df['time'] = df['time'].str.slice(stop=19) #cutting down the time to seconds
    df = df.replace(np.nan, -9999)

    # convert from object to float and round to 8
    df['wind_mw'] = df['wind_mw'].astype(float)
    df['load_mw'] = df['load_mw'].astype(float)
    df['load_mw'] = df['load_mw'].round(decimals = 8)
    df['wind_mw'] = df['wind_mw'].round(decimals = 8)

    # ----------------------------------------
    df.to_csv(filename, index=False, header=True)

    #write metadata to the file-----------------
    with open(filename, 'r+', encoding='utf-8') as f:
        content = f.read()
        f.seek(0, 0)
        # write the metadata columns
        f.write('Headers=11\n')
        f.write(f'InputSource={url}\n')
        f.write(f'TimeResolution={time_resolution}\n')
        f.write(f'ProcessedDate={current_date_time}\n')
        f.write(f'BalancingAuthority={ba}\n')
        f.write('ProcessedBy=WindDataHub\n')
        f.write(f'MeasurementValue={units}\n')
        f.write('Column1=Time(UTC) YYYY-MM-DD HH:MM:SS\n')
        f.write('Column2=Demand(MW)\n')
        f.write('Column3=Generation(MW)\n')
        f.write('MissingValue=-9999.0\n')
        f.write(content)