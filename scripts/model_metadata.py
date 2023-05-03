import pandas as pd
import os
from datetime import datetime
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
    parser.add_argument("directory")
    parser.add_argument("wsRange")
    parser.add_argument("turbineRating")
    parser.add_argument("rotorDiameter")
    parser.add_argument("hubHeight")
    parser.add_argument("specificPower")
    
    return parser


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    directory =  args.directory
    wsRange =  args.wsRange
    turbineRating =  args.turbineRating
    rotorDiameter =  args.rotorDiameter
    hubHeight =  args.hubHeight
    specificPower =  args.specificPower

    date = datetime.utcnow()
    current_date_time = date.strftime("%Y-%m-%d %H:%M:%S")

    original = pd.read_csv('wecc-matt.csv')

    # loop through each file in the directory
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):  # make sure it's a csv file
            filepath = os.path.join(directory, filename)

            df = pd.read_csv(filepath)

            techId = df['tech_id'][0]
            ba_id = df['ba_id'][0]
            url = f"Modelwtk.5m.a1.2018.{techId}.csv"

            plant_name_dict = dict(zip(original['plant_code'], original['plant_name']))

            df = df.drop('ba_id', axis=1)
            df = df.drop('tech_id', axis=1)

            df['time'] = pd.to_datetime(df['time'], unit='s').dt.strftime('%Y-%m-%d %H:%M:%S')

            df.fillna(-9999.0, inplace=True)

            df['wind_pw'] = df['wind_pw'].round(decimals=8)

            df_pivoted = df.pivot(index='time', columns='plant_id', values='wind_pw')
            for col in df_pivoted.columns:
                plant_name = plant_name_dict.get(str(col), 'unknown')
                new_col_name = str(col) + '(' + plant_name + ')'
                df_pivoted.rename(columns={col: new_col_name}, inplace=True)

            df_pivoted.to_csv(filepath, index=True, header=True)


            with open(filepath, 'r+', encoding='utf-8') as f:
                content = f.read()
                f.seek(0, 0)
                # write the metadata columns
                f.write('Headers=18\n')
                f.write(f'InputSource={url}\n')
                f.write(f'TimeResolution=5min\n')
                f.write(f'ProcessedDate={current_date_time}\n')
                f.write('ProcessedBy=WindDataHub\n')
                f.write(f'BalancingAuthority={ba_id}\n')
                f.write('Column1=Time(UTC) YYYY-MM-DD HH:MM:SS\n')
                f.write('Column2=PlantCode(PlantName)\n')
                f.write(f'MeasurementValue=Megawatt\n')
                f.write(f'WSRange={wsRange}\n')
                f.write(f'TurbineRating={turbineRating}(MW)\n')
                f.write(f'RotorDiameter={rotorDiameter}(m)\n')
                f.write(f'HubHeight={hubHeight}(m)\n')
                f.write(f'SpecificPower={specificPower}(W/m2)\n')
                f.write(f'Losses=16.70%\n')
                f.write('MissingValue=-9999.0\n')
                f.write(content)