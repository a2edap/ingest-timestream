import pandas as pd
import os
from datetime import datetime
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from turbine_specs import specs

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
    parser.add_argument("main_directory")
    parser.add_argument("time_resolution")
    
    return parser


if __name__ == '__main__':
    parser = build_parser()
    args = parser.parse_args()
    main_directory =  args.main_directory
    time_resolution = args.time_resolution

    date = datetime.utcnow()
    current_date_time = date.strftime("%Y-%m-%d %H:%M:%S")

    original = pd.read_csv('wecc-matt.csv')

    for subdir in os.listdir(main_directory):
        sub_dir_path = os.path.join(main_directory, subdir)
        if os.path.isdir(sub_dir_path):
            for filename in os.listdir(sub_dir_path):
                if filename.endswith(".csv"):
                    file_path = os.path.join(sub_dir_path, filename)
                           
                    df = pd.read_csv(file_path)

                    tech_id = df['tech_id'][0]
                    url_tech_id = tech_id.lower().replace(".", "")
                    url = f"Modelwtk.{time_resolution}.a1.2018.{url_tech_id}.csv"
                    
                    ba_id = df['ba_id'][0]

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

                    df_pivoted.to_csv(file_path, index=True, header=True)

                    ws_range =  specs.get(tech_id, {}).get('ws_range')
                    turbine_rating =  specs.get(tech_id, {}).get('turbine_rating')
                    rotor_diameter =  specs.get(tech_id, {}).get('rotor_diameter')
                    hub_height =  specs.get(tech_id, {}).get('hub_height')
                    specific_power =  specs.get(tech_id, {}).get('specific_power')

                    with open(file_path, 'r+', encoding='utf-8') as f:
                        content = f.read()
                        f.seek(0, 0)
                        # write the metadata columns
                        f.write('Headers=18\n')
                        f.write(f'InputSource={url}\n')
                        f.write(f'TimeResolution={time_resolution}\n')
                        f.write(f'ProcessedDate={current_date_time}\n')
                        f.write('ProcessedBy=WindDataHub\n')
                        f.write(f'BalancingAuthority={ba_id}\n')
                        f.write('Column1=Time(UTC) YYYY-MM-DD HH:MM:SS\n')
                        f.write('Column2=PlantCode(PlantName)\n')
                        f.write(f'MeasurementValue=Megawatt\n')
                        f.write(f'WSRange={ws_range}\n')
                        f.write(f'TurbineRating={turbine_rating}(MW)\n')
                        f.write(f'RotorDiameter={rotor_diameter}(m)\n')
                        f.write(f'HubHeight={hub_height}(m)\n')
                        f.write(f'SpecificPower={specific_power}(W/m2)\n')
                        f.write(f'Losses=16.70%\n')
                        f.write('MissingValue=-9999.0\n')
                        f.write(content)
