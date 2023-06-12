import pandas as pd
import numpy as np
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
import os
import re

START_TIME = 1514764800


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
    parser.add_argument("input_directory")
    parser.add_argument("output_directory")
    parser.add_argument("time_resolution")
    return parser


if __name__ == "__main__":
    parser = build_parser()
    args = parser.parse_args()
    input_directory = args.input_directory
    output_directory = args.output_directory
    time_resolution = args.time_resolution

    all_turbines = ["turbinea","turbineb","turbinec"]

    for turbine in all_turbines:
        turbine_path = os.path.join(output_directory,turbine)
        if not os.path.exists(turbine_path):
            os.makedirs(turbine_path)

    # csv file with supporting data
    unfiltered_data = pd.read_csv('wecc-matt.csv')
    data = unfiltered_data.loc[
        ~(
            pd.isna(unfiltered_data["plant_name"])
            | pd.isna(unfiltered_data["plant_code"])
        )
    ]

    for filename in os.listdir(input_directory):
        if filename.endswith(".csv"):  # make sure it's a csv file
            filepath = os.path.join(input_directory, filename)

        match = re.search(r"Turbine([A-Za-z]+)\.\d+", filename)
        if match:
            techid = match.group().lower().replace(".", "")

        turbine_cat = techid[:-1]

        df = pd.read_csv(filepath, low_memory=False)
        df = df.drop(df.columns[0], axis=1)
        df.drop(index=df.index[0], axis=0, inplace=True)

        rows = []

        for i in data.index:
            time_stamp = START_TIME
            multiplier = data["nameplate"][i]
            column_name = data["unique_id"][i]
            if column_name in df.columns:
                plant_id = data["plant_code"][i]
                for j in df.index:
                    wind_value = round((df.loc[j, column_name] * multiplier), 8)
                    rows.append(
                        {
                            "time": time_stamp,  # adding to main dataframe
                            "plant_id": plant_id,
                            "tech_id": techid,
                            "ba_id": data["ba_id"][i],
                            "wind_pw": wind_value,
                        }
                    )
                    time_stamp = time_stamp + 300

        main = pd.DataFrame(rows)

        main_grouped = (
            main.groupby(["time", "plant_id", "tech_id", "ba_id"])["wind_pw"]
            .sum()
            .reset_index()
        )

        ba_list = main_grouped["ba_id"].unique()

        for i in ba_list:
            temp = main_grouped[main_grouped["ba_id"] == i]
            temp = temp.sort_values("time")
            temp.to_csv(
                f"{output_directory}/{turbine_cat}/modelwkt.{time_resolution}.a1.2018.{techid}.{i}.csv",
                index=False,
            )
