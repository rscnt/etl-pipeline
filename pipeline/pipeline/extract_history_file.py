import itertools
import logging
from typing import *

import pandas as pd
from pandas import isnull

from pipeline.calculate_metrics_file import calculate_metrics


def extract_history(
    covid19_json_url_path: str,
    states_and_districts: Dict,
    city_stats_output_csv: str,
    hospitalizations_output_csv: str,
    metrics_file_csv: str,
    start_date: str = "2020-04-20",
):
    # 1. Convert the JSON to a DF
    df = pd.read_json(covid19_json_url_path)
    df = df.T

    logging.info("Parsed JSON")

    header = True

    logging.info("Reading states and districts")
    # 2. Now, filter the entries that are in the YAML
    for state, districts in states_and_districts.items():
        json_keys = ["delta", "total"]
        logging.info("Extracting data for State: {}".format(state))

        # 2.1 Filter parent dataframe by state
        districts_series = df[state].apply(pd.Series)["districts"]
        districts_series = districts_series.apply(lambda x_: {} if isnull(x_) else x_)

        # 2.2 Create a DF with district-level columns
        state_df = pd.json_normalize(districts_series)

        for district in districts:
            # 2.3 Create a regular expression to filter the district in the YAML
            y = [".".join(list(p)) for p in itertools.product([district], json_keys)]
            reg = "|".join("^" + i for i in y)

            # 2.4 Filter district using RE
            dist_df = state_df.filter(regex=reg)
            dist_df.insert(1, "district", district)
            dist_df.insert(2, "state", state)

            # 2.5 set index for easy concat
            dist_df.index = df.index
            dist_df.index.set_names(["date"], inplace=True)

            # 2.6 add genenric col names
            new_col = [
                col.replace("{}.".format(district), "") for col in list(dist_df.columns)
            ]
            dist_df.rename(
                dict(zip(list(dist_df.columns), new_col)), axis=1, inplace=True
            )

            # 2.7 Output to CSV
            logging.info("Writing data to output file")
            dist_df.to_csv(
                city_stats_output_csv, mode="w" if header else "a", header=header
            )

            # Calculate metrics
            logging.info("calculating metrics for {}".format(district))
            calculate_metrics(
                dist_df,
                header=header,
                hospitalizations_csv=hospitalizations_output_csv,
                output_city_metrics_csv=metrics_file_csv,
            )

            header = False
