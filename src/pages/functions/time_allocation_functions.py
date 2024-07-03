#!python3.11

import os
import polars as pl
from dotenv import load_dotenv
from .global_vars import TS_COLUMNS

load_dotenv()
DATABASE = os.environ.get("DATABASE")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
TS_TABLE = os.environ.get("TS_TABLE")


def find_task_type_hours(df: pl.DataFrame):
    """
    Function that takes the output from query_ts_table and returns a DataFrame
    with 5 columns (Engineer, ECR, EWR, NPR, Meetings, Miscellaneous)
    :param df: Output from page_functions/query_ts_table
    return stats_df: DataFrame containing the time each engineer spent on each
                     task.
    """
    eng_dict = {
        "ashahinian": "Andre",
        "jbarron": "Jacob",
        "jtorres": "Josiah",
        "malpert": "Michael",
    }

    eng_time_dicts = []
    for eng in list(eng_dict.keys()):
        # Filter to only include rows for that engineer
        eng_df = df.filter(pl.col(TS_COLUMNS[1]) == eng)
        # Columns that need to be null to count as time spent for each task
        ecr_null_cols = TS_COLUMNS[4:9] + [TS_COLUMNS[10]]
        ewr_null_cols = [TS_COLUMNS[3]] + TS_COLUMNS[5:9] + [TS_COLUMNS[10]]
        npr_null_cols = TS_COLUMNS[3:5] + TS_COLUMNS[6:9] + [TS_COLUMNS[10]]
        misc_null_cols = TS_COLUMNS[3:9] + [TS_COLUMNS[10]]
        # Filter engineer DataFrame for condition that the task column is not
        # null and the null columns are null
        ecr_df = eng_df.filter(  # ECR
            pl.col(TS_COLUMNS[3]).is_not_null()
            & pl.all_horizontal(pl.col(ecr_null_cols).is_null())
        )
        ewr_df = eng_df.filter(  # EWR
            pl.col(TS_COLUMNS[4]).is_not_null()
            & pl.all_horizontal(pl.col(ewr_null_cols).is_null())
        )
        npr_df = eng_df.filter(  # NPR
            pl.col(TS_COLUMNS[5]).is_not_null()
            & pl.all_horizontal(pl.col(npr_null_cols).is_null())
        )
        mtg_df = eng_df.filter(  # Meetings
            pl.col(TS_COLUMNS[10]).is_not_null()
        )
        misc_df = eng_df.filter(  # Miscellaneous
            pl.all_horizontal(pl.col(misc_null_cols).is_null())
        )
        # Find the total time spent on the task from the appropriately filtered
        # DataFrame
        ecr_time = ecr_df.select(  # ECR
            pl.col(TS_COLUMNS[2])
        ).sum()[TS_COLUMNS[2]].to_list()[0]
        ewr_time = ewr_df.select(  # EWR
            pl.col(TS_COLUMNS[2])
        ).sum()[TS_COLUMNS[2]].to_list()[0]
        npr_time = npr_df.select(  # NPR
            pl.col(TS_COLUMNS[2])
        ).sum()[TS_COLUMNS[2]].to_list()[0]
        mtg_time = mtg_df.select(  # Meeting
            pl.col(TS_COLUMNS[2])
        ).sum()[TS_COLUMNS[2]].to_list()[0]
        misc_time = misc_df.select(  # Miscellaneous
            pl.col(TS_COLUMNS[2])
        ).sum()[TS_COLUMNS[2]].to_list()[0]
        # Build a Dictionary for the engineers time
        eng_time_dict = {
            "Engineer": eng_dict[eng],
            "ECR": ecr_time,
            "EWR": ewr_time,
            "NPR": npr_time,
            "Meetings": mtg_time,
            "Misc.": misc_time,
        }
        # Add dictionary to the list of dictionaries
        eng_time_dicts.append(eng_time_dict)

    # Create DataFrame from list of dictionaries
    stats_df = pl.from_dicts(eng_time_dicts)

    return stats_df
