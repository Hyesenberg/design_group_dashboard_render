#!python3.11

import os
import polars as pl
import datetime as dt
from dotenv import load_dotenv
from .context import TS_COLUMNS

load_dotenv()
DATABASE = os.environ.get("DATABASE")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
TS_TABLE = os.environ.get("TS_TABLE")


def query_ts_table(
    q_string: str
) -> pl.DataFrame:
    """
    Function that will query a timesheet_entries table and store to a DataFrame
    :param db: Output from connect_to_database()
    :param q_string: String with the desired Query for the DataBase
    return df: Polars DataFrame containing the output from the query
    """
    # Build Connection String
    conn_string = f"{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DATABASE}"
    uri = "postgresql://" + conn_string

    df = pl.read_database_uri(q_string, uri)  # Read Data
    df = df.sort(pl.col(TS_COLUMNS[0]))  # Sort by Date

    return df


def query_ts_table_between_dates(
    start_date: str,
    end_date: str
) -> pl.DataFrame:
    """
    Function that will query the timesheet table for all data between a given
    start_date and end_date.
    :param start_date: String for start date "%Y-%m-%d"
    :param end_date: String for end date "%Y-%m-%d"
    return df: Polars DataFrame containing data from timesheet_entries between
               start_date and end_date
    """
    # Build Query String
    q_string = f'SELECT * FROM {TS_TABLE} WHERE '  # Select from table
    # Date is greater than or equal to start date
    q_string = q_string + f'"Date" >= \'{start_date}\' and '
    # Date is less than end date
    q_string = q_string + f'"Date" < \'{end_date}\''
    df = query_ts_table(q_string)

    return df


def find_task_type_hours(df: pl.DataFrame):
    """
    Function that takes the output from query_ts_table and returns a DataFrame
    with 5 columns (Engineer, ECR, EWR, NPR, Meetings, Miscellaneous)
    :param df: Output from query_ts_table
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


def find_unique_tasks(
    df: pl.DataFrame,
    task_type: str,
) -> list[str]:
    """
    Function that finds the unique task numbers in a DataFrame
    :param df: Output from query_ts_table
    :param task_type: "ECR", "EWR", or "NPR"
    return tasks: List of unique task numbers in the DataFrame provided
    """
    tasks = df.select([task_type]).unique()[task_type]
    tasks = tasks.drop_nulls().sort(descending=True).to_list()
    return tasks


def find_task_dates(
    df: pl.DataFrame,
    task_type: str,
    task_number: str,
) -> (dt.date, dt.date, str):
    """
    Function that returns the first and last date for a task in a DataFrame of
    timesheets.
    :param df: Output from query_ts_table
    :param task_type: "ECR", "EWR", or "NPR"
    :param task_number: String of Number representing specific task
    return start_date: dt.date object representing the first date of task
    return end_date: dt.date object representing the last date of task
    return date_grouping: str containing how the dates should be grouped
    """
    filtered_df = df.filter(pl.col(task_type) == task_number)  # Filter to task
    # Find the dates present
    dates = filtered_df.select(pl.col(TS_COLUMNS[0])).to_series()
    start_date = dates.dt.min()  # Find min date
    end_date = dates.dt.max()  # Find max date

    failed = False
    # If you change the task type without clearing the project, this operation
    # returns a TypeError because the dates are None
    try:
        total_completion_time = end_date - start_date
    except TypeError:
        failed = True

    if failed:  # Got a type error
        # Dummy return to keep the program running
        return dt.date(2023, 2, 15), dt.date(2024, 2, 15), "1mo"

    # Didn't fail, get to finish the function
    if total_completion_time < dt.timedelta(weeks=15):
        date_grouping = "1d"
    elif total_completion_time < dt.timedelta(weeks=40):
        date_grouping = "1w"
    else:
        date_grouping = "1mo"

    return start_date, end_date, date_grouping


def task_specific_metrics(
    df: pl.DataFrame,
    task_type: str,
    task_number: str,
    start_date: dt.date,
    end_date: dt.date,
    date_grouping: str,
) -> (pl.DataFrame, str):
    """
    Function that finds the task specific metrics from a DataFrame
    :param df: Output from query_ts_table
    :param task_type: "ECR", "EWR", or "NPR"
    :param task_number: String of Number representing specific task
    :param start_date: dt.date representing the start of the period
    :param end_date: dt.date representing the end of the period
    :param date_grouping: str representing the grouping for dates
    return stats_df: DataFrame containing the time each engineer spent on each
                     task.
    return date_grouping: Str representing how the dates are grouped
    """
    filtered_df = df.filter(  # Filter to task
        (pl.col(task_type).is_in([task_number]))
        & (pl.col(TS_COLUMNS[0]).is_between(start_date, end_date))
    )

    grouped_df = filtered_df.group_by_dynamic(
        pl.col(TS_COLUMNS[0]),  # Group the date column
        every=date_grouping,  # Into months
        group_by=TS_COLUMNS[1]  # Group by engineer
    ).agg(
        pl.col(TS_COLUMNS[2]).sum()  # Take the sum of time
    ).select(pl.col(TS_COLUMNS[0:3]))  # Re-order columns to match filtered_df

    # Pivot into stats df
    stats_df = grouped_df.pivot(
        index=TS_COLUMNS[0],
        columns=TS_COLUMNS[1],
        values=TS_COLUMNS[2],
        aggregate_function=None,
    )

    num_groups = len(stats_df)
    rename_dict = {
        TS_COLUMNS[0]: TS_COLUMNS[0],
        "ashahinian": "Andre",
        "jbarron": "Jacob",
        "jtorres": "Josiah",
        "malpert": "Michael",
    }

    for key in rename_dict.keys():
        if key not in stats_df.columns:
            stats_df = stats_df.with_columns(
                pl.zeros(num_groups).alias(key),
            )

    stats_df = stats_df.rename(rename_dict).select(
        pl.col(rename_dict.values()))

    return stats_df, date_grouping
