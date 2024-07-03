#!python3.11

import os
import polars as pl
import datetime as dt
from dotenv import load_dotenv
from .global_vars import TS_COLUMNS

load_dotenv()
DATABASE = os.environ.get("DATABASE")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
DB_HOST = os.environ.get("DB_HOST")
DB_PORT = os.environ.get("DB_PORT")
TS_TABLE = os.environ.get("TS_TABLE")


def find_unique_tasks(
    df: pl.DataFrame,
    task_type: str,
) -> list[str]:
    """
    Function that finds the unique task numbers in a DataFrame
    :param df: Output from page_functions.query_ts_table
    :param task_type: "ECR", "EWR", or "NPR"
    return tasks: List of unique task numbers in the DataFrame provided
    """
    tasks = df.select(pl.col(task_type).str.to_uppercase()).unique()
    tasks = tasks.filter(
        ~pl.col(task_type).is_in(["", " "])
    )
    tasks = tasks[task_type].drop_nulls().sort(descending=True).to_list()
    return tasks


def find_task_dates(
    df: pl.DataFrame,
    task_type: str,
    task_numbers: list[str],
) -> (dt.date, dt.date, str):
    """
    Function that returns the first and last date for a task in a DataFrame of
    timesheets.
    :param df: Output from page_functions.query_ts_table
    :param task_type: "ECR", "EWR", "NPR", "Model"
    :param task_numbers: List of strings of numbers representing specific task
    return start_date: dt.date object representing the first date of task
    return end_date: dt.date object representing the last date of task
    return date_grouping: str containing how the dates should be grouped
    """
    # Filter to task
    filtered_df = df.filter(
        pl.col(task_type).str.to_uppercase().is_in(task_numbers)
    )
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
        return None, None, "1mo"

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
    task_numbers: list[str],
    start_date: dt.date,
    end_date: dt.date,
    date_grouping: str,
) -> (pl.DataFrame, str):
    """
    Function that finds the task specific metrics from a DataFrame
    :param df: Output from page_functions.query_ts_table
    :param task_type: "ECR", "EWR", "NPR", "Models"
    :param task_numbers: List of strings of numbers representing specific task
    :param start_date: dt.date representing the start of the period
    :param end_date: dt.date representing the end of the period
    :param date_grouping: str representing the grouping for dates
    return stats_df: DataFrame containing the time each engineer spent on each
                     task.
    return date_grouping: Str representing how the dates are grouped
    """
    filtered_df = df.filter(  # Filter to task
        (pl.col(task_type).str.to_uppercase().is_in(task_numbers))
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


def build_totals_dict(stats_df: pl.DataFrame) -> dict:
    """
    Function to calculate the totals from the stats_df
    :param stats_df: Output from task_specific_metrics
    return totals_dict: Dictionary containing total hours worked by department
                        and each engineer
    """

    totals_df = stats_df.select(
        pl.col(stats_df.columns[1:])
    ).sum()

    totals_dict = {
        "Department": 0,
        "Andre": 0,
        "Jacob": 0,
        "Josiah": 0,
        "Michael": 0
    }

    for col in totals_df.columns:
        totals_dict[col] = totals_df.select(pl.col(col))[col][0]

    totals_dict["Department"] = totals_df.sum_horizontal().to_list()[0]

    return totals_dict
