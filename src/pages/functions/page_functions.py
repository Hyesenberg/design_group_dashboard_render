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
