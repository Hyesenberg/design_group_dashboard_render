import polars as pl

global TS_COLUMNS
global TS_DTYPES

TS_COLUMNS = [
    "Date",
    "Engineer",
    "Time",
    "ECR",
    "EWR",
    "NPR",
    "NCR",
    "TR",
    "EN",
    "Model",
    "Meetings",
    "Other",
    "Comments",
]

TS_DTYPES = [
    pl.Date,
    pl.Utf8,
    pl.Float64,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
    pl.Utf8,
]
