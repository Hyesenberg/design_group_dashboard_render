#!python3.11

from dash import html, dcc, callback, Output, Input, register_page
from dash_bootstrap_templates import load_figure_template
from datetime import date
import datetime as dt
import plotly.express as px
from .functions import page_functions as pfu
from .functions import time_allocation_functions as tafu
from dotenv import load_dotenv
import os

load_dotenv()
NAMESPACE = os.environ.get("NAMESPACE")
DATABASE = os.environ.get("DATABASE")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
TS_TABLE = os.environ.get("TS_TABLE")

load_figure_template("darkly")

register_page(
    __name__,
    name="Time Allocation",
    top_nav=True,
    path="/time-allocation",
)

layout = html.Div([
    html.H1(children="Division of Labor", style={"textAlign": "center"}),
    html.Div(
        children=[
            dcc.DatePickerRange(
                id="allocation-date-picker-range",
                min_date_allowed=date(2021, 1, 1),
                max_date_allowed=dt.datetime.now().date(),
                initial_visible_month=dt.datetime.now().date(),
                end_date=dt.datetime.now().date(),
                start_date=dt.datetime.now().date() - dt.timedelta(weeks=2),
            )
        ],
        className="dash-bootstrap",
        style={"display": "flex", "justifyContent": "center"},
    ),
    dcc.Graph(id="graph-content", className="m-4")
])


@callback(
    Output("graph-content", "figure"),
    Input("allocation-date-picker-range", "start_date"),
    Input("allocation-date-picker-range", "end_date")
)
def update_graph_content(start_date, end_date):
    if not start_date or not end_date:  # Either date is not entered
        fig = px.bar(  # Load Page with Empty Bar Graph
            template="darkly",
        )

        fig.update_layout(
            title="Division of Labor",
            xaxis_title="Engineer",
            yaxis_title="Hours",
        )

        return fig
    else:
        start_date_object = date.fromisoformat(start_date)
        start_date_str = start_date_object.strftime('%Y-%m-%d')
        end_date_object = date.fromisoformat(end_date)
        end_date_str = end_date_object.strftime('%Y-%m-%d')
        df = pfu.query_ts_table_between_dates(start_date_str, end_date_str)

        stats_df = tafu.find_task_type_hours(df)
        eng_time_dict = stats_df.to_dict()

        fig = px.bar(
            stats_df,
            x="Engineer",
            y=list(eng_time_dict.keys())[1:],
            template="darkly",
        )

        fig.update_layout(
            title="Division of Labor",
            xaxis_title="Engineer",
            yaxis_title="Hours",
        )

        return fig
