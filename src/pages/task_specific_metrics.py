#!python3.11

from dash.exceptions import PreventUpdate
from dash import html, dcc, callback, Output, Input, register_page
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import load_figure_template
import polars as pl
import datetime as dt
import plotly.express as px
from .functions import page_functions as pfu
from .functions import task_specific_metrics_functions as tsmfu
from dotenv import load_dotenv
import os
from io import StringIO
import json

load_dotenv()
NAMESPACE = os.environ.get("NAMESPACE")
DATABASE = os.environ.get("DATABASE")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
TS_TABLE = os.environ.get("TS_TABLE")

load_figure_template("darkly")

register_page(
    __name__,
    name="Task Specific Metrics",
    top_nav=True,
    path="/task-specific-metrics",
)
# Query all of timesheet entries
df = pfu.query_ts_table(f"SELECT * FROM {TS_TABLE}")
json_data = df.write_json()  # Convert to JSON to use in dcc.Store object


layout = html.Div([
    dcc.Store(id="df-store", data=json_data),
    dbc.Row(
        [
            dbc.Col(
                html.Div(
                    children=[
                        html.H1("Task Specific Metrics"),
                        dcc.Dropdown(
                            ["ECR", "EWR", "NPR", "Model", "Meetings"],
                            placeholder="Select Task Type",
                            id="task-type-dropdown",
                            style={"margin-bottom": "15px"},
                        ),
                        dcc.Dropdown(
                            placeholder="Select Task Number",
                            id="task-numbers-dropdown",
                            value=[],
                            style={"margin-bottom": "15px"},
                            multi=True,
                        ),
                        dcc.DatePickerRange(
                            id="task-date-picker-range",
                        ),
                        html.H3("Time Frame Grouping"),
                        dbc.RadioItems(
                            options=[
                                {"label": "Daily", "value": "1d"},
                                {"label": "Weekly", "value": "1w"},
                                {"label": "Monthly", "value": "1mo"},
                            ],
                            inline=True,
                            id="date-grouping-radioitems",
                        ),
                    ],
                    className="dash-bootstrap",
                    style={
                        "display": "inline-block",
                        "justifyContent": "left"
                    },
                ),
                width=3,
            ),
            dbc.Col(
                html.Div(
                    children=[
                        html.H1("Results", id="results"),
                        html.H3(id="dpmt-total"),
                        html.H4(id="andre-total"),
                        html.H4(id="jacob-total"),
                        html.H4(id="josiah-total"),
                        html.H4(id="michael-total"),
                    ],
                    className="dash-bootstrap",
                    style={
                        "display": "inline-block",
                        "justifyContent": "left"
                    },
                ),
            )
        ]
    ),
    dcc.Graph(
        id="task-graph",
        figure=px.bar(  # Load Page with Empty Bar Graph
            template="darkly",
            title="Task Workflow",
        )
    )
])


@callback(  # Populates Task Numbers Dropdown
    Output("task-numbers-dropdown", "options"),
    Input("task-type-dropdown", "value"),
    Input("df-store", "data"),
)
def find_task_specific_projects(value, data):
    if not value:  # No value selected from task type dropdown
        return []
    else:  # Selected a task type
        df = pl.read_json(StringIO(data))  # Create a DataFrame again
        tasks = tsmfu.find_unique_tasks(df, value)  # Find unique tasks

        return tasks  # Populate second dropdown


@callback(  # Populates Date Range and Radio Buttons
    Output("task-date-picker-range", "start_date"),
    Output("task-date-picker-range", "min_date_allowed"),
    Output("task-date-picker-range", "end_date"),
    Output("task-date-picker-range", "max_date_allowed"),
    Output("date-grouping-radioitems", "value"),
    Input("task-type-dropdown", "value"),
    Input("task-numbers-dropdown", "value"),
    Input("df-store", "data"),
)
def populate_date_picker_range(task_type, task_numbers, data):
    if not task_type or len(task_numbers) == 0:
        return (None, None, None, None, None)
    else:  # Selected a task type
        df = pl.read_json(StringIO(data))  # Create a DataFrame again
        start_date, end_date, date_grouping = tsmfu.find_task_dates(
            df, task_type, task_numbers
        )

        return (
            start_date,
            start_date,
            end_date,
            end_date,
            date_grouping,
        )


@callback(
    Output("task-graph", "figure"),
    Output("results", "children", allow_duplicate=True),
    Output("dpmt-total", "children", allow_duplicate=True),
    Output("andre-total", "children", allow_duplicate=True),
    Output("jacob-total", "children", allow_duplicate=True),
    Output("josiah-total", "children", allow_duplicate=True),
    Output("michael-total", "children", allow_duplicate=True),
    Input("task-type-dropdown", "value"),
    Input("task-numbers-dropdown", "value"),
    Input("task-date-picker-range", "start_date"),
    Input("task-date-picker-range", "end_date"),
    Input("date-grouping-radioitems", "value"),
    Input("df-store", "data"),
    prevent_initial_call=True,
)
def create_task_graph(
    task_type,
    task_numbers,
    start_date,
    end_date,
    date_grouping,
    data,
):
    # No value selected from task type dropdown
    if not task_type or len(task_numbers) == 0:
        fig = px.bar(  # Load Page with Empty Bar Graph
            template="darkly",
            title="Task Workflow",
        )
        return fig, "Results", "", "", "", "", ""

    else:
        df = pl.read_json(StringIO(data))
        start_date_object = dt.date.fromisoformat(start_date)
        end_date_object = dt.date.fromisoformat(end_date)

        stats_df, time_groups = tsmfu.task_specific_metrics(
            df,
            task_type,
            task_numbers,
            start_date_object,
            end_date_object,
            date_grouping,
        )

        fig = px.bar(
            stats_df,
            x="Date",
            y=stats_df.columns[1:],
            template="darkly",
        )

        x_label_dict = {"1d": "Days", "1w": "Weeks", "1mo": "Months"}

        fig.update_layout(
            title="Task Workflow",
            xaxis_title=x_label_dict[time_groups],
            yaxis_title="Hours",
        )

        totals_dict = tsmfu.build_totals_dict(stats_df)

        start_date_str = start_date_object.strftime("%m/%d/%Y")
        end_date_str = end_date_object.strftime("%m/%d/%Y")

        return (
            fig,
            f"Results: {start_date_str} to {end_date_str}",
            f"Department Total: {totals_dict['Department']} Hours",
            f"Andre: {totals_dict['Andre']} Hours",
            f"Jacob: {totals_dict['Jacob']} Hours",
            f"Josiah: {totals_dict['Josiah']} Hours",
            f"Michael: {totals_dict['Michael']} Hours",
        )


@callback(
    Output("results", "children"),
    Output("dpmt-total", "children"),
    Output("andre-total", "children"),
    Output("jacob-total", "children"),
    Output("josiah-total", "children"),
    Output("michael-total", "children"),
    Input("task-graph", "relayoutData"),
    Input("task-type-dropdown", "value"),
    Input("task-numbers-dropdown", "value"),
    Input("date-grouping-radioitems", "value"),
    Input("task-date-picker-range", "start_date"),
    Input("task-date-picker-range", "end_date"),
    Input("df-store", "data"),
)
def update_totals_calc(
    graph_data,
    task_type,
    task_numbers,
    date_grouping,
    dd_start_date,
    dd_end_date,
    df_data,
):
    # There was no modification to the graph
    if graph_data is None or task_type is None or len(task_numbers) == 0:
        raise PreventUpdate
    else:  # Graph Layout Modified
        # Booleans to know if we can move on at the end
        start_date = False
        end_date = False
        # Extract Data
        json_str = json.dumps(graph_data)
        layout_data = json.loads(json_str)
        # Verify that the x axis was scaled
        # Scaled by user
        if list(layout_data.keys())[0] == "xaxis.range[0]":
            # Extract start and end of scaling
            start_date_data = layout_data["xaxis.range[0]"][0:10]
            start_date = dt.datetime.strptime(
                start_date_data,
                "%Y-%m-%d"
            ).date()
            end_date_data = layout_data["xaxis.range[1]"][0:10]
            end_date = dt.datetime.strptime(
                end_date_data,
                "%Y-%m-%d"
            ).date()
        # Autoscaled
        elif list(layout_data.keys())[0] == "xaxis.autorange":
            start_date = dt.date.fromisoformat(dd_start_date)
            end_date = dt.date.fromisoformat(dd_end_date)
        # Axis not scaled, Prevent Update
        else:
            raise PreventUpdate

        if start_date and end_date:

            start_date_str = start_date.strftime("%m/%d/%Y")
            end_date_str = end_date.strftime("%m/%d/%Y")
            df = pl.read_json(StringIO(df_data))  # Create a DataFrame again

            stats_df, time_groups = tsmfu.task_specific_metrics(
                df,
                task_type,
                task_numbers,
                start_date,
                end_date,
                date_grouping,
            )

            totals_dict = tsmfu.build_totals_dict(stats_df)

            return (
                f"Results: {start_date_str} to {end_date_str}",
                f"Department Total: {totals_dict['Department']} Hours",
                f"Andre: {totals_dict['Andre']} Hours",
                f"Jacob: {totals_dict['Jacob']} Hours",
                f"Josiah: {totals_dict['Josiah']} Hours",
                f"Michael: {totals_dict['Michael']} Hours",
            )
