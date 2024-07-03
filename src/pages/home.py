#!python3.11

import dash
from dash import html, register_page
import dash_bootstrap_components as dbc
from dotenv import load_dotenv
import os

load_dotenv()
NAMESPACE = os.environ.get("NAMESPACE")
DATABASE = os.environ.get("DATABASE")
DB_USERNAME = os.environ.get("DB_USERNAME")
DB_PASSWORD = os.environ.get("DB_PASSWORD")
TS_TABLE = os.environ.get("TS_TABLE")

register_page(
    __name__,
    name="Home",
    top_nav=True,
    path="/")


layout = html.Div([
    html.H1(children="The Team", style={"textAlign": "center"}),
    dbc.Row(
        [
            dbc.Col(
                html.Div(
                    children=[
                        html.Img(
                            src=dash.get_asset_url("Andre Shahinian.jpg"),
                            style={"height": "200px"}
                        ),
                        html.Br(),
                        html.Strong("Andre Shahinian"),
                    ],
                    style={"textAlign": "center"}
                ),
                width=3,
            ),
            dbc.Col(
                html.Div(
                    children=[
                        html.Img(
                            src=dash.get_asset_url("Jacob Barron.jpg"),
                            style={"height": "200px"}
                        ),
                        html.Br(),
                        html.Strong("Jacob Barron"),
                    ],
                    style={"textAlign": "center"}
                ),
                width=3,
            ),
        ],
        justify="center",
    ),
    html.Br(),
    html.Br(),
    dbc.Row(
        [
            dbc.Col(
                html.Div(
                    children=[
                        html.Img(
                            src=dash.get_asset_url("Josiah Torres.jpg"),
                            style={"height": "200px"}
                        ),
                        html.Br(),
                        html.Strong("Josiah Torres"),
                    ],
                    style={"textAlign": "center"},
                ),
                width=3,
            ),
            dbc.Col(
                html.Div(
                    children=[
                        html.Img(
                            src=dash.get_asset_url("Michael Alpert.jpg"),
                            style={"height": "200px"}
                        ),
                        html.Br(),
                        html.Strong("Michael Alpert"),
                    ],
                    style={"textAlign": "center"}
                ),
                width=3,
            ),
        ],
        justify="center",
    )
])
