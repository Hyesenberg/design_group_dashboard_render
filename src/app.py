#!python3.11

from dash import Dash, html, page_container
import dash_bootstrap_components as dbc
from navbar import create_navbar

NAVBAR = create_navbar()
APP_TITLE = "Design Group Dashboard"

dbc_css = (
    "https://cdn.jsdelivr.net/gh/AnnMarieW/dash-bootstrap-templates@V1.0.2/dbc.min.css"
)

app = Dash(
    __name__,
    suppress_callback_exceptions=True,
    external_stylesheets=[
        dbc.themes.SUPERHERO,  # Dash Themes CSS
        dbc_css,
    ],
    title=APP_TITLE,
    use_pages=True,
)

server = app.server

app.layout = html.Div(
    children=[
        NAVBAR,
        html.Br(),
        page_container,
    ],
    className="dbc dbc-ag-grid",
)


if __name__ == '__main__':
    app.run(
        # debug=True,
        host="0.0.0.0"
    )
