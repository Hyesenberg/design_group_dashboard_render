#!python3.11

import dash_bootstrap_components as dbc


def create_navbar():
    navbar = dbc.NavbarSimple(
        children=[
            dbc.DropdownMenu(
                nav=True,
                in_navbar=True,
                label="Menu",
                align_end=True,
                children=[  # Add as many menu items as you need
                    dbc.DropdownMenuItem(
                        "Home",
                        href="/"
                    ),
                    dbc.DropdownMenuItem(
                        "Time Allocation",
                        href="/time-allocation"
                    ),
                    dbc.DropdownMenuItem(
                        "Task Specific Metrics",
                        href="/task-specific-metrics"
                    ),
                ],
            ),
        ],
        brand="Design Group Dashboard",
        brand_href="/",
        sticky="top",
        color="dark",
        dark=True,
        fluid=True,
    )

    return navbar
