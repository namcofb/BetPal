import sqlite3
import plotly.express as px
import pandas as pd

from dash import Dash, dcc, html, Input, Output
from data import SQLRepository
from builder import build_status_pie_chart, build_history_bar_chart, calculate_totals


# Connect to SQLRepository
conn = "bet_hist.db"
repo = SQLRepository(connection=conn)

# Get list of existing tables from db and append `ALLTIME` option to merge all tables
tables = pd.read_sql_query(sql="SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name", con=repo.connection)
if len(tables) <= 1:
    upd_tables = tables.squeeze()
else:
    upd_tables = tables.squeeze().tolist()
    upd_tables.append("ALLTIME")

# Instantiate our Dash app
app = Dash(__name__)

app.layout = html.Div(
    [
        html.H1("Welcome to your Dashboard!"),
        html.P("Choose a month:"),
        dcc.Dropdown(upd_tables, value=upd_tables[0], id="month-dropdown"),
        dcc.Graph(id="pie-chart"),
        html.Div(
            [
                html.H2("We see you've been busy!"),
                html.P("Here's a summary of your bets placed so far:"),
                html.Div(id="totals-text")
        ]   
        ),
        dcc.Graph(id="bar-chart")
    ]
)

@app.callback(
    Output("pie-chart", "figure"),
    Input("month-dropdown", "value")    
)
def serve_pie_chart(value):
    if value == "ALLTIME":
        df = repo.read_table(table_name="null", merge=True)
        fig = build_status_pie_chart(df=df)
    else:
        df = repo.read_table(table_name=value)
        fig = build_status_pie_chart(df=df)
    # Return figure   
    return fig

@app.callback(
    Output("totals-text", "children"),
    Input("month-dropdown", "value")
)
def serve_totals(value):
    if value == "ALLTIME":
        df = repo.read_table(table_name="null", merge=True)
        totals = calculate_totals(df=df)
    else:
        df = repo.read_table(table_name=value)
        totals = calculate_totals(df=df)
    # Present dictionary in readable format
    text = [
        html.H3(f"Total bets placed: {totals['Total bets placed']}"),
        html.H3(f"Total Staked: N{totals['Total Staked']}"),
        html.H3(f"Total Won: N{totals['Total Won']}"),
        html.H3(f"Total Lost: N{totals['Total Lost']}")
    ]
    return text


@app.callback(
    Output("bar-chart", "figure"),
    Input("month-dropdown", "value")    
)
def serve_hist_bar_chart(value):
    if value == "ALLTIME":
        df = repo.read_table(table_name="null", merge=True)
        fig = build_history_bar_chart(df=df)
    else:
        df = repo.read_table(table_name=value)
        fig = build_history_bar_chart(df=df)
    # Return figure  
    fig.update_layout(title=f"Bet Stat History ({value})") 
    return fig





app.run_server(debug=True)