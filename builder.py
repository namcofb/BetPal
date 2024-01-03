import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from data import SQLRepository



def build_status_pie_chart(df):

    bet_status_chart = df["Status"].value_counts().to_frame()

    fig = go.Figure(data=[
        go.Pie(labels=bet_status_chart.index,
            values=bet_status_chart["count"],
            hole=.3,
            title="Bet Status History"
            )
    ])

    return fig


def calculate_totals(df):
    # Calcute total wins
    mask_wins = df["Status"] == "Won"
    df_wins = df[mask_wins]
    Total_wins = df_wins["Total Return"].sum().round(2)

    # Calculate total lost
    mask_lost = df["Status"] == "Lost"
    df_lost = df[mask_lost]
    Total_lost = df_lost["Total Stake"].sum().round(2)

    # Calculate total stakes
    Total_staked = df["Total Stake"].sum().round(2)
    
    # Get Total bets placed
    Total_bets = len(df)

    #Create df to hold totals
    df_totals = {
        "Total bets placed": Total_bets,
        "Total Staked": Total_staked,
        "Total Won": Total_wins,
        "Total Lost": Total_lost
    }
    # Return totals
    return df_totals


def build_history_bar_chart(df):

    count_group = count_group = df["Status"].groupby(df.index).value_counts().rename("Count").reset_index()

    fig = px.bar(
        count_group, 
        x="Date", 
        y="Count", 
        color="Status", 
        title="Bet Status History"
    )

    fig.update_layout(xaxis_title="Date", yaxis_title="Bets")

    return fig