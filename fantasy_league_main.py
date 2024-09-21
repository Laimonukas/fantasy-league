import os
from datetime import datetime

import streamlit as st
import polars as pl
import plotly.express as px

import scripts.helper_functions as hp

if "logged_in" not in st.session_state:
    st.warning("Please login, use sidebar")
    st.page_link("pages/1_login_page.py")
else:
    st.header(f"Main - {st.session_state['name']}")
    settings_json = hp.return_settings_data(os.path.abspath("data/settings.json"))

    match_data_df = hp.return_match_data(os.path.abspath("data/match_data.csv"))
    player_performance_df = hp.calculate_performance(match_data_df,
                                                     settings_json["multipliers"])
    avg_performance_df = hp.return_avg_performance(os.path.abspath("data/avg_performance.csv"),
                                                   player_performance_df)
    schedule_df = hp.read_schedule(os.path.abspath("data/schedule.csv"))
    players_df = hp.read_players(os.path.abspath("data/players.csv"))
    player_cost = hp.return_player_pricing(os.path.abspath("data/players_cost.csv"),
                                           players_df)
    none_list = [v is None for v in [settings_json,
                                     match_data_df,
                                     player_performance_df,
                                     schedule_df,
                                     players_df,
                                     player_cost]]
    if none_list.count(True) >= 1:
        st.warning("Something is wrong, not all data loaded in.")
    else:

        enabled_filter = schedule_df.filter(pl.col("locked") == False)