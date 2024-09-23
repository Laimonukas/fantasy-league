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
    event_fantasy_teams = hp.return_event_selection(os.path.abspath(f"data/teams/{st.session_state['name']}_teams.csv"))

    team_owners = hp.return_only_team_owners(os.path.abspath("data/logins.csv"))

    none_list = [v is None for v in [settings_json,
                                     match_data_df,
                                     player_performance_df,
                                     schedule_df,
                                     players_df,
                                     player_cost,
                                     event_fantasy_teams,
                                     team_owners]]
    if none_list.count(True) >= 1:
        st.warning("Something is wrong, not all data loaded in.")
    else:
        upcoming_series = schedule_df.filter(pl.col("locked") == False)


        fantasy_team_builds, fantasy_results_tab = st.tabs(["Fantasy Teams", "Fantasy Results"])

        with fantasy_team_builds:
            your_fantasy_team_tab, other_players_tab = st.tabs(["Your Fantasy Team", "Other Fantasy Teams"])
            with your_fantasy_team_tab:
                st.subheader("Previous fantasy teams:")
                st.dataframe(event_fantasy_teams)

                st.subheader("Upcoming Series Teams:")
                selected_series = st.selectbox(label="Series:", options=upcoming_series["name"])

                allowed_points = 15
                spending_list = [0, 0, 0, 0, 0]
                top_col, jng_col, mid_col, bot_col, sup_col = st.columns(5)
                with top_col:
                    player_selection_df = player_cost.filter((pl.col("position") == "top") &
                                                             (pl.col("playercost") <= (allowed_points - sum(spending_list))))["playername",
                                                                                                                              "playercost"]
                    top_selection = st.selectbox(label="Top:",
                                                 options=hp.stringify_player_costs(player_selection_df))
                    if top_selection is not None:
                        spending_list[0] = int(top_selection[-2])

                with jng_col:
                    player_selection_df = player_cost.filter((pl.col("position") == "jng") &
                                                             (pl.col("playercost") <= (allowed_points - sum(spending_list))))["playername",
                                                                                                                              "playercost"]
                    jng_selection = st.selectbox(label="Jng:",
                                                 options=hp.stringify_player_costs(player_selection_df))
                    if jng_selection is not None:
                        spending_list[1] = int(jng_selection[-2])
                with mid_col:
                    player_selection_df = player_cost.filter((pl.col("position") == "mid") &
                                                             (pl.col("playercost") <= (allowed_points - sum(spending_list))))["playername",
                                                                                                                              "playercost"]
                    mid_selection = st.selectbox(label="mid:",
                                                 options=hp.stringify_player_costs(player_selection_df))
                    if mid_selection is not None:
                        spending_list[2] = int(mid_selection[-2])
                with bot_col:
                    player_selection_df = player_cost.filter((pl.col("position") == "bot") &
                                                             (pl.col("playercost") <= (allowed_points - sum(spending_list))))["playername",
                                                                                                                              "playercost"]
                    bot_selection = st.selectbox(label="bot:",
                                                 options=hp.stringify_player_costs(player_selection_df))
                    if bot_selection is not None:
                        spending_list[3] = int(bot_selection[-2])
                with sup_col:
                    player_selection_df = player_cost.filter((pl.col("position") == "sup") &
                                                             (pl.col("playercost") <= (allowed_points - sum(spending_list))))["playername",
                                                                                                                              "playercost"]
                    sup_selection = st.selectbox(label="sup:",
                                                 options=hp.stringify_player_costs(player_selection_df))
                    if sup_selection is not None:
                        spending_list[4] = int(sup_selection[-2])

                st.text(f"Remaining points: {allowed_points - sum(spending_list)}")

                mult_col, mult_player_col = st.columns(2)

                with mult_col:
                    mult_selection = st.selectbox(label="Modifier:",
                                                  options=settings_json["modifiers"].keys())

                with mult_player_col:
                    if [v is None for v in [top_selection,
                                            jng_selection,
                                            mid_selection,
                                            bot_selection,
                                            sup_selection]].count(True) > 0:
                        st.warning("Not all positions filled for multiplier")
                    else:
                        mult_player_selection = st.selectbox(label="For player:",
                                                             options=[top_selection[:-4],
                                                                      jng_selection[:-4],
                                                                      mid_selection[:-4],
                                                                      bot_selection[:-4],
                                                                      sup_selection[:-4],
                                                                      None])

                if [v is None for v in [top_selection,
                                        jng_selection,
                                        mid_selection,
                                        bot_selection,
                                        sup_selection]].count(True) > 0:
                    st.warning("Not all positions filled, can't save.")
                else:
                    if st.button(label="Save selection"):

                        new_event_row_df = pl.DataFrame(data=[[selected_series,
                                                               top_selection[:-4],
                                                               jng_selection[:-4],
                                                               mid_selection[:-4],
                                                               bot_selection[:-4],
                                                               sup_selection[:-4],
                                                               mult_selection,
                                                               mult_player_selection]],
                                                        schema={"eventname": pl.String,
                                                                "top": pl.String,
                                                                "jng": pl.String,
                                                                "mid": pl.String,
                                                                "bot": pl.String,
                                                                "sup": pl.String,
                                                                "modifier": pl.String,
                                                                "player": pl.String})

                        event_fantasy_teams = event_fantasy_teams.filter(pl.col("eventname") != selected_series)
                        event_fantasy_teams = event_fantasy_teams.vstack(new_event_row_df)
                        event_fantasy_teams.write_csv(os.path.abspath(f"data/teams/{st.session_state['name']}_teams.csv"))
                        st.rerun()

            with other_players_tab:
                team_owner_selection = st.selectbox(label="Select player",
                                                    options=team_owners)
                st.dataframe(hp.return_event_selection(os.path.abspath(f"data/teams/{team_owner_selection}_teams.csv")))

        with fantasy_results_tab:

            fantasy_results_placement, fantasy_results_over_time = st.tabs(["Current Placement",
                                                                            "Over time"])

            with fantasy_results_placement:
                event_dict = hp.event_matches_dictionary(schedule_df,
                                                         match_data_df)

                score = hp.score_for_specific_team(event_fantasy_teams.row(1, named=True),
                                                   event_dict,
                                                   settings_json["multipliers"],
                                                   settings_json["modifiers"])
                st.dataframe(score)
