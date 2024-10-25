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

        fantasy_team_builds, fantasy_results_tab, player_results_tab = st.tabs(["Fantasy Teams",
                                                                                "Fantasy Results",
                                                                                "Player Results"])

        with fantasy_team_builds:
            your_fantasy_team_tab, other_players_tab = st.tabs(["Your Fantasy Team", "Other Fantasy Teams"])
            with your_fantasy_team_tab:
                st.subheader("Previous fantasy teams:")
                st.dataframe(event_fantasy_teams.tail())

                st.subheader("Upcoming Series Teams:")
                selected_series = st.selectbox(label="Series:", options=upcoming_series["name"])
                if selected_series is not None:
                    allowed_tier = upcoming_series.row(by_predicate=(pl.col("name") == selected_series), named=True)
                    allowed_tier = allowed_tier["tier_lock"]
                    if allowed_tier is None:
                        allowed_tier_mask = (pl.col("tier").is_in(["T1", "T2", "T3", "T4", None, ""]))
                    else:
                        allowed_tier_mask = (pl.col("tier") == allowed_tier)

                    allowed_points = 15
                    spending_list = [0, 0, 0, 0, 0]
                    top_col, jng_col, mid_col, bot_col, sup_col = st.columns(5)
                    with top_col:
                        player_selection_df = player_cost.filter((pl.col("position") == "top") &
                                                                 (pl.col("playercost") <= (allowed_points - sum(spending_list))) &
                                                                 allowed_tier_mask)["playername",
                                                                                    "playercost"]
                        top_selection = st.selectbox(label="Top:",
                                                     options=hp.stringify_player_costs(player_selection_df))
                        if top_selection is not None:
                            spending_list[0] = int(top_selection[-2])

                    with jng_col:
                        player_selection_df = player_cost.filter((pl.col("position") == "jng") &
                                                                 (pl.col("playercost") <= (allowed_points - sum(spending_list))) &
                                                                 allowed_tier_mask)["playername",
                                                                                    "playercost"]
                        jng_selection = st.selectbox(label="Jng:",
                                                     options=hp.stringify_player_costs(player_selection_df))
                        if jng_selection is not None:
                            spending_list[1] = int(jng_selection[-2])
                    with mid_col:
                        player_selection_df = player_cost.filter((pl.col("position") == "mid") &
                                                                 (pl.col("playercost") <= (allowed_points - sum(spending_list))) &
                                                                 allowed_tier_mask)["playername",
                                                                                    "playercost"]
                        mid_selection = st.selectbox(label="mid:",
                                                     options=hp.stringify_player_costs(player_selection_df))
                        if mid_selection is not None:
                            spending_list[2] = int(mid_selection[-2])
                    with bot_col:
                        player_selection_df = player_cost.filter((pl.col("position") == "bot") &
                                                                 (pl.col("playercost") <= (allowed_points - sum(spending_list))) &
                                                                 allowed_tier_mask)["playername",
                                                                                    "playercost"]
                        bot_selection = st.selectbox(label="bot:",
                                                     options=hp.stringify_player_costs(player_selection_df))
                        if bot_selection is not None:
                            spending_list[3] = int(bot_selection[-2])
                    with sup_col:
                        player_selection_df = player_cost.filter((pl.col("position") == "sup") &
                                                                 (pl.col("playercost") <= (allowed_points - sum(spending_list))) &
                                                                 allowed_tier_mask)["playername",
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
                    elif selected_series is not None:
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
                results = hp.return_fantasy_teams_by_stage(os.path.abspath("data/"))
                st.dataframe(results, height=10000)

        with fantasy_results_tab:

            fantasy_results_placement, fantasy_results_over_series = st.tabs(["Current Placement",
                                                                              "Results over Series"])
            combined_results = hp.return_combined_results_of_each_owner(team_owners,
                                                                        schedule_df,
                                                                        match_data_df,
                                                                        settings_json)
            if combined_results is not None:
                with fantasy_results_placement:
                    fig = px.histogram(combined_results,
                                       x="owner",
                                       y="overall",
                                       text_auto=True,
                                       barmode="group",
                                       histfunc="sum")
                    fig.update_layout(xaxis=dict(autorange="reversed",
                                                 title="Players"),
                                      title="Each player scores")

                    st.plotly_chart(fig)

                with fantasy_results_over_series:
                    fig = px.bar(data_frame=combined_results,
                                 x="eventname",
                                 y="overall",
                                 color="owner",
                                 text="overall",
                                 barmode="group")
                    fig.update_layout(xaxis=dict(autorange="reversed",
                                                 title="Series"),
                                      title="Each player scores over series")
                    st.plotly_chart(fig)
            else:
                st.warning("No results yet.")

        with player_results_tab:
            filtered_matches_df = match_data_df.filter(pl.col("playername").is_in(player_cost["playername"]))
            filter_option = st.selectbox(label="Filter by:",
                                         options=["Stage",
                                                  "Date",
                                                  "Player",
                                                  "Position",
                                                  "Team",
                                                  "Tier"])

            match filter_option:
                case "Stage":
                    stage_option = st.selectbox(label="Stage to select:",
                                                options=schedule_df["name"])
                    stage_row = schedule_df.row(by_predicate=(pl.col("name") == stage_option),
                                                named=True)

                    filtered_matches_df = hp.return_filtered_matches(filtered_matches_df,
                                                                     stage_row["start_str"],
                                                                     stage_row["end_str"])
                case "Date":
                    schedule_start_date = st.date_input(label="Start date",
                                                        min_value=datetime.strptime("2024-09-25",
                                                                                    "%Y-%m-%d"),
                                                        max_value=datetime.strptime("2024-11-03", "%Y-%m-%d"))

                    schedule_end_date = st.date_input(label="End date",
                                                      min_value=schedule_start_date,
                                                      max_value=datetime.strptime("2024-11-03", "%Y-%m-%d"))
                    schedule_start_date = schedule_start_date.strftime("%Y-%m-%d")
                    schedule_end_date = schedule_end_date.strftime("%Y-%m-%d")
                    filtered_matches_df = hp.return_filtered_matches(filtered_matches_df,
                                                                     schedule_start_date,
                                                                     schedule_end_date)
                case "Player":
                    player_option = st.selectbox(label="Player to filter for:",
                                                 options=player_cost["playername"])
                    filtered_matches_df = filtered_matches_df.filter(pl.col("playername") == player_option)
                case "Position":
                    position_option = st.selectbox(label="Filter for position:",
                                                   options=["top", "jng", "mid", "bot", "sup"])
                    filtered_matches_df = filtered_matches_df.filter(pl.col("position") == position_option)
                case "Team":
                    teams = set(filtered_matches_df["teamname"].to_list())
                    team_option = st.selectbox(label="Team to filter for:",
                                               options=teams)
                    filtered_matches_df = filtered_matches_df.filter(pl.col("teamname") == team_option)
                case "Tier":
                    tier_option = st.selectbox(label="Tier to filter for:",
                                               options=player_cost["tier"])
                    filtered_players_df = player_cost.filter(pl.col("tier") == tier_option)
                    filtered_matches_df = filtered_matches_df.filter(pl.col("playername").is_in(filtered_players_df["playername"]))

            performance_data = hp.calculate_performance(filtered_matches_df,
                                                        settings_json["multipliers"])
            filtered_matches_df = filtered_matches_df.with_columns(performance=performance_data["performance"])

            raw_data_tab, visual_data_tab = st.tabs(["Raw Data", "Visuals"])

            with raw_data_tab:
                st.dataframe(filtered_matches_df)

            with visual_data_tab:
                column_option = st.selectbox(label="What data to show:",
                                             options=["kills",
                                                      "deaths",
                                                      "assists",
                                                      "firstblood",
                                                      "barons",
                                                      "damagetochampions",
                                                      "dpm",
                                                      "visionscore",
                                                      "total cs",
                                                      "golddiffat10",
                                                      "golddiffat20",
                                                      "performance"])
                base_tab, trend_tab = st.tabs(["Basic", "Trend"])
                with base_tab:
                    fig = px.scatter(data_frame=filtered_matches_df,
                                     x="date",
                                     y=column_option,
                                     color="playername",
                                     hover_data=["performance",
                                                 "gameid",
                                                 "result",
                                                 "side"])
                    st.plotly_chart(fig)
                with trend_tab:
                    check_win = st.checkbox(label="Result(win/loss) as Y column")
                    y_col = "result" if check_win else "performance"
                    fig = px.scatter(data_frame=filtered_matches_df,
                                     y=y_col,
                                     x=column_option,
                                     color="playername",
                                     hover_data=["performance",
                                                 "gameid",
                                                 "result",
                                                 "side"],
                                     trendline="ols")
                    st.plotly_chart(fig)

