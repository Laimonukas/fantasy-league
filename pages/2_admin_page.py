import streamlit as st
import os
import polars as pl
from datetime import datetime
import scripts.helper_functions as hp

if "logged_in" not in st.session_state or ("is_admin" not in st.session_state or st.session_state["is_admin"] is False):
    st.warning("You are not logged in or unauthorized to use this")
    st.page_link("pages/1_login_page.py")
else:
    st.header(f"Admin page - {st.session_state['name']}")
    login_df = hp.read_login(os.path.abspath("data/logins.csv"))
    user_tab, event_tab, session_state_tab, match_data_tab = st.tabs(["User data",
                                                                      "Event data",
                                                                      "Session State",
                                                                      "Match data"])

    with user_tab:
        st.subheader("User data")

        add_tab, remove_tab, edit_tab = st.tabs(["Add", "Remove", "Edit"])
        with add_tab:
            user_id = st.number_input(label="User id",
                                      min_value=login_df["id"].max() + 1)

            user_name = st.text_input(label="Username")
            user_password = hp.return_hash(st.text_input(label="Password"))
            user_admin = st.checkbox(label="Is admin",
                                     value=False)

            if st.button(label="Save user"):
                if user_name in login_df["name"] or user_id < login_df["id"].max():
                    st.warning(f"Faulty inputs")
                else:
                    new_user = pl.DataFrame(data=[[user_id, user_name, user_password, user_admin]],
                                            schema=["id", "name", ("password", pl.String), "admin"])
                    login_df = login_df.vstack(new_user)
                    login_df.write_csv(file="data/logins.csv")
                    st.rerun()

        with remove_tab:
            st.dataframe(login_df)
            id_to_remove = st.number_input(label="Remove user id:",
                                           min_value=login_df["id"].min(),
                                           max_value=login_df["id"].max())
            if st.button(label="Remove"):
                if id_to_remove in login_df["id"]:
                    filtered_df = login_df.filter(pl.col("id") != id_to_remove)
                    login_df = filtered_df
                    login_df.write_csv(file="data/logins.csv")
                    st.rerun()

        with edit_tab:
            edited_df = st.data_editor(data=login_df)

            if st.button(label="Save"):
                edited_df.write_csv(file="data/logins.csv")
                st.rerun()

    with event_tab:
        st.subheader("Event Data")
        schedule_df = hp.read_schedule(os.path.abspath("data/schedule.csv"))
        settings_json = hp.return_settings_data(os.path.abspath("data/settings.json"))

        basic_event_tab, weeks_event_tab, players_event_tab, fantasy_teams_tab = st.tabs(["Basic",
                                                                                          "Weeks",
                                                                                          "Players",
                                                                                          "Fantasy Teams"])
        with basic_event_tab:
            st.page_link(label="Main Wiki", page="https://lol.fandom.com/wiki/2024_Season_World_Championship")
            st.text("Start date: 2024-09-25")
            st.text("End date: 2024-11-02")

            st.page_link(label="Play-in Wiki", page="https://lol.fandom.com/wiki/2024_Season_World_Championship/Play-In")
            st.text("Start date: 2024-09-25")
            st.text("End date: 2024-09-29")

            st.page_link(label="Main Event Wiki", page="https://lol.fandom.com/wiki/2024_Season_World_Championship/Main_Event")
            st.text("Start date: 2024-09-29")
            st.text("End date: 2024-11-02")

        with weeks_event_tab:

            schedule_df = st.data_editor(schedule_df, num_rows="dynamic")
            if st.button(label="Save edits"):
                schedule_df.write_csv("data/schedule.csv")
                st.rerun()

            min_start_date = schedule_df["end_str"].max() if len(schedule_df["end_str"]) != 0 else "2024-09-25"
            min_start_date = "2024-09-25" if min_start_date is None else min_start_date


            schedule_name = st.text_input(label="Event name")


            stage_selection = st.selectbox(label="Stage",
                                           options=["playins", "swiss", "knockout"])
            schedule_start_date = st.date_input(label="Start date",
                                                min_value=datetime.strptime(min_start_date,
                                                                            "%Y-%m-%d"),
                                                max_value=datetime.strptime("2024-11-03", "%Y-%m-%d"))

            schedule_end_date = st.date_input(label="End date",
                                              min_value=schedule_start_date,
                                              max_value=datetime.strptime("2024-11-03", "%Y-%m-%d"))

            if st.button("Add event"):
                new_df = pl.DataFrame(data=[[stage_selection,
                                             schedule_name,
                                             schedule_start_date.strftime("%Y-%m-%d"),
                                             schedule_end_date.strftime("%Y-%m-%d")]],
                                      schema={"stage": pl.String,
                                              "name": pl.String,
                                              "start_str": pl.String,
                                              "end_str": pl.String})

                schedule_df = schedule_df.vstack(new_df)
                schedule_df.write_csv("data/schedule.csv")
                st.rerun()

        with players_event_tab:
            basic_players_info, players_pricing = st.tabs(["Basic Info", "Pricing"])
            with basic_players_info:
                players_df = hp.read_players(os.path.abspath("data/players.csv"))
                players_df = st.data_editor(players_df, num_rows="dynamic")

                if st.button(label="Save", key="players_event_save_button"):
                    players_df.write_csv(file="data/players.csv")
                    st.rerun()

            with players_pricing:
                if players_df is not None:
                    pricing_df = hp.return_player_pricing(os.path.abspath("data/players_cost.csv"),
                                                          players_df)
                    pricing_df = st.data_editor(pricing_df)

                    col1, col2 = st.columns(2)
                    with col1:
                        if st.button(label="Rebuild costs", key="pricing_rebuild"):
                            hp.remake_player_pricing(os.path.abspath("data/players_cost.csv"),
                                                     players_df,
                                                     hp.calculate_performance(hp.return_match_data(os.path.abspath("data/match_data.csv")),
                                                                              settings_json["multipliers"]))
                            st.rerun()
                    with col2:
                        if st.button(label="Save edits", key="pricing_edit_save"):
                            pricing_df.write_csv("data/players_cost.csv")
                            st.rerun()

                else:
                    st.warning("Player info file missing")

        with fantasy_teams_tab:
            team_owners = hp.return_only_team_owners(os.path.abspath("data/logins.csv"))

            fantasy_player_selection = st.selectbox("Edit for player:",
                                                    options=team_owners)
            owner_teams = hp.return_event_selection(os.path.abspath(f"data/teams/{fantasy_player_selection}_teams.csv"))
            owner_teams = st.data_editor(owner_teams, num_rows="dynamic")
            if st.button(label="Save edits for owner"):
                owner_teams.write_csv(os.path.abspath(f"data/teams/{fantasy_player_selection}_teams.csv"))
                st.rerun()


    with session_state_tab:
        st.subheader("Session state:")
        st.session_state

        st.subheader("Settings:")
        if settings_json is not None:
            st.json(settings_json)
        else:
            st.warning("JSON settings missing")

    with match_data_tab:
        match_df = hp.return_match_data(os.path.abspath("data/match_data.csv"))
        if match_df is None:
            st.warning("No match data found, upload it.")
            uploaded_file = st.file_uploader(label="Upload missing match data",
                                             type="csv",
                                             accept_multiple_files=False,
                                             key="missing_file_upload")
            if uploaded_file is not None:
                match_df = hp.read_uploaded_file(uploaded_file)
                match_df = match_df.filter(pl.col("league") == settings_json["event_name"])
                match_df = match_df.filter(pl.col("patch") == settings_json["patch"])
                match_df = match_df[settings_json["needed_columns"]]
                match_df.write_csv("data/match_data.csv")
                performance_df = hp.calculate_performance(match_df, settings_json["multipliers"])
                performance_df.write_csv("data/player_performance.csv")
                uploaded_file = None
                st.rerun()
        else:
            basic_match_data_tab, filter_match_data_tab, avg_performance_tab = st.tabs(["Basic/update",
                                                                                        "Filter",
                                                                                        "Avg. Performance"])

            with basic_match_data_tab:
                st.data_editor(match_df)
                uploaded_file = st.file_uploader(label="Update data",
                                                 type="csv",
                                                 accept_multiple_files=False,
                                                 key=f"update_file_upload")
                if uploaded_file is not None:
                    match_df = hp.read_uploaded_file(uploaded_file)
                    match_df = match_df.filter(pl.col("league") == settings_json["event_name"])
                    match_df = match_df.filter(pl.col("patch") == settings_json["patch"])
                    match_df = match_df[settings_json["needed_columns"]]
                    performance_df = hp.calculate_performance(match_df, settings_json["multipliers"])
                    performance_df.write_csv("data/player_performance.csv")
                    match_df.write_csv("data/match_data.csv")

            with filter_match_data_tab:
                all_columns = match_df.columns
                filter_column = st.selectbox(label="Filter on column:",
                                             options=all_columns)

                all_options = match_df[filter_column].unique(maintain_order=True).to_list()
                filter_choice = st.selectbox(label="Selection:",
                                             options=all_options)
                st.dataframe(match_df.filter(pl.col(filter_column) == filter_choice))
                st.subheader("Performance")
                st.dataframe(hp.calculate_performance(match_df.filter(pl.col(filter_column) == filter_choice),
                                                      settings_json["multipliers"]))

            with avg_performance_tab:
                performance_df = hp.calculate_performance(match_df, settings_json["multipliers"])
                if performance_df is None:
                    st.warning("Base performance missing")
                else:
                    avg_performance_df = hp.return_avg_performance(os.path.abspath("data/avg_performance.csv"),
                                                                   performance_df)
                    st.data_editor(avg_performance_df)

                    if st.button(label="Recalculate averages", key="average_performance_calculation"):
                        avg_performance_df = hp.recalculate_avg_performance(os.path.abspath("data/avg_performance.csv"),
                                                                            performance_df)
                        st.rerun()
