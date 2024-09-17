import streamlit as st
import os
import polars as pl
from datetime import datetime
import scripts.helper_functions as hp

if "logged_in" not in st.session_state or ("is_admin" not in st.session_state or st.session_state["is_admin"] is False):
    st.warning("You are not logged in or unauthorized to use this")
    st.page_link("pages/1_login_page.py")
else:
    st.header(f"Admin page - {st.session_state["name"]}")
    login_df = hp.read_login(os.path.abspath("data/logins.csv"))
    user_tab, event_tab, session_state_tab = st.tabs(["User data", "Event data", "Session State"])

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
        basic_event_tab, weeks_event_tab, players_event_tab = st.tabs(["Basic", "Weeks", "Players"])
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
                                                max_value=datetime.strptime("2024-11-02", "%Y-%m-%d"))

            schedule_end_date = st.date_input(label="End date",
                                              min_value=schedule_start_date,
                                              max_value=datetime.strptime("2024-11-02", "%Y-%m-%d"))

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
            players_df = hp.read_players(os.path.abspath("data/players.csv"))
            players_df = st.data_editor(players_df, num_rows="dynamic")

            if st.button(label="Save", key="players_event_save_button"):
                players_df.write_csv(file="data/players.csv")
                st.rerun()

    with session_state_tab:
        st.session_state
