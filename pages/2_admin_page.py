import streamlit as st
import os
import polars as pl
from datetime import datetime

if "logged_in" not in st.session_state or ("is_admin" not in st.session_state or st.session_state["is_admin"] is False):
    st.warning("You are not logged in or unauthorized to use this")
    st.page_link("pages/1_login_page.py")
else:
    st.header("Admin page")
    user_tab, event_tab, session_state_tab = st.tabs(["User data", "Event data", "Session State"])

    with user_tab:
        st.subheader("User data")

        add_tab, remove_tab, edit_tab = st.tabs(["Add", "Remove", "Edit"])
        with add_tab:
            user_id = st.number_input(label="User id",
                                      min_value=st.session_state["login_df"]["id"].max() + 1)

            user_name = st.text_input(label="Username")
            user_password = str(hash(st.text_input(label="Password")))
            user_admin = st.checkbox(label="Is admin",
                                     value=False)

            if st.button(label="Save user"):
                if user_name in st.session_state["login_df"]["name"] or user_id < st.session_state["login_df"]["id"].max():
                    st.warning(f"Faulty inputs")
                else:
                    new_user = pl.DataFrame(data=[[user_id, user_name, user_password, user_admin]],
                                            schema=["id", "name", ("password", pl.String), "admin"])
                    login_df = st.session_state["login_df"]
                    login_df = login_df.vstack(new_user)
                    st.session_state["login_df"] = login_df
                    login_df.write_csv(file="data/logins.csv")
                    st.rerun()

        with remove_tab:
            st.dataframe(st.session_state["login_df"])
            id_to_remove = st.number_input(label="Remove user id:",
                                           min_value=st.session_state["login_df"]["id"].min(),
                                           max_value=st.session_state["login_df"]["id"].max())
            if st.button(label="Remove"):
                login_df = st.session_state["login_df"]
                if id_to_remove in login_df["id"]:
                    filtered_df = login_df.filter(pl.col("id") != id_to_remove)
                    login_df = filtered_df
                    st.session_state["login_df"] = login_df
                    login_df.write_csv(file="data/logins.csv")
                    st.rerun()

        with edit_tab:
            edited_df = st.data_editor(data=st.session_state["login_df"])

            if st.button(label="Save"):
                st.session_state["login_df"] = edited_df
                edited_df.write_csv(file="data/logins.csv")
                st.rerun()

    with event_tab:
        st.subheader("Event Data")
        basic_event_tab, weeks_event_tab = st.tabs(["Basic", "Weeks"])
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
            if not os.path.isfile("data/schedule.csv"):
                schedule_df = pl.DataFrame(data=[],
                                           schema={"name": pl.String,
                                                   "start_date": pl.Date,
                                                   "end_date": pl.Date,
                                                   "start_str": pl.String,
                                                   "end_str": pl.String})
                schedule_df.write_csv(file="data/schedule.csv")
            else:
                schedule_df = pl.read_csv(source="data/schedule.csv",
                                          schema={"name": pl.String,
                                                  "start_date": pl.Date,
                                                  "end_date": pl.Date,
                                                  "start_str": pl.String,
                                                  "end_str": pl.String})
            schedule_df = st.data_editor(schedule_df, num_rows="dynamic")
            schedule_name = st.text_input(label="Event name")
            schedule_start_date = st.date_input(label="Start date",
                                                min_value=datetime.strptime("2024-09-25", "%Y-%m-%d"),
                                                max_value=datetime.strptime("2024-11-02", "%Y-%m-%d"))

            schedule_end_date = st.date_input(label="End date",
                                              min_value=schedule_start_date,
                                              max_value=datetime.strptime("2024-11-02", "%Y-%m-%d"))
            if st.button("Add event"):
                new_df = pl.DataFrame(data=[[schedule_name,
                                            schedule_start_date,
                                            schedule_end_date,
                                            schedule_start_date.strftime("%Y-%m-%d"),
                                            schedule_end_date.strftime("%Y-%m-%d")]],
                                      schema={"name": pl.String,
                                              "start_date": pl.Date,
                                              "end_date": pl.Date,
                                              "start_str": pl.String,
                                              "end_str": pl.String})

                schedule_df = schedule_df.vstack(new_df)
                schedule_df.write_csv("data/schedule.csv")
