import streamlit as st
import os
import polars as pl

if "login_data_path" not in st.session_state:
    st.session_state["login_data_path"] = "data/logins.csv"
    if os.path.isfile(st.session_state["login_data_path"]):
        login_df = pl.read_csv(st.session_state["login_data_path"],
                               schema={"id": pl.Int64,
                                        "name": pl.String,
                                        "password": pl.String,
                                        "admin": pl.Boolean})
        st.session_state["login_df"] = login_df
    elif "login_df" not in st.session_state:
        login_df = pl.DataFrame(data=[[0, "admin", str(hash("123")), True]],
                                schema={"id": pl.Int64,
                                        "name": pl.String,
                                        "password": pl.String,
                                        "admin": pl.Boolean})
        st.session_state["login_df"] = login_df
        login_df.write_csv(file=st.session_state["login_data_path"])
    else:
        login_df = st.session_state["login_df"]
else:
    login_df = st.session_state["login_df"]

st.header("Login page")


if "logged_in" not in st.session_state:
    login_name = st.text_input(label="Username")
    login_password = str(hash(st.text_input(label="Password")))
    try:
        login_row = login_df.row(by_predicate=(pl.col("name") == login_name))
        if login_password == str(login_row[2]):
            st.session_state["logged_in"] = True
            st.session_state["name"] = login_name
            st.session_state["is_admin"] = login_row[3]
            st.rerun()
        else:
            st.text("Wrong username/password")
    except pl.RowsError:
        st.text("Wrong username/password")

    if st.button(label="Log in"):
        try:
            login_row = login_df.row(by_predicate=(pl.col("name") == login_name))
            if login_password == str(login_row[2]):
                st.session_state["logged_in"] = True
                st.session_state["name"] = login_name
                st.session_state["is_admin"] = login_row[3]
                st.rerun()
            else:
                st.text("Wrong username/password")
        except pl.RowsError:
            st.text("Wrong username/password")

else:
    st.warning("Already logged in")
    st.page_link("fantasy_league_main.py")

