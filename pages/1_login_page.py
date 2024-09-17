import streamlit as st
import os
import sys
import hashlib

import polars as pl
from polars.exceptions import RowsError

sys.path.append('../')

import scripts.helper_functions as hp



def main():
    st.header("Login page")
    st.text(os.path.abspath("data/logins.csv"))
    if "logged_in" not in st.session_state:
        login_df = hp.read_login(os.path.abspath("data/logins.csv"))

        login_name = st.text_input(label="Username")
        login_password = hp.return_hash(st.text_input(label="Password"))
        try:
            login_row = login_df.row(by_predicate=(pl.col("name") == login_name))
            if login_password == str(login_row[2]):
                st.session_state["logged_in"] = True
                st.session_state["name"] = login_name
                st.session_state["is_admin"] = login_row[3]
                st.rerun()
            else:
                st.text("Wrong username/password")
        except RowsError:
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
            except RowsError:
                st.text("Wrong username/password")

    else:
        st.warning("Already logged in")
        st.page_link("fantasy_league_main.py")

if __name__ == '__main__':
    main()