import os

import streamlit as st
import polars as pl
import plotly.express as px


if "logged_in" not in st.session_state:
    st.warning("Please login, use sidebar")
    st.page_link("pages/1_login_page.py")
else:
    st.text("yyay")