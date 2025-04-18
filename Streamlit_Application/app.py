import streamlit as st
import os


PAGES = {
    "Home": "Home.py",
    "Historical Resale Price Predictor": "XGBoost.py",
    "Future Resale Price Predictor": "LinReg.py"
}

st.sidebar.title('Navigation')
selection = st.sidebar.radio("Go to", list(PAGES.keys()))

page = PAGES[selection]
exec(open(page).read())
