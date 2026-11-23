"""Registry of navigation pages, filled in by app.py so any page can link to any other."""
PAGES: dict = {}


def open_page(key: str):
    import streamlit as st
    st.switch_page(PAGES[key])
