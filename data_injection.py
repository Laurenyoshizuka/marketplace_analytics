################
# Landing Page #
################

import pandas as pd
from sqlalchemy import create_engine
import streamlit as st
from sqlalchemy import create_engine
import os

db_url = st.secrets["DB_URL"]
engine = create_engine(db_url)


@st.cache_data
def load_csv_to_db(file, table_name):
    df = pd.read_csv(file, encoding='latin-1')
    df.to_sql(table_name, engine, if_exists='replace', index=False)
    return f"{table_name} loaded with {len(df)} rows."


st.title("Product Data Analyst Task")

data_folder = "data"

if not os.path.exists(data_folder):
    st.error(f"Data directory not found: {data_folder}")
else:
    files_loaded = []
    for filename in os.listdir(data_folder):
        if filename.endswith(".csv"):
            filepath = os.path.join(data_folder, filename)
            table_name = filename.replace(".csv", "").lower()
            result = load_csv_to_db(filepath, table_name)
            st.success(result)
            files_loaded.append(table_name)
    
    if not files_loaded:
        st.warning("No CSV files found in data directory.")