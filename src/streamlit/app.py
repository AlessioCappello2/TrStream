import streamlit as st
import requests
import pandas as pd

st.set_page_config(page_title="DuckDB SQL Editor", layout="wide")
st.title("DuckDB SQL Editor")

# User input SQL
sql = st.text_area("Enter SQL query:", height=150)

if st.button("Run Query"):
    if not sql.strip():
        st.warning("Please enter a SQL query")
    else:
        try:
            # Send query to FastAPI backend
            response = requests.post(
                "http://querier:8000/query",
                json={"sql": sql},
            )
            response.raise_for_status()
            result = response.json()

            st.success(f"{result['rows']} rows returned")
            df = pd.DataFrame(result['data'])
            st.dataframe(df)
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to run query: {e}")
        except Exception as e:
            st.error(f"Error: {e}")
