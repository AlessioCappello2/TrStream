import streamlit as st
import requests
import pandas as pd
import threading
import time

API_BASE = "http://querier:8000"

st.set_page_config(page_title="DuckDB SQL Editor", layout="wide")
st.title("DuckDB SQL Editor")

tab_query, tab_alias = st.tabs(["Run Query", "Manage Aliases"])

# Tab query
with tab_query:
    st.subheader("Run SQL query")
    sql = st.text_area("Enter SQL query:", height=150)

    if st.button("Run Query"):
        if not sql.strip():
            st.warning("Please enter a SQL query")
        else:
            try:
                placeholder = st.empty()
                start = time.perf_counter()
                result_container = {"response": None}

                def run_query():
                    try: 
                        result_container["response"] = requests.post(
                                                        f"{API_BASE}/query",
                                                        json={"sql": sql},
                                                    )
                    except Exception:
                        result_container["response"] = e
                        raise Exception
                    
                t = threading.Thread(target=run_query)
                t.start()

                while t.is_alive():
                    elapsed = time.perf_counter() - start 
                    placeholder.info(f"Running... {elapsed:.2f} sec")
                    time.sleep(0.1)

                t.join()
                placeholder.empty()

                response = result_container["response"]
                result = response.json()
                response.raise_for_status()

                placeholder.success(f"Completed in {elapsed:.2f} sec - {result['rows']} rows returned")
                df = pd.DataFrame(result['data'])
                st.dataframe(df)
            except Exception as e:
                try:
                    detail = result_container["response"].json()["detail"]
                except Exception:
                    detail = "Unknown internal error"
                st.error(detail)

# Tab alias
with tab_alias:
    st.subheader("Create or Update Alias")

    alias_name = st.text_input("Alias name (e.g. tx)", key="alias_name")
    alias_path = st.text_input("Parquet path (e.g. s3://bucket/path/*.parquet)", key="alias_path")

    if st.button("Create Alias"):
        if not alias_name or not alias_path:
            st.error("Both alias name and path are required.")
        else:
            response = requests.post(
                f"{API_BASE}/alias",
                json={"name": alias_name, "path": alias_path}
            )

            if response.status_code == 200:
                st.success(response.json()["detail"])
            else:
                try:
                    detail = response.json().get("detail", "Unknown internal error")
                except Exception:
                    detail = response.text
                st.error(detail)