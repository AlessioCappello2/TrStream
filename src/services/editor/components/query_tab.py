import time
import requests
import threading
import pandas as pd
import streamlit as st


def render_query_tab(api_base: str):
    """
    Render the query execution tab.
    
    Args:
        api_base: Base URL for the querier API
    """
    st.subheader("Run SQL Query")

    sql = st.text_area(
        "Enter SQL query:", 
        height=150,
        placeholder="SELECT * FROM read_parquet('s3://trstream-analytics/tb-stripe/**/*.parquet');",  
        help="Only SELECT and WITH queries are allowed."
    )

    if st.button("Run Query"):
        if not sql.strip():
            st.warning("Please enter a SQL query")
        else:
            _execute_query(api_base, sql)


def _execute_query(api_base: str, sql: str):
    """
    Execute a SQL query against the API.
    
    Args:
        api_base: Base URL for the querier API
        sql: SQL query to execute
    """
    placeholder = st.empty()
    start = time.perf_counter()
    result_container = {"response": None, "error": None}

    def run_query():
        try: 
            result_container["response"] = requests.post(
                                            f"{api_base}/query",
                                            json={"sql": sql},
                                        )
        except Exception as e:
            result_container["error"] = str(e)
            raise Exception
        
    t = threading.Thread(target=run_query)
    t.start()

    while t.is_alive():
        elapsed = time.perf_counter() - start 
        placeholder.info(f"Running... {elapsed:.2f} sec")
        time.sleep(0.1)

    t.join()
    placeholder.empty()

    if result_container["error"]:
        st.error(f"Request failed: {result_container['error']}")
        return 
    
    response = result_container["response"]
    elapsed = time.perf_counter() - start 

    try:
        result = response.json()

        if response.status_code == 200:
                placeholder.success(f"Completed in {elapsed:.2f} sec - {result['rows']} rows returned")
                df = pd.DataFrame(result['data'])
                st.dataframe(df)

        else:
            detail = result.get("detail", "Unknown error")
            st.error(f"Query failed: {detail}")

    except Exception as e:
        st.error(f"Failed to parse response: {e}")