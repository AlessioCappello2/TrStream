import requests 
import streamlit as st

def render_alias_tab(api_base: str):
    """
    Render the alias management tab.
    
    Args:
        api_base: Base URL for the querier API
    """
    st.subheader("Create or Update Alias")

    alias_name = st.text_input("Alias name", key="alias_name", placeholder="transactions")
    alias_path = st.text_input("Parquet path", key="alias_path", placeholder="bucket/path/*.parquet")

    with st.expander("Examples"):
        st.code("""
# Single file
Alias: transactions_jan
Path: trstream-analytics/tb-revolut/year=2026/month=01/day=01/data.parquet

# All files in folder (pattern matching)
Alias: transactions_2026
Path: trstream-analytics/tb-faker/year=2026/**/*.parquet""")

    if st.button("Create Alias"):
        if not alias_name or not alias_path:
            st.error("Both alias name and path are required.")
        else:
            _create_alias(api_base, alias_name, alias_path)

    st.markdown("---")
    st.markdown("#### Usage")
    if alias_name:
        st.code(f"SELECT * FROM {alias_name}() LIMIT 10;", language="sql")
    else:
        st.code("SELECT * FROM your_alias() LIMIT 10;", language="sql")


def _create_alias(api_base: str, alias_name: str, alias_path: str):
    """
    Create a table alias via the API.
    
    Args:
        api_base: Base URL for the querier API
        alias_name: Name of the alias to create
        alias_path: S3/MinIO path for the alias
    """
    try:
        response = requests.post(
             f"{api_base}/alias",
             json={"name": alias_name, "path": alias_path},
             timeout=15
        )

        result = response.json()

        if response.status_code == 200:
            st.success(result.get("detail", "Alias registered successfully!"))
        else:
            try:
                detail = result.get("detail", "Unknown error")
            except Exception as e:
                detail = response.text or "Unknown error"
            st.error(f"Failed to create alias: {detail}")
  
    except requests.exceptions.Timeout:
        st.error("Request timed out!")
    except requests.exceptions.ConnectionError:
        st.error("Could not connect to querier service!")
    except Exception as e:
        st.error(f"Unexpected error: {e}")