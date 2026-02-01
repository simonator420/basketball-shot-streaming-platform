import os
import pandas as pd
import streamlit as st
import requests
from databricks import sql


def _need(name: str) -> str:
    """
    Helper function to read required app environment variables.

    If the variable is missing, the Streamlit app is stopped
    and error message is shown.
    """
    v = os.getenv(name)
    if not v:
        st.error(f"Missing env var: {name}")
        st.stop()
    return v


@st.cache_data(ttl=50)
def _get_access_token() -> str:
    """
    Obtain an OAuth access token from Databricks using
    Service Principal credentials (client_id + client_secret).

    The token is cached for a short time (TTL = 50 seconds)
    to avoid requesting a new token on every rerun.
    """
    
    # Read Service Principal credentials from environment variables
    client_id = _need("DATABRICKS_CLIENT_ID")
    client_secret = _need("DATABRICKS_CLIENT_SECRET")
    
    # Read Databricks workspace host
    host = _need("DATABRICKS_HOST").rstrip("/")
    
    # Ensure the host contains the HTTPS scheme
    if not host.startswith("http"):
        host = "https://" + host

    # Databricks OAuth token endpoint
    token_url = f"{host}/oidc/v1/token"

    # Request an access token using the client credentials flow
    resp = requests.post(
        token_url,
        auth=(client_id, client_secret),
        data={"grant_type": "client_credentials", "scope": "all-apis"},
        timeout=20,
    )
    
    # Stop the app if token request fails
    if resp.status_code != 200:
        st.error(f"Token request failed: {resp.status_code} {resp.text}")
        st.stop()

    # Return the access token from the response
    return resp.json()["access_token"]


@st.cache_data(ttl=60)
def sql_to_pandas(query: str) -> pd.DataFrame:
    """
    Execute a SQL query against a Databricks SQL Warehouse
    and return the result as a Pandas DataFrame.

    Results are cached for 60 seconds to reduce load
    on the SQL Warehouse and improve dashboard performance.
    """
    
    # Read Databricks connection details
    host = _need("DATABRICKS_HOST").replace("https://", "")
    http_path = _need("DATABRICKS_HTTP_PATH")
    
    # Retrieve a valid OAuth access token
    token = _get_access_token()

    try:
        # Connect to Databricks SQL Warehouse
        with sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                rows = cur.fetchall()
                # Extract column names from cursor metadata
                cols = [d[0] for d in cur.description] if cur.description else []
        # Convert result to Pandas DataFrame
        return pd.DataFrame(rows, columns=cols)

    except Exception as e:
        # Handle SQL or connection errors
        st.error(f"SQL Query Error: {e}")
        st.error(f"Query was: {query}")
        return pd.DataFrame()
