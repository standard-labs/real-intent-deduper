import streamlit as st
import pandas as pd
import requests

from dotenv import load_dotenv
from io import StringIO
import os
from concurrent.futures import ThreadPoolExecutor


# ---- Setup ----

load_dotenv()

COUCHDROP_API_KEY = os.getenv("COUCHDROP_API_KEY")
LIST_URL = "https://fileio.couchdrop.io/file/ls"
DOWNLOAD_URL = "https://fileio.couchdrop.io/file/download"

DEDUPE_KEYS: str = {"md5"}


def _list_user_csvs(user_email: str) -> list[dict]:
    """Pull a directory of all files associated with a particular user by email."""
    response = requests.post(
        f"{LIST_URL}",
        headers={"token": f"{COUCHDROP_API_KEY}"},
        params={"path": f"/Real_Intent/Customers/{user_email}/"}
    )

    response.raise_for_status()
    files = response.json()["ls"]
    return [f for f in files if f["filename"].endswith(".csv")]


def _download_csv(path: str) -> pd.DataFrame:
    """
    Download a CSV file from Couchdrop.

    Raises on non-200 codes as `path` is assumed to exist.
    """
    response = requests.post(
        f"{DOWNLOAD_URL}",
        headers={"token": f"{COUCHDROP_API_KEY}"},
        params={"path": path}
    )
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def download_user_csvs(user_email: str) -> list[pd.DataFrame]:
    """
    Download all CSV files associated with a particular user by email.

    Args:
        user_email (str): The user's email.

    Returns:
        list[pd.DataFrame]: A list of dataframes, one for each CSV file.
    """
    user_csvs: list[dict] = _list_user_csvs(user_email)

    def _build_path(f: dict) -> str:
        return f"/Real_Intent/Customers/{user_email}/{f['filename']}"

    download_inputs: list[str] = [_build_path(f) for f in user_csvs]
    with ThreadPoolExecutor(max_workers=20) as executor:
        return list(executor.map(_download_csv, download_inputs))


def _is_same_file(df1: pd.DataFrame, df2: pd.DataFrame, dedupe_key: str) -> bool:
    """Check if two dataframes are the same based on a dedupe key."""
    return df1[dedupe_key].equals(df2[dedupe_key])


def remove_duplicates(
    new_df: pd.DataFrame, 
    existing_dfs: list[pd.DataFrame], 
    dedupe_key: str
) -> pd.DataFrame:
    """Remove duplicates from a new dataframe based on an existing set of dataframes."""
    foreign_existing_dfs: list[pd.DataFrame] = [
        df for df in existing_dfs if not _is_same_file(new_df, df, dedupe_key)
    ]

    combined_existing = pd.concat(foreign_existing_dfs, ignore_index=True)
    deduped_df = new_df[~new_df[dedupe_key].isin(combined_existing[dedupe_key])]
    return deduped_df


def main():
    st.title('Existing Lead Remover')
    st.info("""
            This app removes duplicate leads from a CSV file,
            where duplicate leads are those that have already
            been shared to a user. This app ensures that each
            lead is only shared to a user once throughout the 
            entire history of RealIntent.
            """)
    
    st.header('Upload CSV File')
    uploaded_file = st.file_uploader("Choose a CSV file", type="csv")

    email: str = st.text_input("Enter your email")

    if uploaded_file and email:
        df = pd.read_csv(uploaded_file)
        email = email.strip().lower()

        st.subheader("Uploaded Leads")
        st.dataframe(df)  # Display original data

        with st.spinner("Removing existing leads..."):
            if not (existing_dfs := download_user_csvs(email)):
                st.info("No previous files found. Showing original leads.")
                cleaned_df = df
                st.success("No deduplication needed.")
                st.stop()
                return  # No need to deduplicate

            # Deduplicate for each dedupe key
            cleaned_df = df
            for dedupe_key in DEDUPE_KEYS:
                cleaned_df = remove_duplicates(cleaned_df, existing_dfs, dedupe_key)

            st.success("Deduplication complete.")

        # Display results
        st.subheader("Deduplicated Leads")
        st.dataframe(cleaned_df)

        # Download
        csv = cleaned_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download deduplicated CSV",
            data=csv,
            file_name='deduplicated_file.csv',
            mime='text/csv',
        )


if __name__ == "__main__":
    main()
