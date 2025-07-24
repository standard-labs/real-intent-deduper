import streamlit as st
import pandas as pd
import requests

from dotenv import load_dotenv
from io import StringIO
import os


# ---- Setup ----

load_dotenv()

COUCHDROP_API_KEY = os.getenv("COUCHDROP_API_KEY")
LIST_URL = "https://fileio.couchdrop.io/file/ls"
DOWNLOAD_URL = "https://fileio.couchdrop.io/file/download"

DEDUPE_KEYS: str = {"email_1", "phone_1"}


def list_user_csvs(user_email: str) -> list[dict]:
    """Pull a directory of all files associated with a particular user by email."""
    response = requests.post(
        f"{LIST_URL}",
        headers={"token": f"{COUCHDROP_API_KEY}"},
        params={"path": f"/Real_Intent/Customers/{user_email}/"}
    )

    if response.status_code != 200:
        st.error(f"Failed to list files: {response.json()}")
        st.stop()
        return

    files = response.json()["ls"]
    return [f for f in files if f["filename"].endswith(".csv")]


def download_csv(path: str) -> pd.DataFrame:
    """Download a CSV file from Couchdrop."""
    response = requests.post(
        f"{DOWNLOAD_URL}",
        headers={"token": f"{COUCHDROP_API_KEY}"},
        params={"path": path}
    )
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))


def remove_duplicates(
    new_df: pd.DataFrame, 
    existing_dfs: list[pd.DataFrame], 
    dedupe_key: str
) -> pd.DataFrame:
    """Remove duplicates from a new dataframe based on an existing set of dataframes."""
    combined_existing = pd.concat(existing_dfs, ignore_index=True)
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
        st.dataframe(df)

        with st.spinner("Removing existing leads..."):
            user_csvs: list[dict] = list_user_csvs(email)
            existing_dfs: list[pd.DataFrame] = []

            for f in user_csvs:
                path = f"/Real_Intent/Customers/{email}/{f['filename']}"
                if f["filename"] != uploaded_file.name:  # exclude current file
                    try:
                        existing_dfs.append(download_csv(path))
                    except Exception as e:
                        st.warning(f"Failed to load {f['name']}: {e}")

            if not existing_dfs:
                st.info("No previous files found. Showing original leads.")
                cleaned_df = df
                st.success("No deduplication needed.")
                st.stop()
                return

            # Deduplicate for each dedupe key
            cleaned_df = df
            for dedupe_key in DEDUPE_KEYS:
                cleaned_df = remove_duplicates(cleaned_df, existing_dfs, dedupe_key)

            st.success("Deduplication complete.")
            st.subheader("Cleaned Leads")
            st.dataframe(cleaned_df)

        df = cleaned_df
        st.subheader("Cleaned Leads")
        st.write(df)

        # Download
        csv = df.to_csv(index=False).encode('utf-8')
        st.download_button(
            label="Download cleaned CSV",
            data=csv,
            file_name='cleaned_file.csv',
            mime='text/csv',
        )


if __name__ == "__main__":
    main()
