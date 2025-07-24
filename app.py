import streamlit as st
import pandas as pd
import requests
from io import StringIO

from dotenv import load_dotenv
import os

load_dotenv()

COUCHDROP_API_KEY = os.getenv("COUCHDROP_API_KEY")
LIST_URL = "https://fileio.couchdrop.io/file/ls"
DOWNLOAD_URL = "https://fileio.couchdrop.io/file/download"

def list_user_csvs(user_email):
    response = requests.get(
        f"{LIST_URL}",
        headers={"token": f"{COUCHDROP_API_KEY}"},
        params={"path": f"/Real_Intent/Customers/{user_email}/"}
    )
    try:
        response.raise_for_status()
        files = response.json()
        return [f for f in files if f["filename"].endswith(".csv")]
    except:
        return []

def download_csv(path):
    response = requests.get(
        f"{DOWNLOAD_URL}",
        headers={"token": f"{COUCHDROP_API_KEY}"},
        params={"path": path}
    )
    response.raise_for_status()
    return pd.read_csv(StringIO(response.text))

def remove_duplicates(new_df, existing_dfs, dedupe):
    combined_existing = pd.concat(existing_dfs, ignore_index=True)
    deduped_df = new_df[~new_df[dedupe].isin(combined_existing[dedupe])]
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

    email = st.text_input("Enter your email")

    if uploaded_file and email:
        df = pd.read_csv(uploaded_file)
        email = email.strip().lower()

        st.subheader("Uploaded Leads")
        st.dataframe(df)

        with st.spinner("Removing existing leads..."):
            user_csvs = list_user_csvs(email)
            existing_dfs = []

            for f in user_csvs:
                path = f"/Real_Intent/Customers/{email}/{f['filename']}"
                if f["filename"] != uploaded_file.name:  # Exclude current file
                    try:
                        existing_dfs.append(download_csv(path))
                    except Exception as e:
                        st.warning(f"Failed to load {f['name']}: {e}")

            if existing_dfs:
                dedupe = "email_1"
                cleaned_df = remove_duplicates(df, existing_dfs, dedupe)
                dedupe = "phone_1"
                cleaned_df = remove_duplicates(cleaned_df, existing_dfs, dedupe)
                st.success("Deduplication complete.")
                st.subheader("Cleaned Leads")
                st.dataframe(cleaned_df)
            else:
                st.info("No previous files found. Showing original leads.")
                cleaned_df = df

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