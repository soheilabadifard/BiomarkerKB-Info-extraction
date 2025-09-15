import io
from typing import Optional

import pandas as pd
import requests

# --- API Endpoints ---
API_BASE = "https://api.biomarkerkb.org"
SEARCH_PATH = "/biomarker/search"
DOWNLOAD_PATH = "/data/list_download"


def create_list_for_record_type(record_type: str) -> Optional[str]:
    """
    Step 1: Creates a search list for a given record_type to get a list_id.
    """
    print(f"🔬 Creating a search list for record_type: '{record_type}'...")
    url = f"{API_BASE}{SEARCH_PATH}"

    # The payload now filters by 'record_type'
    payload = {
        "record_type": record_type,
        "size": 50000  # Use a large size to get as many results as possible
    }

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=120)
        response.raise_for_status()
        data = response.json()

        list_id = data.get("list_id")
        if list_id:
            print(f"  ✅ Successfully created list. List ID: {list_id}")
            return list_id
        else:
            print("  ❌ Error: 'list_id' not found in the response.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  ❌ API request failed: {e}")
        return None


def download_list_data(list_id: str) -> Optional[pd.DataFrame]:
    """
    Step 2: Uses the list_id to download the data and return it as a pandas DataFrame.
    """
    print(f"📂 Downloading data for List ID: {list_id}...")
    url = f"{API_BASE}{DOWNLOAD_PATH}"

    payload = {
        "id": list_id,
        "download_type": "biomarker_list",
        "format": "csv",
        "compressed": False
    }

    headers = {"Content-Type": "application/json", "Accept": "text/csv"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=300)
        response.raise_for_status()
        csv_data = response.text

        if not csv_data or len(csv_data.splitlines()) <= 1:
            print("  ⚠️ Data download was successful, but contained no records.")
            return None

        print("  ✅ Data downloaded successfully.")
        string_file = io.StringIO(csv_data)
        df = pd.read_csv(string_file)
        return df
    except (requests.exceptions.RequestException, pd.errors.EmptyDataError) as e:
        print(f"  ❌ Data download or parsing failed: {e}")
        return None


# --- Main Execution Block ---
if __name__ == "__main__":
    # You can change this to other types like 'canonical' if needed
    target_record_type = "biomarker"
    output_filename = f"record_type_{target_record_type}_biomarkers.xlsx"

    print(f"🚀 Starting process to find all records with record_type: '{target_record_type}'")

    # Step 1: Create the list for the target record_type
    list_id = create_list_for_record_type(target_record_type)

    # Step 2: If a list_id was created, download the data
    if list_id:
        results_df = download_list_data(list_id)

        if results_df is not None and not results_df.empty:
            print("\n" + "#" * 50)
            print("### 📊 Final Results ###")
            print("#" * 50)

            print(f"📈 Found {results_df.shape[0]} records with record_type '{target_record_type}'.")

            print(f"💾 Saving results to '{output_filename}'...")
            results_df.to_excel(output_filename, index=False)
            print(f"  ✅ Done! Your file '{output_filename}' is ready.")
        else:
            print("\n--- 🤷 No data was found for the specified record_type. ---")
    else:
        print("\n--- 🤷 Process failed at Step 1. Could not create a list. ---")
