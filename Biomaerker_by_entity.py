import io
from typing import Optional

import pandas as pd
import requests

# --- API Endpoints ---
API_BASE = "https://api.biomarkerkb.org"
SEARCH_PATH = "/biomarker/search"
DOWNLOAD_PATH = "/data/list_download"


def create_biomarker_list(biomarker_name: str) -> Optional[str]:
    """
    Step 1: Sends a SINGLE biomarker name to create a list and get its ID.
    """
    print(f"🔬 Creating a search list for '{biomarker_name}'...")
    url = f"{API_BASE}{SEARCH_PATH}"

    payload = {
        "biomarker_entity_name": biomarker_name,
        "size": 10000
    }

    headers = {"Content-Type": "application/json", "Accept": "application/json"}

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=60)
        response.raise_for_status()
        data = response.json()

        list_id = data.get("list_id")
        if list_id:
            print(f"  ✅ Successfully created list. List ID: {list_id}")
            return list_id
        else:
            print(f"  ❌ Error: 'list_id' not found in response for '{biomarker_name}'.")
            return None
    except requests.exceptions.RequestException as e:
        print(f"  ❌ API request failed for '{biomarker_name}': {e}")
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
        response = requests.post(url, json=payload, headers=headers, timeout=120)
        response.raise_for_status()
        csv_data = response.text

        if not csv_data or len(csv_data.splitlines()) <= 1:
            print("  ⚠️ Data download was successful, but contained no records.")
            return pd.DataFrame()  # Return an empty DataFrame

        print("  ✅ Data downloaded successfully.")
        string_file = io.StringIO(csv_data)
        df = pd.read_csv(string_file)
        return df
    except (requests.exceptions.RequestException, pd.errors.EmptyDataError) as e:
        print(f"  ❌ Data download or parsing failed: {e}")
        return None


# --- Main Execution Block ---
if __name__ == "__main__":

    blist = pd.read_excel("Biomarkers_Categorization.xlsx")
    print("Read Data successfully:")
    biomarkers_to_enrich = blist['BioMarker'].dropna().to_list()
    # biomarkers_to_enrich = [
    #     "Interleukin-6",
    #     "MUC16",
    #     "KLK3",
    #     "ESR1",
    #     "Albumin"  # This will have different columns to show the merge works
    # ]

    output_filename = "biomarker_results.xlsx"

    # A list to hold all the individual DataFrame results
    all_results = []

    # --- Statistics Counters ---
    total_biomarkers = len(biomarkers_to_enrich)
    step_1_successes = 0
    step_2_successes = 0

    print("🚀 Starting enrichment process for each biomarker individually...")
    for biomarker in biomarkers_to_enrich:
        print("\n" + "=" * 50)
        print(f"Processing biomarker: {biomarker}")
        print("=" * 50)

        list_id = create_biomarker_list(biomarker)

        df = None  # Reset DataFrame for each loop
        if list_id:
            step_1_successes += 1  # Increment Step 1 counter
            df = download_list_data(list_id)

        if df is not None and not df.empty:
            step_2_successes += 1  # Increment Step 2 counter
            df['query_biomarker'] = biomarker
            all_results.append(df)
        else:
            # Add a placeholder row for any biomarker that didn't yield data
            placeholder = {
                'query_biomarker': biomarker,
                'biomarker_canonical_id': 'No data found' if list_id else 'Step 1 failed',
            }
            all_results.append(pd.DataFrame([placeholder]))

    if all_results:
        print("\n" + "#" * 50)
        print("### 📊 Combining all results... ###")
        print("#" * 50)

        final_df = pd.concat(all_results, ignore_index=True)

        print(f"📈 Final table has {final_df.shape[0]} rows and {final_df.shape[1]} columns.")

        print(f"💾 Saving final results to '{output_filename}'...")
        final_df.to_excel(output_filename, index=False)
        print("  ✅ Done! Your file is ready.")

    else:
        print("\n--- 🤷 No data was successfully enriched. ---")

    # --- Final Statistics Summary ---
    print("\n" + "=" * 50)
    print("--- 📈 Process Summary ---")
    print(f"Total Biomarkers in List: {total_biomarkers}")
    print(f"Step 1 Successful (List ID created): {step_1_successes}")
    print(f"Step 2 Successful (Data downloaded): {step_2_successes}")
    print("=" * 50)
