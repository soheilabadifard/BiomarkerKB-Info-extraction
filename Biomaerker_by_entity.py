from __future__ import annotations

from typing import Optional

import pandas as pd

from bkb_client import download_with_size_escalation

# --- Search behaviour configuration ---
INITIAL_SEARCH_SIZE = 10_000
MAX_SIZE_ATTEMPTS = 4


def fetch_biomarker_records(biomarker_name: str) -> Optional[pd.DataFrame]:
    def payload_factory(size: Optional[int]) -> dict[str, object]:
        payload = {"biomarker_entity_name": biomarker_name}
        if size is not None:
            payload["size"] = size
        return payload

    return download_with_size_escalation(
        payload_factory=payload_factory,
        description=f"biomarker '{biomarker_name}'",
        expect_label=biomarker_name,
        initial_size=INITIAL_SEARCH_SIZE,
        max_attempts=MAX_SIZE_ATTEMPTS,
    )




# --- Main Execution Block ---
if __name__ == "__main__":
    blist = pd.read_excel("Biomarkers_Categorization.xlsx")
    print("Read Data successfully:")
    biomarkers_to_enrich = blist["BioMarker"].dropna().to_list()

    output_filename = "biomarker_results.xlsx"

    all_results = []
    total_biomarkers = len(biomarkers_to_enrich)
    step_1_successes = 0
    step_2_successes = 0

    print("🚀 Starting enrichment process for each biomarker individually...")
    for biomarker in biomarkers_to_enrich:
        print("\n" + "=" * 50)
        print(f"Processing biomarker: {biomarker}")
        print("=" * 50)

        df = fetch_biomarker_records(biomarker)

        if df is not None and not df.empty:
            step_1_successes += 1
            step_2_successes += 1
            df["query_biomarker"] = biomarker
            all_results.append(df)
        else:
            placeholder = {
                "query_biomarker": biomarker,
                "biomarker_canonical_id": "No data found" if df is not None else "Step 1 failed",
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

    print("\n" + "=" * 50)
    print("--- 📈 Process Summary ---")
    print(f"Total Biomarkers in List: {total_biomarkers}")
    print(f"Step 1 Successful (List ID created): {step_1_successes}")
    print(f"Step 2 Successful (Data downloaded): {step_2_successes}")
    print("=" * 50)
