from __future__ import annotations

from typing import Optional

import pandas as pd

from bkb_client import download_with_size_escalation

INITIAL_SEARCH_SIZE = 50_000
MAX_SIZE_ATTEMPTS = 4


def fetch_specimen_records(specimen_name: str) -> Optional[pd.DataFrame]:
    def payload_factory(size: Optional[int]) -> dict[str, object]:
        payload = {"specimen_name": specimen_name}
        if size is not None:
            payload["size"] = size
        return payload

    return download_with_size_escalation(
        payload_factory=payload_factory,
        description=f"specimen '{specimen_name}'",
        expect_label=specimen_name,
        initial_size=INITIAL_SEARCH_SIZE,
        max_attempts=MAX_SIZE_ATTEMPTS,
    )


if __name__ == "__main__":
    target_specimen = "cerebrospinal fluid"
    output_filename = "cerebrospinal fluid_specimen_biomarkers.xlsx"

    print(f"🚀 Starting process to find all biomarkers with specimen: '{target_specimen}'")

    results_df = fetch_specimen_records(target_specimen)

    if results_df is not None and not results_df.empty:
        print("\n" + "#" * 50)
        print("### 📊 Final Results ###")
        print("#" * 50)

        print(f"📈 Found {results_df.shape[0]} biomarkers associated with '{target_specimen}'.")

        print(f"💾 Saving results to '{output_filename}'...")
        results_df.to_excel(output_filename, index=False)
        print(f"  ✅ Done! Your file '{output_filename}' is ready.")
    else:
        print("\n--- 🤷 No data was found for the specified specimen. ---")
