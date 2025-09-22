from __future__ import annotations

from typing import Optional

import pandas as pd

from bkb_client import download_with_size_escalation

INITIAL_SEARCH_SIZE = 50_000
MAX_SIZE_ATTEMPTS = 4


def fetch_record_type_records(record_type: str) -> Optional[pd.DataFrame]:
    def payload_factory(size: Optional[int]) -> dict[str, object]:
        payload = {"record_type": record_type}
        if size is not None:
            payload["size"] = size
        return payload

    return download_with_size_escalation(
        payload_factory=payload_factory,
        description=f"record_type '{record_type}'",
        expect_label=record_type,
        initial_size=INITIAL_SEARCH_SIZE,
        max_attempts=MAX_SIZE_ATTEMPTS,
    )


if __name__ == "__main__":
    target_record_type = "biomarker"
    output_filename = f"record_type_{target_record_type}_biomarkers.xlsx"

    print(f"🚀 Starting process to find all records with record_type: '{target_record_type}'")

    results_df = fetch_record_type_records(target_record_type)

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
