# BiomarkerKB Data Extraction Scripts

This project contains a collection of Python scripts designed to programmatically interact with the [BiomarkerKB.org](https://www.biomarkerkb.org/) API. These scripts automate the process of searching for and downloading biomarker data based on various criteria.

The scripts use a two-step API workflow:
1.  **Create a List**: A `POST` request is sent to the `/biomarker/search` endpoint with a specific query to create a temporary list of biomarkers on the server. The server responds with a unique `list_id`.
2.  **Download Data**: The `list_id` is then sent in a `POST` request to the `/data/list_download` endpoint to retrieve the complete dataset in CSV format.

---

##  Prerequisites

Before running these scripts, you need to have Python 3 installed, along with the following libraries:

* **requests**: For making HTTP API calls.
* **pandas**: For handling and processing the data.
* **openpyxl**: For saving the final data to `.xlsx` Excel files.

You can install all the necessary libraries with a single command:
```bash
pip install requests pandas openpyxl

```
---
## Scripts and Usage
Below is a description of each script and how to run it.

1. General Biomarker list (biomarker_by_entity.py)

This is the main script for enriching a custom list of biomarker names. It processes each biomarker individually, combines all the results into a single table, and includes placeholder rows for any biomarkers that could not be found. It also provides a final summary of the process.

To use it:

* Open the script and modify the biomarkers_to_enrich list with your own identifiers.
* Run the script from your terminal:

```bash
python biomaerker_by_entity.py
```
* The output will be saved to a file named biomarker_results.xlsx.

2.  Extract X Specimen Biomarkers

This script is specifically designed to find and download all biomarker records where the associated specimen is "X".

To use it:
* Run the script from your terminal:

```bash
python Biomarker_by_specimen.py
```
* The output will be saved to a file named X_biomarkers.xlsx.

3. Extract by Record Type 

This script finds and downloads all records that match a specific record_type, such as "biomarker".

To use it:
* Open the script and modify the record_type variable with your desired type.
* Run the script from your terminal:
```bash
python Biomarker_by_redordtype.py
```

* The output will be saved to a file named record_type_biomarkers.xlsx.
