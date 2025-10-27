# 📊 Quality Check Python Scripts

## Description

Welcome to the central repository for data quality check (QC) scripts. This repository contains automated scripts designed to ensure the accuracy and completeness of our data across different reports: **RADET**, **HTS**, and **PMTCT_HTS**.

## 🚀 Getting Started

### 1\. Prerequisites

* **Python 3**
* **Google Colab**

### 2\. How to Run the Script

1.  **Download** the specific script you need.

2.  **Open** the file in any text editor (VS Code, Notepad, Sublime Text, etc.).

3.  **Edit the two required variables** at the very top of the script (see the next section).

4.  **Save** the file.

5.  **Run the saved file in your Terminal or Command Prompt or on google colab.**


## 📝 Required Variable Changes

Each script is pre-configured with placeholder variables. You **MUST** update these two variables inside the Python file to point to your local machine's folders:

| Variable Name | Purpose | Example Value to Change |
| :--- | :--- | :--- |
| **`FOLDER_PATH`** | The **input directory** containing the raw data files (Excel or CSV) that the script needs to check. | `"C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/FY26Q1_RADET"` |
| **`OUTPUT_BASE_DIR`** | The **output directory** where the final quality check report (Excel or CSV file) will be saved. | `"C:/Users/oluwabukola.arowolo/OneDrive - Palladium International, LLC/Documents/DataFi/Project_Export_Quality_Check"` |

**⚠️ IMPORTANT:**
  
  * Keep the **quotes** around the file paths\!

-----

## 📁 Available Scripts

This repository contains the following quality check scripts:

| Script Filename | Program Area | Description |
| :--- | :--- | :--- |
| **`qc_radet.py`** | RADET Data | Performs Quality checks specific to RADET report. |
| **`qc_hts.py`** | HTS Data | Performs Quality checks specific to HIV Testing Services (HTS) report. |
| **`qc_pmtct_hts.py`** | PMTCT Data | Performs Quality checks specific to Prevention of Mother-to-Child Transmission (PMTCT) report. |

-----

## ❓ Troubleshooting & Support

  * **Error running the script?** Double-check that your `FOLDER_PATH` and `OUTPUT_BASE_DIR` are correctly formatted and enclosed in quotes.

## Authors & Acknowledgement
-----
## 👥 Main Contributors

  * **[Arowolo Oluwabukola]** ([@Haddy-Oluwabukola](https://github.com/Haddy-Oluwabukola))
  

For further assistance, please contact the main contributor or open an **Issue** on this GitHub page.
