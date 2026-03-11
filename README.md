# CALPADS K-12 UPC Pipeline (Refactored)

**Author:** Merari Santana-Carbajal

**Organization:** Orange County Department of Education

**Last Updated:** 03/10/2026

## Overview

This project refactors the original CALPADS K-12 UPC processing script into a reproducible pipeline that processes multiple academic years and both reporting levels (LEA and School) automatically.

The pipeline:

1.  Reads raw CALPADS UPC Excel files
2.  Standardizes variable names and data types
3.  Creates coded/dummy variables
4.  Generates a cleaned flat CSV
5.  Builds fact and dimension tables
6.  Validates primary keys
7.  Optionally exports tables to the OCDE server

The script is designed so that future CALPADS UPC releases can be processed with minimal changes.

## Project Structure

```         
CALPADS_K-12/
│
├─ run_calpads_k12_all_years.R
├─ R/
│   └─ calpads_k12_functions.R
│
├─ data/
│   ├─ raw/            # Raw CALPADS UPC Excel files
│   ├─ processed/      # Clean flat CSV outputs
│   └─ final_local/    # Fact and dimension tables saved locally
```

## How to Run the Pipeline

The datasets required to run the pipeline are already included in the `data/raw/` folder.

To run the pipeline:

1.  Open the script:

```         
run_calpads_k12_all_years.R
```

2.  Run the script.

The pipeline will automatically:

-   process all academic years listed in years

-   process both LEA and School levels

-   generate cleaned datasets

-   create fact and dimension tables

-   preview export metadata

## Exporting Tables to the OCDE Server

By default, the pipeline does not export tables to the OCDE server.

To enable export, change this line in `run_calpads_k12_all_years.R`:

```         
run_final_export <- FALSE
```

to:

```         
run_final_export <- TRUE
```

When enabled, the pipeline writes fact and dimension tables to the OCDE server using `safe_fwrite().`

## How to Add a New Year

To process a new CALPADS UPC release:

**Step 1**

Add the new Excel file to:

```         
data/raw/
```

Example:

```         
cupc2627-k12.xlsx 
```

**Step 2**

Update the `years` variable in:

```         
run_calpads_k12_all_years.R
```

Example:

```         
years <- 2019:2026
```

**Note:** The `years` variable represents the **starting academic year** (e.g., 2026 corresponds to the 2026–2027 academic year). The pipeline internally adds one year to generate the final reporting year.

**Step 2a: Running a Single Year (Optional)**

If you only want to process **one academic year** (for example, when adding a new release) you can modify the `years` variable to include only that year.

Example:

```         
years <- 2026
```

This will run the pipeline only for the **2026–2027 academic year** and prevent re-processing or overwriting outputs for previous years.

**Step 3**

Run the script again.

The pipeline will automatically:

-   detect the new year

-   generate the cleaned dataset

-   create fact and dimension tables

-   export tables (if export is enabled)

No additional code changes are required.

## Key Improvements Over the Original Script

### Reproducible Pipeline

The original script processed **one academic year at a time** and required manual editing of file paths and variables.

This refactored pipeline:

-   processes **multiple years automatically**

-   processes **both LEA and School levels**

-   dynamically generates file names and table names

This makes the script **reusable for future CALPADS UPC releases.**

### Portable File Paths

The refactored pipeline uses:

```         
here::here()
```

instead of hard-coded working directories, allowing the script to run from the project root without modifying paths.

### Modular Functions

Processing logic was moved into reusable functions in:

```         
R/calpads_k12_functions.R
```

This improves:

-   readability

-   maintainability

-   debugging

## Changes to Recoding Logic

Several recoding rules were updated to improve robustness and consistency across years.

### School Type

```         
"Special Ed (Public)" OR "Special Education Schools (Public)" → 14
```

-   Handles both naming variations.

```         
"District Office" → 16
```

-   Added to handle cases appearing in newer datasets.

### NSLP Status

Added category:

```         
"Not Participating" → 7
```

The NSLP coding scheme was standardized to include all known categories:

| nslp_status           | nslp_status_num |
|-----------------------|-----------------|
| Breakfast Provision 2 | 1               |
| CEP                   | 2               |
| Lunch Provision 2     | 3               |
| **Provision** **1**   | **4**           |
| Provision 2           | 5               |
| Provision 3           | 6               |
| Not Participating     | 7               |

In the original scripts, "Provision 1" was not included, which caused "Provision 2" to be coded as 4.

Including "Provision 1" ensures the coding scheme is **complete** and **consistent** **across** **years**, even when certain categories are absent in a dataset.

### High Grade

```         
c("Post Secondary", "Post-Secondary") → 15
```

-   Handles both label variations.

## Dimension Table Updates

Two additional dimensions are now exported to the OCDE server:

-   `school_type`

-   `charter`

These dimensions were created in the original script but were not included in the export section.

Dimension descriptions were also slightly standardized for clarity.

## Notes

Local copies of fact and dimension tables are saved to:

```         
data/final_local/
```

for verification before exporting to the OCDE server.

Export previews are printed during runtime so table names and metadata can be reviewed before enabling server export.
