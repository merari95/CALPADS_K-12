# =========================================
# Run CALPADS K-12 pipeline for multiple years
# =========================================
# Author: Merari Santana-Carbajal
# Organization: Orange County Department of Education
# Last Updated: 03/08/26

# This script sources reusable functions and runs the full CALPADS K-12
# pipeline for each requested academic year and level (LEA and School).
# Outputs are stored in a named results list and flat CSV files are written
# to the processed data folder.
#
# How to use this script:
# -----------------------
# - For a normal processing run (no OCDE export), keep run_final_export = FALSE.
# - To export final fact and dimension tables to the OCDE server, set
#   run_final_export = TRUE.
# - Preview sections below are safe because do_export = FALSE, so they only
#   print export metadata and do not write to the OCDE server.
library(psych)
library(tidyverse)
library(janitor)
library(data.table)
library(stringr)
library(readr)
library(glue)
library(cdetidy)
library(here)

source(here::here("R", "calpads_k12_functions_merari.R"))


# =========================================
# Export settings
# =========================================
# Set run_final_export to TRUE only when you are ready to write fact and 
# dimension tables to the OCDE server.
run_final_export <- FALSE

# =========================================
# Runtime configuration
# =========================================
levels <- c("LEA", "School")
years <- 2019:2023

raw_dir <- here::here("data", "raw")
processed_dir <- here::here("data", "processed")
final_local_dir <- here::here("data", "final_local")


if (!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)
if (!dir.exists(final_local_dir)) dir.create(final_local_dir, recursive = TRUE)

# =========================================
# Dimension export specifications
# =========================================
# These are used by cupc_k12_export_dims() to build OCDE table names
# and descriptions for dimension table exports.
dim_export_specs <- list(
  entities = list(
    table_name = "cupck12_entities",
    description = "calpads k-12 upc entities dim table"
  ),
  districts = list(
    table_name = "cupck12_districts",
    description = "unique list of school districts calpads k-12 upc"
  ),
  schools = list(
    table_name = "cupck12_schools",
    description = "unique list of schools calpads k-12 upc"
  ),
  ed_option_type = list(
    table_name = "dim_cupck12_ed_option_type",
    description = "dimension table for education option type."
  ),
  nslp_status = list(
    table_name = "dim_cupck12_nslp_status",
    description = "dimension table for nslp status."
  ),
  charter_funding = list(
    table_name = "dim_cupck12_charter_funding",
    description = "dimension table for charter funding in calpads upc k-12."
  ),
  irc = list(
    table_name = "dim_cupck12_irc",
    description = "dimension table for whether an LEA is an independently reporting charter in calpads upc k-12."
  ),
  low_grade = list(
    table_name = "dim_cupck12_low_grade",
    description = "dimension table for an LEAs lowest grade in calpads upc k-12."
  ),
  high_grade = list(
    table_name = "dim_cupck12_high_grade",
    description = "dimension table for an LEAs highest grade in calpads upc k-12."
  ),
  calpads_fall1_cert = list(
    table_name = "dim_cupck12_calpads_fall1_cert",
    description = "dimension table for whether an LEA fall 1 is certified in calpads upc k-12."
  ),
  school_type = list(
    table_name = "dim_cupck12_school_type",
    description = "dimension table for school type in calpads upc k-12."
  ),
  charter = list(
    table_name = "dim_cupck12_charter",
    description = "dimension table for charter indicator in calpads upc k-12."
  )
)


# Store outputs from each year/level run for later inspection
results <- list()

for (yr in years) {
  for (lvl in levels) {
    message("Running start_year = ", yr, " | level = ", lvl)
    
    data_year <- yr + 1
    
    # Use ending academic year + level as the results-list key
    # Example: "2020_LEA"
    run_name <- paste0(data_year, "_", lvl)
    
    results[[run_name]] <- run_cupc_k12_year_level(
      start_year = yr,
      level = lvl,
      raw_dir = raw_dir,
      processed_dir = processed_dir,
      final_local_dir = final_local_dir,
      validate_dummies = TRUE,
      verbose = TRUE,
      run_final_export = run_final_export,
      specs = dim_export_specs
    )
    
    # Verification output: prints summary information and export metadata for review.
    # This does not affect the pipeline or OCDE exports.
    cat("\n=====================================\n")
    cat("Verification for", run_name, "\n")
    cat("=====================================\n")
    
    # # Check fact table size
    cat("\nFact table rows:\n")
    print(nrow(results[[run_name]]$fact))
    
    # # Check suppression columns detected
    cat("\nSuppression columns detected:\n")
    print(results[[run_name]]$suppression_cols)
    
    # # Print dimension tables
    for (dim_name in names(results[[run_name]]$dims)) {
      cat("\n--- Dimension:", dim_name, "---\n")
      print(results[[run_name]]$dims[[dim_name]])
      }
    
    # Note:
    # The preview blocks below print QA/export metadata for review.
    # Because do_export = FALSE, they do not write to the OCDE server.
    # To perform the actual export, set run_final_export = TRUE above.
    # --------------------------------------------------------------
    # Preview dimension export metadata (does not write to server)
    # --------------------------------------------------------------
    # data_year is the ending academic year
    # Example: start_year = 2018 -> data_year = 2019
    #
    cupc_k12_export_dims(
       dims = results[[run_name]]$dims,
       specs = dim_export_specs,
       data_year = data_year,
       do_export = FALSE
     )
    
    # ---------------------------------------------------------------
    # Preview fact table export metadata (does not write to server)
    # ---------------------------------------------------------------
    # Example LEA fact table name pattern:
    #   cupc_k12_19
    #
    # Example School fact table name pattern:
    #   cupc_k12_school_fact_19
    #
    if (lvl == "LEA") {
       cupc_k12_fact_export(
         df_fact = results[[run_name]]$fact,
         data_year = data_year,
         table_prefix = "cupc_k12",
         do_export = FALSE
       )
     } else {
       cupc_k12_fact_export(
       df_fact = results[[run_name]]$fact,
       data_year = data_year,
       table_prefix = "cupc_k12_school_fact",
       do_export = FALSE
       )
     }
    
    message("Finished start_year = ", yr, " | level = ", lvl)
  }
}