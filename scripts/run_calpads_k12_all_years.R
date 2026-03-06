# =========================================
# Run CALPADS K-12 pipeline for multiple years
# =========================================
# This script sources reusable functions and runs the full CALPADS K-12
# pipeline for each requested academic year and level (LEA and School).
# Outputs are stored in a named results list and flat CSV files are written
# to the processed data folder.

library(psych)
library(tidyverse)
library(janitor)
library(data.table)
library(stringr)
library(readr)
library(glue)
library(cdetidy)

source("R/calpads_k12_functions.R")

raw_dir <- "data/raw"
processed_dir <- "data/processed"

if (!dir.exists(processed_dir)) dir.create(processed_dir, recursive = TRUE)

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

# =========================================
# Years and levels to run
# =========================================
# Start with one year while testing, then expand to all years.
years <- 2018
levels <- c("LEA", "School")

# Example for full run:
# years <- 2018:2024

results <- list()

for (yr in years) {
  for (lvl in levels) {
    message("Running start_year = ", yr, " | level = ", lvl)
    
    run_name <- paste0(yr, "_", lvl)
    
    results[[run_name]] <- run_cupc_k12_year_level(
      start_year = yr,
      level = lvl,
      raw_dir = raw_dir,
      processed_dir = processed_dir,
      validate_dummies = FALSE,
      verbose = TRUE
    )
    
    # -----------------------------------------
    # Optional: preview dimension export metadata
    # -----------------------------------------
    # data_year is the ending academic year
    # Example: start_year = 2018 -> data_year = 2019
    #
    # cupc_k12_export_dims(
    #   dims = results[[run_name]]$dims,
    #   specs = dim_export_specs,
    #   data_year = yr + 1,
    #   do_export = FALSE
    # )
    
    # -----------------------------------------
    # Optional: preview fact table export metadata
    # -----------------------------------------
    # Example LEA fact table name pattern:
    #   cupc_k12_19
    #
    # Example School fact table name pattern:
    #   cupc_k12_school_fact_19
    #
    # if (lvl == "LEA") {
    #   cupc_k12_fact_export(
    #     df_fact = results[[run_name]]$fact,
    #     data_year = yr + 1,
    #     table_prefix = "cupc_k12",
    #     do_export = FALSE
    #   )
    # } else {
    #   cupc_k12_fact_export(
    #     df_fact = results[[run_name]]$fact,
    #     data_year = yr + 1,
    #     table_prefix = "cupc_k12_school_fact",
    #     do_export = FALSE
    #   )
    # }
    
    message("Finished start_year = ", yr, " | level = ", lvl)
  }
}