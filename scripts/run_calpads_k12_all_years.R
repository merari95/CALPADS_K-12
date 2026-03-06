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

years <- 2018:2024
levels <- c("LEA", "School")

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
    
    message("Finished start_year = ", yr, " | level = ", lvl)
  }
}