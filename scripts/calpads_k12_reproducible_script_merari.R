#META-INFO
#REPRODUCIBLE SCRIPT: CALPADS UPC GRADES K-12, 2020-2024 PREP
#WEBPAGE WITH DATA FILE: https://www.cde.ca.gov/ds/ad/filescupc.asp
#LAST EDITED ON: 3.3.2026
#CREATED BY: Merari Santana-Carbajal

#R packages you need to run the script below
library(psych)
library(tidyverse)
library(janitor)
library(data.table)
library(stringr)
library(readr)
library(glue)
#RUN TO INSTALL CDETIDY PACKAGE
##devtools::install_github("jaubele/cdetidy")
library(cdetidy)

# THIS FOR LEA LEVEL, SCHOOL LEVEL AFTER ####

# Step 1. Save new CDE data to folder on T drive ####
## THIS IS AN EXCEL FILE WITH MULTIPLE TABS, SO DATA FILE IS SAVED MANUALLY


#Function reads the dataset
read_cupc_k12_step2 <- function(start_year,
                                skip = 1, # starts reading at 2nd row (set to what works in your system)
                                level = c("LEA", "School"), # select which tab 
                                raw_dir,
                                show_str = TRUE,
                                show_view = FALSE) {
  
  level <- match.arg(level)
 
   # If level equals "LEA" -> use "LEA-Level CALPADS UPC Data"
  # Otherwise -> use "School-Level CALPADS UPC Data"
  sheet_name <- ifelse(level == "LEA",
                       "LEA-Level CALPADS UPC Data",
                       "School-Level CALPADS UPC Data")
 
   # Extracts the last 2 numbers of the start year and the following year 
   # for naming the dataset ex: start_year = 2019 ==> 1920
  file_code <- paste0(
    substr(as.character(start_year), 3, 4),
    substr(as.character(start_year + 1), 3, 4)
  )
  
  # Creates file name and gives file_path
  file_name <- paste0("cupc", file_code, "-k12.xlsx")
  file_path <- file.path(raw_dir, file_name)
  
  if (!file.exists(file_path)) {
    stop("File not found: ", file_path)
  }
  
  # Step 2. Read in latest CDS data #### 
  df <- readxl::read_excel(file_path, sheet = sheet_name, skip = skip)
  
  # Step 3: Cleaning and renaming columns ####
  df <- df %>% 
    janitor::clean_names()
  
  # Renaming specific long columns to shorter ones
  expected_sources <- c(
    "academic_year",
    "educational_option_type",
    "nslp_provision_status",
    "charter_school_y_n",
    "charter_number",
    "charter_funding_type",
    "calpads_unduplicated_pupil_count_upc",
    "calpads_fall_1_certification_status_y_n",
    "free_reduced_meal_program",
    "frpm_status",
    "english_learner_el"
  )
  
  # Checks datasetmfor missing columns from expected_sources
  missing_sources <- setdiff(expected_sources, names(df))
  if (length(missing_sources) > 0) {
    message("Step 3.1 note: missing source columns in this file: ",
            paste(missing_sources, collapse = ", "))
  }
  
  df <- df %>%
    dplyr::rename(
      year              = dplyr::any_of("academic_year"),
      ed_option_type    = dplyr::any_of("educational_option_type"),
      nslp_status       = dplyr::any_of("nslp_provision_status"),
      charter           = dplyr::any_of("charter_school_y_n"),
      charter_num       = dplyr::any_of("charter_number"),
      charter_funding   = dplyr::any_of("charter_funding_type"),
      calpads_upc_count = dplyr::any_of("calpads_unduplicated_pupil_count_upc"),
      calpads_fall1_cert= dplyr::any_of("calpads_fall_1_certification_status_y_n"),
      frpm              = dplyr::any_of(c("free_reduced_meal_program", "frpm_status")),
      english_learner   = dplyr::any_of("english_learner_el")
    )
  
  
  # Prints a label in console when you look at the data structure 
  # (i.e. 'show_str = TRUE') 
  if (show_str) {
    message("---- str(df) for ", file_name, " (", level, ") ----")
    print(str(df))
  }
  # View dataframe (i.e. 'show_view = TRUE') 
  if (show_view) {
    View(df)
  }
  
  df
}

#t raw_dir <- "/Users/merarisantana/Desktop/OCDE/CALPADS_K-12/data/raw"
#t cupc_1819_lea <- read_cupc_k12_step2(2018, level="LEA", raw_dir=raw_dir)
#t cupc_1819_school <- read_cupc_k12_step2(2018, level="School", raw_dir=raw_dir)



# Function for Step 4: Changing column types to integer and suppresion scan
cupc_k12_step4_types <- function(df,
                                 int_cols = c(      
                                   "total_enrollment",
                                   "frpm",
                                   "foster",
                                   "homeless",
                                   "migrant_program",
                                   "direct_certification",
                                   "unduplicated_frpm_eligible_count",
                                   "english_learner",
                                   "calpads_upc_count",
                                   "tribal_foster_youth"   # appears in year 2025
                                 ),
                                 show_str_before = TRUE,
                                 show_str_after = TRUE,
                                 warn_missing = TRUE) {
  
  ## Step 4.1: Suppression ####
  ### some columns have suppressed data with an * which we want to remove, but we want to know if it was suppressed
  ### to differentiate between NA and suppressed, we create a dummy variable
  
  ### star_scan scans all columns for "*" and then with $columns on the end, creates a list of all columns in 
  ### our data with suppression. We then assign the list an object name suppression_cols to use later
 
  # Step 4.1: Suppression (scan ALL columns)
  suppression_cols <- cdetidy::star_scan(df)$columns
  
  # Optional: message about *any* suppression anywhere
  if (length(suppression_cols) > 0) {
    message("Suppression '*' detected in columns: ",
            paste(suppression_cols, collapse = ", "))
  } else {
    message("No suppression '*' detected in any column.")
  }
  
  # Optional safety: warn if suppression touches integer conversion columns
  suppressed_int_cols <- intersect(suppression_cols, intersect(int_cols, names(df)))
  if (length(suppressed_int_cols) > 0) {
    message("WARNING: Suppression '*' appears in columns you plan to convert to integer: ",
            paste(suppressed_int_cols, collapse = ", "),
            ". Converting now may lose suppression info unless you create flags first.")
  }
  
  # Looks at data structure before column type conversion
  if (show_str_before) {
    message("---- str(df) BEFORE type conversion ----")
    print(str(df))
  }
  
  # int_cols: columns you expect to convert to integers
  # names(df): columns that actually exist in the dataset
  
  # Finds columns that exist in BOTH lists
  present <- intersect(int_cols, names(df)) 
 
   # Finds expected columns that are NOT in the dataset
  missing <- setdiff(int_cols, names(df))
 
  # Skips missing columns and thus prevents crashing
  if (warn_missing && length(missing) > 0) {
    message("Step 4 note: these expected columns were NOT found and were skipped: ",
            paste(missing, collapse = ", "))
  }
  
  # Convert only columns that exist to integer
  df <- df %>%
    dplyr::mutate(dplyr::across(dplyr::all_of(present), ~ as.integer(.x)))
  
  # Looks at data structure after column type conversion
  if (show_str_after) {
    message("---- str(df) AFTER type conversion ----")
    print(str(df))
  }
  # Return BOTH: the cleaned df and the stored suppression column list
  list(
    df = df,
    suppression_cols = suppression_cols
  )
}


#t step4_out <- cupc_k12_step4_types(cupc_1819_lea)

#t cupc_1819_lea_step4 <- step4_out$df
#t suppression_cols <- step4_out$suppression_cols


