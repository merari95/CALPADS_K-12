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

#t Example Usage:
raw_dir <- "/Users/merarisantana/Desktop/OCDE/CALPADS_K-12/data/raw"
cupc_1819_lea <- read_cupc_k12_step2(2018, level="LEA", raw_dir=raw_dir)
cupc_1819_school <- read_cupc_k12_step2(2018, level="School", raw_dir=raw_dir)



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

#t Example usage:
step4_out <- cupc_k12_step4_types(cupc_1819_lea) # this stores the entire output
#t cupc_1819_lea_step4 <- step4_out$df # this extracts the cleaned dataframe
#t suppression_cols <- step4_out$suppression_cols #this extracts the suppression columns, if any



# Step 4.2 function: create numeric/dummy versions of selected categorical fields
# - Skips gracefully if a column doesn't exist in a given year
# - Logs what was recoded vs skipped
# - Optional validate=TRUE prints cross-tabs for any created columns


# Helper: only apply a mutate block if a source column exists
# verbose is a logical argument (TRUE/FALSE) that controls whether 
# the function prints messages about what it’s doing.
mutate_if_exists <- function(df, colname, fn, verbose = TRUE) {
  
  # Check that the input 'df' is actually a data frame.
  # If it is not, stop execution and return an error message.
  if (!is.data.frame(df)) stop("df must be a data.frame")
  
  # Check if the column name we want to modify exists in the dataframe.
  if (colname %in% names(df)) {
    
    # If verbose = TRUE, print a message in the console telling us
    # which column is currently being recoded.
    if (isTRUE(verbose)) message("Recoding: ", colname)
    
    # Run the function passed in as 'fn' on the dataframe.
    # 'fn' will usually contain a mutate() block that transforms the column.
    # The result of fn(df) should be a modified dataframe.
    fn(df)
  } else {
    
    # If the column does NOT exist in the dataframe,
    # print a message saying it is being skipped.
    if (isTRUE(verbose)) message("Skipping missing column: ", colname)
    
    # Return the dataframe unchanged.
    df
  }
}


#- Optional validate=TRUE prints BEFORE + AFTER tables + str() (like original script)
# New helper to match original script validation (table BEFORE + table AFTER + str)
validate_recode <- function(df, orig, coded) {
  # only run if both columns exist
  if (!all(c(orig, coded) %in% names(df))) return(invisible(NULL))
  
  message("\n--- Validation: ", orig, " -> ", coded, " ---")
  
  # BEFORE: category counts (matches first table() call)
  print(table(df[[orig]], useNA = "ifany"))
  
  # AFTER: cross-tab of original vs coded (matchessecond table() call)
  print(table(df[[orig]], df[[coded]], useNA = "ifany"))
  
  # structure check (matches your str() call)
  str(df[[coded]])
}

# validate: whether to print validation tables at the end
# 'L' guarantees the output type is integer type (and not a type double)
cupc_k12_school_step4_2_dummies <- function(df, validate = FALSE, verbose = TRUE) {
  stopifnot(is.data.frame(df))
  
  # helper: replace literal "N/A" with NA after coercing to character
  naize <- function(x) dplyr::na_if(as.character(x), "N/A")
  
  # ---- charter -> charter_dummy
  df <- mutate_if_exists(df, "charter", function(d) {
    d %>%
      mutate(
        charter = naize(charter),
        charter_dummy = case_when(
          charter == "Yes" ~ 1L,
          charter == "No"  ~ 0L,
          TRUE             ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- school_type -> school_type_num
  df <- mutate_if_exists(df, "school_type", function(d) {
    d %>%
      mutate(
        school_type = naize(school_type),
        school_type_num = case_when(
          school_type == "Alternative Schools of Choice" ~ 1L,
          school_type == "Continuation High Schools" ~ 2L,
          school_type == "County Community" ~ 3L,
          school_type == "District Community Day Schools" ~ 4L,
          school_type == "Elemen Schools In 1 School Dist. (Public)" ~ 5L,
          school_type == "Elementary Schools (Public)" ~ 6L,
          school_type == "High Schools (Public)" ~ 7L,
          school_type == "High Schools In 1 School Dist. (Public)" ~ 8L,
          school_type == "Intermediate/Middle Schools (Public)" ~ 9L,
          school_type == "Junior High Schools (Public)" ~ 10L,
          school_type == "Juvenile Court Schools" ~ 11L,
          school_type == "K-12 Schools (Public)" ~ 12L,
          school_type == "Opportunity Schools" ~ 13L,
          school_type == "Special Education Schools (Public)" ~ 14L,
          school_type == "Preschool" ~ 15L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- ed_option_type -> ed_option_type_num
  df <- mutate_if_exists(df, "ed_option_type", function(d) {
    d %>%
      mutate(
        ed_option_type = naize(ed_option_type),
        ed_option_type_num = case_when(
          ed_option_type == "Alternative School of Choice" ~ 1L,
          ed_option_type == "Community Day School" ~ 2L,
          ed_option_type == "Continuation School" ~ 3L,
          ed_option_type == "County Community School" ~ 4L,
          ed_option_type == "District Special Education Consortia School" ~ 5L,
          ed_option_type == "Home and Hospital" ~ 6L,
          ed_option_type == "Juvenile Court School" ~ 7L,
          ed_option_type == "Opportunity School" ~ 8L,
          ed_option_type == "Special Education School" ~ 9L,
          ed_option_type == "Traditional" ~ 10L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- nslp_status -> nslp_status_num (may be missing in later years)
  df <- mutate_if_exists(df, "nslp_status", function(d) {
    d %>%
      mutate(
        nslp_status = naize(nslp_status),
        nslp_status_num = case_when(
          nslp_status == "Breakfast Provision 2" ~ 1L,
          nslp_status == "CEP" ~ 2L,
          nslp_status == "Lunch Provision 2" ~ 3L,
          nslp_status == "Provision 1" ~ 4L,
          nslp_status == "Provision 2" ~ 5L,
          nslp_status == "Provision 3" ~ 6L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- charter_funding -> charter_funding_num
  df <- mutate_if_exists(df, "charter_funding", function(d) {
    d %>%
      mutate(
        charter_funding = naize(charter_funding),
        charter_funding_num = case_when(
          charter_funding == "Directly funded" ~ 1L,
          charter_funding == "Locally funded" ~ 2L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- irc -> irc_num
  df <- mutate_if_exists(df, "irc", function(d) {
    d %>%
      mutate(
        irc = naize(irc),
        irc_num = case_when(
          irc == "N" ~ 0L,
          irc == "Y" ~ 1L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- low_grade -> low_grade_num
  df <- mutate_if_exists(df, "low_grade", function(d) {
    d %>%
      mutate(
        low_grade = naize(low_grade),
        low_grade_num = case_when(
          low_grade %in% as.character(1:12) ~ suppressWarnings(as.integer(low_grade)),
          low_grade == "Adult" ~ 13L,
          low_grade == "K" ~ 14L,
          low_grade == "P" ~ 15L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- high_grade -> high_grade_num
  df <- mutate_if_exists(df, "high_grade", function(d) {
    d %>%
      mutate(
        high_grade = naize(high_grade),
        high_grade_num = case_when(
          high_grade %in% as.character(1:13) ~ suppressWarnings(as.integer(high_grade)),
          high_grade == "Adult" ~ 14L,
          high_grade == "Post-Secondary" ~ 15L,
          high_grade == "K" ~ 16L,
          high_grade == "P" ~ 17L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- calpads_fall1_cert -> calpads_fall1_cert_num
  df <- mutate_if_exists(df, "calpads_fall1_cert", function(d) {
    d %>%
      mutate(
        calpads_fall1_cert_num = case_when(
          as.character(calpads_fall1_cert) ==
            "In Expected List But We Do Not Have Data For This School/LEA" ~ 0L,
          as.character(calpads_fall1_cert) == "Y" ~ 1L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  

  # ---- Ensure any created *_num / *_dummy columns are integers
  # uses intersect() because only checks columns that were created (and 
  # ignores columns not in dataset)
  created_cols <- intersect(
    c(
      "charter_dummy", "school_type_num", "ed_option_type_num", "nslp_status_num",
      "charter_funding_num", "irc_num", "low_grade_num", "high_grade_num",
      "calpads_fall1_cert_num"
    ),
    names(df)
  )
  # coerces columns to integer
  if (length(created_cols) > 0) {
    df <- df %>% mutate(across(all_of(created_cols), as.integer))
  }
  
  # ---- Optional validation output
  if (isTRUE(validate)) {
    # prints BEFORE table + AFTER cross-tab + str() for each recode pair; just like original script
    message("Validation output (before + after + str), only when columns exist:")
    
    validate_recode(df, "charter", "charter_dummy")
    validate_recode(df, "school_type", "school_type_num")
    validate_recode(df, "ed_option_type", "ed_option_type_num")
    validate_recode(df, "nslp_status", "nslp_status_num")
    validate_recode(df, "charter_funding", "charter_funding_num")
    validate_recode(df, "irc", "irc_num")
    validate_recode(df, "low_grade", "low_grade_num")
    validate_recode(df, "high_grade", "high_grade_num")
    validate_recode(df, "calpads_fall1_cert", "calpads_fall1_cert_num")
  }
  
  df
}

#t Example usage:
cupc_1819_school <- cupc_k12_school_step4_2_dummies(
  cupc_1819_school,
  validate = TRUE,
  verbose = TRUE
)


# Step 4.3: convert academic year "YYYY-YYYY" to two-digit integer
cupc_k12_step4_3_year <- function(df) {
  
  df %>%
    mutate(
      year = as.integer(stringr::str_sub(as.character(year), -2, -1))
    )
}

#t Example Usage
cupc_1819_school <- cupc_k12_step4_3_year(cupc_1819_school)

# Step 5: Pad CDS codes and create cds variable ####
### this custom function creates a CDS code variable and ensures that all CDS codes are the correct length
### CDS code is the unique identifier the state gives to each school. We have the three pieces of CDS 
### code in this file (county, district and school code) so we concatenate those to make cds
cupc_k12_step5 <- function(df) {
  cdetidy::pad_cds_codes(df)
}

#t Example Usage
cupc_1819_school <- cupc_k12_step5(cupc_1819_school)

# Step 6: Export Data ####
## Step 6.1: Flat csv file ####
## we want this version that is less messy for REDI staff and other folks who want to work with data 
## but are not going to be doing visualizations
#~ fwrite(cupc_1819_k12, "T:/CDE data releases/Enrollment Data/CALPADS UPC Grades K-12/2018-19/cupc_k12_LEA_19_clean.csv")
#~ flat_csv_out_path <- "T:/CDE data releases/Enrollment Data/CALPADS UPC Grades K-12/2018-19/cupc_k12_LEA_19_clean.csv" #@
flat_csv_out_path <- "/Users/merarisantana/Desktop/OCDE/CALPADS_K-12/data/processed/cupc_k12_LEA_19_clean.csv" #@

# Function exports Flat CSV file
export_flat_csv <- function(df) {
  fwrite(df, flat_csv_out_path)
  df
}
#t Example Usage
cupc_1819_school <- export_flat_csv(cupc_1819_school)

# Step 6.2: Create FACT table (numeric-only) + validate primary key
cupc_k12_step6_2_fact <- function(df,
                                  pk = "cds",
                                  drop_cols = c(
                                    "county_name", "district_name", "school_name",
                                    "charter", "district_type", "school_type", "ed_option_type",
                                    "nslp_status", "charter_funding", "irc",
                                    "low_grade", "high_grade", "calpads_fall1_cert"
                                  ),
                                  full_run = TRUE) {
  stopifnot(is.data.frame(df))
  
  # Drop only columns that exist (prevents errors across years)
  cols_to_drop <- intersect(drop_cols, names(df))
  
  df_fact <- df %>%
    dplyr::select(-dplyr::all_of(cols_to_drop))
  
  # Validate primary key (will error if primary key (pk) missing)
  validate_primary_key(df_fact, pk, full_run = full_run)
  
  df_fact
}

#t Example Usage
cupc_1819_k12_fact <- cupc_k12_step6_2_fact(cupc_1819_school, pk = "cds", full_run = TRUE)
