# =========================================
# CALPADS K-12 reusable functions
# =========================================
# Author: Merari Santana-Carbajal
# Organization: Orange County Department of Education
# Last Updated: 03/10/26

# Step 2-3.1: Read and standardize CALPADS UPC data
# -------------------------------------------------
# read_cupc_k12 : Reads the CALPADS UPC Excel file for a given
# academic year and level (LEA-level or School-level). It performs several 
# preprocessing steps so downstream functions can assume a consistent structure:
#
# 1. Dynamically constructs the file name from the starting academic year
#    (e.g., start_year = 2018 --> file "cupc1819-k12.xlsx").
# 2. Selects the appropriate Excel sheet depending on the level (LEA vs School).
# 3. Reads the data using readxl.
# 4. Standardizes column names using janitor::clean_names() so they are lowercase
#    and use underscores instead of spaces.
# 5. Renames several long CALPADS variable names to shorter, consistent names
#    used throughout the pipeline (e.g., academic_year --> year).
# 6. Optionally prints the structure of the dataset or opens a viewer window
#    for debugging during development.
#
# The result is a clean dataframe with consistent column names that can be used
# by later steps in the pipeline (type conversion, dummy creation, fact/dim tables).
#
# Example:
# df <- read_cupc_k12(start_year = 2019, level = "LEA", raw_dir = "data/raw")
read_cupc_k12 <- function(start_year,
                          skip = 0,
                          level = c("LEA", "School"),
                          raw_dir,
                          show_str = FALSE,
                          show_view = FALSE) {
  level <- match.arg(level)
  # If level equals "LEA" -> use "LEA-Level CALPADS UPC Data"
  # Otherwise -> use "School-Level CALPADS UPC Data"
  sheet_name <- ifelse(level == "LEA",
                       "LEA-Level CALPADS UPC Data",
                       "School-Level CALPADS UPC Data")
  
  # Extract the last two digits of start_year and start_year + 1
  # to build the academic-year file code, e.g. 2019 -> "1920"
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
  
  # Read CALPADS UPC data, clean column names, convert literal "N/A" to NA,
  # and rename selected variables to standardized names used in the pipeline.
  df <- readxl::read_excel(file_path, sheet = sheet_name, skip = skip) |>
    janitor::clean_names() |>
    dplyr::mutate(dplyr::across(where(is.character), ~ dplyr::na_if(.x, "N/A"))) |> 
    dplyr::rename(
      year = dplyr::any_of("academic_year"),
      ed_option_type = dplyr::any_of("educational_option_type"),
      nslp_status = dplyr::any_of("nslp_provision_status"),
      charter = dplyr::any_of("charter_school_y_n"),
      charter_num = dplyr::any_of("charter_number"),
      charter_funding = dplyr::any_of("charter_funding_type"),
      calpads_upc_count = dplyr::any_of("calpads_unduplicated_pupil_count_upc"),
      calpads_fall1_cert = dplyr::any_of("calpads_fall_1_certification_status_y_n"),
      frpm = dplyr::any_of(c("free_reduced_meal_program", "frpm_status")),
      english_learner = dplyr::any_of("english_learner_el")
    )
  
  if (show_str) {
    message("---- str(df) for ", file_name, " (", level, ") ----")
    print(str(df))
  }
  
  if (show_view) {
    View(df)
  }
  
  df
}

# Step 4-4.1: Types + suppression scan
# ------------------------------------
# cupc_k12_types : Prepares numeric columns for downstream 
# analysis and database export. It performs two main tasks:
#
# 1. Detects suppressed values ("*") in the dataset using cdetidy::star_scan().
#    The function records which columns contain suppression so they can be handled
#    appropriately later in the pipeline if needed.
#
# 2. Converts a predefined set of numeric count variables to integer type.
#    These variables represent enrollment counts and related student subgroup totals.
#
# Additional behavior:
# - If suppression is detected in a column scheduled for integer conversion,
#   a warning is printed.
# - If expected numeric columns are missing from the dataset (which may occur
#   in some years), the function skips them and prints a note.
# - Optional debugging flags allow printing the structure of the dataset
#   before and after type conversion ('show_str_before' and 'show_str_after').
#
# The function returns a list containing:
#   $df                --> the dataframe with updated column types
#   $suppression_cols  --> names of columns containing suppressed values ("*")
#
# Example:
# step4_out <- cupc_k12_types(df)
# df <- step4_out$df
# suppression_cols <- step4_out$suppression_cols
cupc_k12_types <- function(df,
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
                             "tribal_foster_youth"
                           ),
                           show_str_before = FALSE,
                           show_str_after = FALSE,
                           warn_missing = TRUE) {
  
  # Suppression (scan ALL columns)
  suppression_cols <- cdetidy::star_scan(df)$columns
  
  # Optional: message about any suppression anywhere
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
  
  # Optionally print the data structure before type conversion
  if (show_str_before) {
    message("---- str(df) BEFORE type conversion ----")
    print(str(df))
  }
  
  present <- intersect(int_cols, names(df))
  missing <- setdiff(int_cols, names(df))
  
  # Skips missing columns and thus prevents crashing
  if (warn_missing && length(missing) > 0) {
    message("Step 4 note: these expected columns were NOT found and were skipped: ",
            paste(missing, collapse = ", "))
  }
  
  # Converts existing columns to integer
  df <- df |>
    dplyr::mutate(dplyr::across(dplyr::all_of(present), ~ as.integer(.x)))
  
  # Optionally print the data structure after type conversion
  if (show_str_after) {
    message("---- str(df) AFTER type conversion ----")
    print(str(df))
  }
  
  # Returns BOTH: the cleaned df and the stored suppression column list
  list(
    df = df,
    suppression_cols = suppression_cols
  )
}

# Helper
# Safely applies a mutation only if the specified column exists in the dataframe.
# This prevents errors when columns differ across years or datasets.
mutate_if_exists <- function(df, colname, fn, verbose = TRUE) {
  if (!is.data.frame(df)) stop("df must be a data.frame")
  
  if (colname %in% names(df)) {
    if (isTRUE(verbose)) message("Recoding: ", colname)
    fn(df)
  } else {
    if (isTRUE(verbose)) message("Skipping missing column: ", colname)
    df
  }
}

# Helper for validation.
# Prints tables to verify that a categorical variable was correctly recoded
# into its numeric/dummy version.
validate_recode <- function(df, orig, coded) {
  if (!all(c(orig, coded) %in% names(df))) return(invisible(NULL))
  
  message("\n--- Validation: ", orig, " -> ", coded, " ---")
  
  # Original category counts
  print(table(df[[orig]], useNA = "ifany"))
  
  # Cross-tab of original values against coded values
  print(table(df[[orig]], df[[coded]], useNA = "ifany"))
  
  # Cross-tab of original values against coded values
  str(df[[coded]])
}

# Step 4.2: Dummy/coded versions of categorical variables
# -------------------------------------------------------
# cupc_k12_dummies : Recodes selected categorical CALPADS
# variables into numeric dummy/coded versions.
#
# It uses mutate_if_exists() so the function can run safely even when some
# variables are missing in certain years or datasets.
#
# Additional behavior:
# - Converts literal "N/A" values to NA before recoding.
# - Ensures all created dummy/coded columns are stored as integers.
# - Optionally prints validation tables to confirm that original categories
#   were mapped correctly to their numeric versions.
#
# Example:
# df <- cupc_k12_dummies(df, validate = TRUE, verbose = TRUE)
cupc_k12_dummies <- function(df, validate = FALSE, verbose = TRUE) {
  stopifnot(is.data.frame(df))
  
  # helper: replace literal "N/A" with NA after coercing to character
  naize <- function(x) dplyr::na_if(as.character(x), "N/A")
  
  # ---- charter -> charter_dummy
  df <- mutate_if_exists(df, "charter", function(d) {
    d |>
      dplyr::mutate(
        charter = naize(charter),
        charter_dummy = dplyr::case_when(
          charter == "Yes" ~ 1L,
          charter == "No" ~ 0L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- school_type -> school_type_num
  df <- mutate_if_exists(df, "school_type", function(d) {
    d |>
      dplyr::mutate(
        school_type = naize(school_type),
        
        # Standardize one school_type label before numeric recoding
        school_type = dplyr::case_when(
          school_type == "Special Ed (Public)" ~ "Special Education Schools (Public)",
          TRUE ~ school_type
        ),
        
        school_type_num = dplyr::case_when(
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
          school_type == "District Office" ~ 16L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- ed_option_type -> ed_option_type_num
  df <- mutate_if_exists(df, "ed_option_type", function(d) {
    d |>
      dplyr::mutate(
        ed_option_type = naize(ed_option_type),
        ed_option_type_num = dplyr::case_when(
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
    d |>
      dplyr::mutate(
        nslp_status = naize(nslp_status),
        nslp_status_num = dplyr::case_when(
          nslp_status == "Breakfast Provision 2" ~ 1L,
          nslp_status == "CEP" ~ 2L,
          nslp_status == "Lunch Provision 2" ~ 3L,
          nslp_status == "Provision 1" ~ 4L,
          nslp_status == "Provision 2" ~ 5L,
          nslp_status == "Provision 3" ~ 6L,
          nslp_status == "Not Participating" ~ 7L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- charter_funding -> charter_funding_num
  df <- mutate_if_exists(df, "charter_funding", function(d) {
    d |>
      dplyr::mutate(
        charter_funding = naize(charter_funding),
        charter_funding_num = dplyr::case_when(
          charter_funding == "Directly funded" ~ 1L,
          charter_funding == "Locally funded" ~ 2L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- irc -> irc_num
  df <- mutate_if_exists(df, "irc", function(d) {
    d |>
      dplyr::mutate(
        irc = naize(irc),
        irc_num = dplyr::case_when(
          irc == "N" ~ 0L,
          irc == "Y" ~ 1L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- low_grade -> low_grade_num
  # Numeric grades remain numeric; special grade labels are assigned custom codes
  df <- mutate_if_exists(df, "low_grade", function(d) {
    d |>
      dplyr::mutate(
        low_grade = naize(low_grade),
        low_grade_num = dplyr::case_when(
          low_grade %in% as.character(1:12) ~ suppressWarnings(as.integer(low_grade)),
          low_grade == "Adult" ~ 13L,
          low_grade == "K" ~ 14L,
          low_grade == "P" ~ 15L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- high_grade -> high_grade_num
  # Numeric grades remain numeric; special grade labels are assigned custom codes
  df <- mutate_if_exists(df, "high_grade", function(d) {
    d |>
      dplyr::mutate(
        high_grade = naize(high_grade),
        high_grade_num = dplyr::case_when(
          high_grade %in% as.character(1:13) ~ suppressWarnings(as.integer(high_grade)),
          high_grade == "Adult" ~ 14L,
          high_grade %in% c("Post Secondary", "Post-Secondary") ~ 15L,
          high_grade == "K" ~ 16L,
          high_grade == "P" ~ 17L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # ---- calpads_fall1_cert -> calpads_fall1_cert_num
  df <- mutate_if_exists(df, "calpads_fall1_cert", function(d) {
    d |>
      dplyr::mutate(
        calpads_fall1_cert_num = dplyr::case_when(
          as.character(calpads_fall1_cert) ==
            "In Expected List But We Do Not Have Data For This School/LEA" ~ 0L,
          as.character(calpads_fall1_cert) == "Y" ~ 1L,
          TRUE ~ NA_integer_
        )
      )
  }, verbose = verbose)
  
  # Ensure the recoded numeric/dummy columns created above are stored as integers
  # Use intersect() to keep only recode columns that actually exist in df.
  # This avoids errors when some variables are missing in a given dataset/year.
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
    df <- df |>
      dplyr::mutate(dplyr::across(dplyr::all_of(created_cols), as.integer))
  }
  
  # ---- Optional validation output
  if (isTRUE(validate)) {
    
    # Print original category counts, cross-tab of original vs coded values,
    # and the structure of each coded column
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

# Step 4.3: Standardize academic year
# -----------------------------------
# cupc_k12_year : Converts the academic year field (e.g., "2018-2019") 
# into a two-digit numeric year (e.g., 19) so it matches the format used in 
# table names and downstream database exports.
cupc_k12_year <- function(df) {
  df |>
    dplyr::mutate(
      year = as.integer(stringr::str_sub(as.character(year), -2, -1))
    )
}

# Step 5: Create standardized CDS identifier
# -------------------------------------------
# cupc_k12_cds : Applies cdetidy::pad_cds_codes() to ensure 
# county, district, and school codes are properly padded with leading zeros 
# and concatenated into a standardized CDS identifier used as the primary key for schools.
cupc_k12_cds <- function(df) {
  cdetidy::pad_cds_codes(df)
}

# Step 6.1: Export flat CSV for local use
# ---------------------------------------
# export_flat_csv : Writes the cleaned dataset to a flat CSV file for REDI staff 
# who want to work with data but are not going to be doing visualizations
export_flat_csv <- function(df, path) {
  data.table::fwrite(df, path)
  df
}

# Step 6.2: Create fact table
# ---------------------------
# cupc_k12_fact : Creates a fact table version of the dataset for database export.
# A fact table contains primarily numeric variables used for analysis and joins,
# so descriptive text fields (e.g., names and categorical labels) are removed.
#
# The function also validates that the primary key (default: CDS) uniquely
# identifies each row so the table can safely be exported to the database.
cupc_k12_fact <- function(df,
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
  
  df_fact <- df |>
    dplyr::select(-dplyr::all_of(cols_to_drop))
  
  # Validate primary key: ensures pk column exists and uniquely identifies rows
  validate_primary_key(df_fact, pk, full_run = full_run)
  
  df_fact
}

#! safe_fwrite() only runs when do_export = TRUE
#! default behavior is preview-only to prevent accidental overwrite
# Step 6.2b: Export fact table to OCDE server
# -------------------------------------------
# cupc_k12_fact_export : Exports the fact table to the OCDE server using
# safe_fwrite(). The table name is automatically constructed from the
# table prefix and the two-digit academic year (e.g., 2019 -> "19").
cupc_k12_fact_export <- function(df_fact,
                                 data_year,
                                 table_prefix = "cupc_k12",
                                 table_name = NULL,
                                 data_source = "cde",
                                 data_type = "enrollment",
                                 user_note = "fact file.",
                                 data_description = NULL,
                                 do_export = FALSE) {
  stopifnot(is.data.frame(df_fact))
  stopifnot(is.numeric(data_year), length(data_year) == 1)
  
  # create two-digit suffix for table name (e.g., 2019 -> "19")
  yy <- sprintf("%02d", data_year %% 100)
  
  if (is.null(table_name)) {
    table_name <- paste0(table_prefix, "_", yy)
  }
  
  # default description if not provided
  if (is.null(data_description)) {
    data_description <- paste0("calpads upc k-12 file ", data_year)
  }
  
  # If do_export = TRUE, write the fact table to the OCDE server
  # (use with care in production)
  if (do_export) {
    safe_fwrite(
      df_fact,
      table_name = table_name,
      data_year = data_year,
      data_source = data_source,
      data_description = data_description,
      data_type = data_type,
      user_note = user_note     )
    
    # If do_export = FALSE, print a preview of the export metadata instead
  } else {
    cat("\n--- Preview Fact Export ---\n")
    cat("table_name:", table_name, "\n")
    cat("data_year:", data_year, "\n")
    cat("description:", data_description, "\n")
    cat("rows:", nrow(df_fact), "\n")
  }
  
  invisible(TRUE)
}

# Step 6.3a: Dimension table helper functions 
# (make_dim_if_exists and maybe_as_integer)
# -------------------------------------------
# make_dim_if_exists : Creates a dimension table from selected columns if
# those columns exist in the dataset. The function extracts distinct values,
# optionally sorts them, removes missing values for a key column, and can
# filter out zero-coded values when needed.
make_dim_if_exists <- function(df, cols, arrange_by = NULL, filter_nonzero_col = NULL) {
  if (!all(cols %in% names(df))) return(NULL)
  
  out <- df |>
    dplyr::select(dplyr::all_of(cols)) |>
    dplyr::distinct()
  
  if (!is.null(arrange_by) && arrange_by %in% names(out)) {
    out <- out |>
      dplyr::arrange(.data[[arrange_by]])
  }
  
  if (!is.null(arrange_by) && arrange_by %in% names(out)) {
    out <- out |>
      dplyr::filter(!is.na(.data[[arrange_by]]))
  }
  
  if (!is.null(filter_nonzero_col) && filter_nonzero_col %in% names(out)) {
    out <- out |>
      dplyr::filter(.data[[filter_nonzero_col]] != 0)
  }
  
  out
}

# maybe_as_integer : Converts a column to integer if it exists in the dataframe.
# This is useful when preparing dimension tables where key fields must be numeric.
maybe_as_integer <- function(df, col) {
  if (col %in% names(df)) {
    df |>
      dplyr::mutate(!!col := as.integer(.data[[col]]))
  } else {
    df
  }
}

# Step 6.3b: Build dimension tables
# ---------------------------------
# cupc_k12_dims : Creates a named list of dimension tables from the cleaned
# CALPADS dataset. These tables store unique identifier fields and lookup
# values for categorical variables so they can be linked to the fact table
# in downstream analysis and database exports.
#
# The function uses helper functions to:
# - safely create dimensions only when required columns exist
# - sort dimension tables and apply small cleanup steps where needed
# - convert key identifier columns to integer when needed
# - remove school_code = 0 from the schools dimension, since those rows
#   represent district-level records rather than actual schools
cupc_k12_dims <- function(df) {
  stopifnot(is.data.frame(df))
  
  dims <- list()
  
  # ---- entities dim
  dims$entities <- make_dim_if_exists(
    df,
    cols = c("year", "cds", "county_code", "district_code", "school_code")
  )
  
  # ---- districts dim
  dims$districts <- make_dim_if_exists(
    df,
    cols = c("year", "district_code", "district_name"),
    arrange_by = "district_name"
  )
  if (!is.null(dims$districts)) {
    dims$districts <- maybe_as_integer(dims$districts, "district_code")
  }
  
  # ---- schools dim
  # Filter out rows with school_code = 0, because those represent
  # district-level records rather than actual schools
  dims$schools <- make_dim_if_exists(
    df,
    cols = c("year", "school_code", "school_name"),
    arrange_by = "school_code"
  )
  if (!is.null(dims$schools)) {
    dims$schools <- dims$schools |>
      dplyr::mutate(school_code = as.integer(school_code)) |>
      dplyr::filter(school_code != 0) |>
      dplyr::arrange(school_code)
  }
  
  # ---- categorical lookup dimensions
  # created only when both label and coded columns exist in the dataset
  dims$school_type <- make_dim_if_exists(
    df,
    cols = c("year", "school_type", "school_type_num"),
    arrange_by = "school_type_num"
  )
  
  dims$ed_option_type <- make_dim_if_exists(
    df,
    cols = c("year", "ed_option_type", "ed_option_type_num"),
    arrange_by = "ed_option_type_num"
  )
  
  dims$nslp_status <- make_dim_if_exists(
    df,
    cols = c("year", "nslp_status", "nslp_status_num"),
    arrange_by = "nslp_status_num"
  )
  
  dims$charter_funding <- make_dim_if_exists(
    df,
    cols = c("year", "charter_funding", "charter_funding_num"),
    arrange_by = "charter_funding_num"
  )
  
  dims$charter <- make_dim_if_exists(
    df,
    cols = c("year", "charter", "charter_dummy"),
    arrange_by = "charter_dummy"
  )
  
  dims$irc <- make_dim_if_exists(
    df,
    cols = c("year", "irc", "irc_num"),
    arrange_by = "irc_num"
  )
  
  dims$low_grade <- make_dim_if_exists(
    df,
    cols = c("year", "low_grade", "low_grade_num"),
    arrange_by = "low_grade_num"
  )
  
  dims$high_grade <- make_dim_if_exists(
    df,
    cols = c("year", "high_grade", "high_grade_num"),
    arrange_by = "high_grade_num"
  )
  
  dims$calpads_fall1_cert <- make_dim_if_exists(
    df,
    cols = c("year", "calpads_fall1_cert", "calpads_fall1_cert_num"),
    arrange_by = "calpads_fall1_cert_num"
  )
  
  dims
}

# Step 6.4a: Validate dimension table primary keys
# -----------------------------------------------
# cupc_k12_validate_dims : Checks that each dimension table has a valid
# primary key before export. The function runs validate_primary_key()
# on each dimension table if it exists, ensuring that key columns
# uniquely identify rows and can safely join to the fact table.
cupc_k12_validate_dims <- function(dims, full_run = TRUE) {
  stopifnot(is.list(dims))
  
  # Safely skips dimensions that don't exist for that year.
  validate_if_present <- function(x, pk) {
    if (is.null(x)) return(invisible(NULL))
    validate_primary_key(x, pk, full_run = full_run)
  }
  
  validate_if_present(dims$entities, "cds")
  validate_if_present(dims$districts, "district_code")
  validate_if_present(dims$schools, "school_code")
  validate_if_present(dims$school_type, "school_type_num")
  validate_if_present(dims$ed_option_type, "ed_option_type_num")
  validate_if_present(dims$nslp_status, "nslp_status_num")
  validate_if_present(dims$charter_funding, "charter_funding_num")
  validate_if_present(dims$charter, "charter_dummy")
  validate_if_present(dims$irc, "irc_num")
  validate_if_present(dims$low_grade, "low_grade_num")
  validate_if_present(dims$high_grade, "high_grade_num")
  validate_if_present(dims$calpads_fall1_cert, "calpads_fall1_cert_num")
  
  invisible(TRUE)
}


#! safe_fwrite() only runs when do_export = TRUE
#! default behavior is preview-only to prevent accidental overwrite
# Step 6.4b: Export dimension tables to OCDE server
# -------------------------------------------------
# cupc_k12_export_dims : Exports dimension tables to the OCDE server using
# safe_fwrite(). Table names are built dynamically from a metadata list
# and the two-digit academic year suffix.
cupc_k12_export_dims <- function(dims,
                                 specs,
                                 data_year,
                                 do_export = FALSE) {
  stopifnot(is.list(dims))
  
  # Create two-digit year suffix for dynamic table names/descriptions
  yy <- sprintf("%02d", data_year %% 100)
  
  # Look up export metadata for each dimension from the specs list
  for (name in names(specs)) {
    dim_df <- dims[[name]]
    
    # Skip dimensions that don't exist for that year
    if (is.null(dim_df)) {
      message("Skipping dimension: ", name)
      next
    }
    
    # Build year-specific table name and description
    base_name <- specs[[name]]$table_name
    suffix <- specs[[name]]$suffix
    
    if (base_name == "dim") {
      table_name <- paste0("dim", yy, "_", suffix)
    } else {
      table_name <- paste0(base_name, "_", yy, "_", suffix)
    }
    
    description <- paste0(specs[[name]]$description, " ", data_year)
    
    # If do_export = TRUE, write the dimension table to the OCDE server
    # (use with care in production)
    if (do_export) {
      safe_fwrite(
        dim_df,
        table_name = table_name,
        dimension_type = "annualized",
        data_source = "cde",
        data_year = data_year,
        data_type = "dim",
        data_description = description,
        user_note = "dim table."
      )
      # If do_export = FALSE, print a preview of the export metadata instead
    } else {
      cat("\n--- Preview Export ---\n")
      cat("dimension:", name, "\n")
      cat("table_name:", table_name, "\n")
      cat("description:", description, "\n")
      cat("rows:", nrow(dim_df), "\n")
    }
  }
  invisible(TRUE)
}

# Pipeline: Run the full CALPADS K-12 workflow for one academic year
# and one reporting level (LEA or School).
# =============================================================================
# This wrapper:
# - reads and cleans the raw CALPADS UPC file
# - creates coded/dummy variables and the CDS key
# - writes a cleaned flat CSV
# - builds the fact table and dimension tables
# - validates primary keys
# - optionally exports final tables to the OCDE server
run_cupc_k12_year_level <- function(start_year,
                                    level = c("LEA", "School"),
                                    raw_dir,
                                    processed_dir,
                                    final_local_dir,
                                    validate_dummies = FALSE,
                                    verbose = TRUE,
                                    run_final_export = FALSE,
                                    specs = NULL) {
  
  level <- match.arg(level)
  
  # ending academic year used in export/file naming
  data_year <- start_year + 1
  
  # Step 2-3.1: Read and standardize CALPADS UPC data
  df <- read_cupc_k12(
    start_year = start_year,
    level = level,
    raw_dir = raw_dir,
    show_str = FALSE,
    show_view = FALSE
  )
  
  # Step 4-4.1: Convert numeric columns and detect suppression
  step4_out <- cupc_k12_types(
    df,
    show_str_before = FALSE,
    show_str_after = FALSE
  )
  
  df <- step4_out$df
  suppression_cols <- step4_out$suppression_cols
  
  # Step 4.2: Create dummy/coded variables
  df <- cupc_k12_dummies(
    df,
    validate = validate_dummies,
    verbose = verbose
  )
  
  # Step 4.3: Standardize academic year
  df <- cupc_k12_year(df)
  
  # Step 5: Create CDS identifier
  df <- cupc_k12_cds(df)
  
  # Step 6.1: Export flat CSV for local use
  yy <- sprintf("%02d", data_year %% 100)
  level_label <- ifelse(level == "LEA", "LEA", "school")
  
  flat_csv_name <- paste0("cupc_k12_", level_label, "_", yy, "_clean.csv")
  flat_csv_path <- file.path(processed_dir, flat_csv_name)
  
  df <- export_flat_csv(df, flat_csv_path)
  
  # Step 6.2: Create fact table
  df_fact <- cupc_k12_fact(df, pk = "cds", full_run = TRUE)
  
  # Step 6.3b: Build dimension tables
  dims <- cupc_k12_dims(df)
  
  # Step 6.4a: Validate dimension table primary keys
  cupc_k12_validate_dims(dims, full_run = TRUE)
  
  # Step 6.4b: Save local copy of fact table for QC/review
  fact_local_name <- ifelse(
    level == "LEA",
    paste0("cupc_k12_", yy, ".csv"),
    paste0("cupc_k12_", yy, "_school_fact.csv")
  )
  fact_local_path <- file.path(final_local_dir, fact_local_name)
  data.table::fwrite(df_fact, fact_local_path)
  
  # Step 6.4c: Save local copies of dimension tables for QC/review
  dim_local_paths <- list()
  
  for (dim_name in names(dims)) {
    if (!is.null(dims[[dim_name]])) {
      
      if (level == "LEA") {
        dim_file_name <- switch(
          dim_name,
          entities = paste0("cupck12_", yy, "_entities.csv"),
          districts = paste0("cupck12_", yy, "_districts.csv"),
          schools = paste0("cupck12_", yy, "_schools.csv"),
          school_type = paste0("dim", yy, "_cupck12_school_type.csv"),
          ed_option_type = paste0("dim", yy, "_cupck12_ed_option_type.csv"),
          nslp_status = paste0("dim", yy, "_cupck12_nslp_status.csv"),
          charter_funding = paste0("dim", yy, "_cupck12_charter_funding.csv"),
          charter = paste0("dim", yy, "_cupck12_charter.csv"),
          irc = paste0("dim", yy, "_cupck12_irc.csv"),
          low_grade = paste0("dim", yy, "_cupck12_low_grade.csv"),
          high_grade = paste0("dim", yy, "_cupck12_high_grade.csv"),
          calpads_fall1_cert = paste0("dim", yy, "_cupck12_calpads_fall1_cert.csv")
        )
      } else {
        dim_file_name <- switch(
          dim_name,
          entities = paste0("cupck12_school_", yy, "_entities.csv"),
          districts = paste0("cupck12_school_", yy, "_districts.csv"),
          schools = paste0("cupck12_school_", yy, "_schools.csv"),
          school_type = paste0("dim", yy, "_cupck12_school_school_type.csv"),
          ed_option_type = paste0("dim", yy, "_cupck12_school_ed_option_type.csv"),
          nslp_status = paste0("dim", yy, "_cupck12_school_nslp_status.csv"),
          charter_funding = paste0("dim", yy, "_cupck12_school_charter_funding.csv"),
          charter = paste0("dim", yy, "_cupck12_school_charter.csv"),
          irc = paste0("dim", yy, "_cupck12_school_irc.csv"),
          low_grade = paste0("dim", yy, "_cupck12_school_low_grade.csv"),
          high_grade = paste0("dim", yy, "_cupck12_school_high_grade.csv"),
          calpads_fall1_cert = paste0("dim", yy, "_cupck12_school_calpads_fall1_cert.csv")
        )
      }
      
      dim_file_path <- file.path(final_local_dir, dim_file_name)
      data.table::fwrite(dims[[dim_name]], dim_file_path)
      dim_local_paths[[dim_name]] <- dim_file_path
    }
  }
  
  
  # Step 6.5: Final export to OCDE server
  # run_final_export controls whether final OCDE server export happens
  if (run_final_export) {
    if (is.null(specs)) {
      stop("specs must be provided when run_final_export = TRUE")
    }
    
    fact_table_name <- if (level == "LEA") {
      paste0("cupc_k12_", yy)
    } else {
      paste0("cupc_k12_", yy, "_school_fact")
    }
    
    cupc_k12_fact_export(
      df_fact = df_fact,
      data_year = data_year,
      table_name = fact_table_name,
      do_export = TRUE
    )
    
    cupc_k12_export_dims(
      dims = dims,
      specs = specs,
      data_year = data_year,
      do_export = TRUE
    )
  } else {
    message("Final export disabled. Set run_final_export = TRUE to write to OCDE server.")
  }
  
  # Return all outputs
  list(
    start_year = start_year,
    data_year = data_year,
    level = level,
    clean = df,
    fact = df_fact,
    dims = dims,
    suppression_cols = suppression_cols,
    flat_csv_path = flat_csv_path,
    fact_local_path = fact_local_path,
    dim_local_paths = dim_local_paths
  )
}