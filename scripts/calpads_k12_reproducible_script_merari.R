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

# Step 2. Read in latest CDS data #### 

#Function reads the dataset
read_cupc_k12_step2 <- function(start_year,
                                level = c("LEA", "School"), # select which tab 
                                raw_dir,
                                show_str = TRUE,
                                show_view = TRUE) {
  
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
  
  # Reads excel dataset
  df <- readxl::read_excel(file_path, sheet = sheet_name, skip = 1)
  
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