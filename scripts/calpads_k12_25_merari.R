#META-INFO
#R SCRIPT: CALPADS UPC GRADES K-12, 2024-25 PREP
#WEBPAGE WITH DATA FILE: https://www.cde.ca.gov/ds/ad/filescupc.asp
#LAST EDITED ON: 2.2.2026
#LAST EDITED BY: JA

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
## 1819 is the year, 912 is the grade span (9-12)
#~ setwd("T:/CDE data releases/Enrollment Data/CALPADS UPC Grades K-12/2024-25")
setwd("/Users/merarisantana/Desktop/OCDE/CALPADS_K-12/data/raw") #@
cupc_2425_k12 <- readxl::read_excel("cupc2425-k12.xlsx", sheet = "LEA-Level CALPADS UPC Data", skip = 1)

#checking data types for all variables and looking through data
str(cupc_2425_k12)
view(cupc_2425_k12)

# Step 3: Cleaning and renaming columns ####
##clean_names function just makes all names lowercase and removes spaces and replaces with underscore
cupc_2425_k12 <- cupc_2425_k12 %>% 
  clean_names()
names(cupc_2425_k12)

## Step 3.1: Simplifying column names that are too long #### 
cupc_2425_k12 <- cupc_2425_k12 %>% #d
  rename(year = academic_year, ed_option_type = educational_option_type, frpm_status = free_reduced_meal_program,
         charter = charter_school_y_n, charter_num = charter_number, charter_funding = charter_funding_type,
         calpads_upc_count = calpads_unduplicated_pupil_count_upc, calpads_fall1_cert = calpads_fall_1_certification_status_y_n,
         frpm = free_reduced_meal_program, english_learner = english_learner_el)
names(cupc_2425_k12)

# Step 4: Make sure all number columns are numeric or integer ####
### we need numeric when it has a decimal point, integer when it's a whole number

## Step 4.1: Suppression ####
### some columns have suppressed data with an * which we want to remove, but we want to know if it was suppressed
### to differentiate between NA and suppressed, we create a dummy variable

### star_scan scans all columns for "*" and then with $columns on the end, creates a list of all columns in 
### our data with suppression. We then assign the list an object name suppression_cols to use later

#### this data has no suppression
suppression_cols <- star_scan(cupc_2425_k12)$columns

### changing columns to numeric or integer dependent on if it has a decimal point or not
### we change to numeric when possible because it takes up less storage

### IMPORTANT: Although school_code, district_code and county_code could be converted to numeric, 
### we don't want to do that because we are going to use a function later that adds leading or 
### trailing zeros
str(cupc_2425_k12)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(total_enrollment = as.integer(total_enrollment),
         frpm = as.integer(frpm),
         foster = as.integer(foster),
         homeless = as.integer(homeless),
         migrant_program = as.integer(migrant_program),
         direct_certification = as.integer(direct_certification),
         unduplicated_frpm_eligible_count = as.integer(unduplicated_frpm_eligible_count),
         english_learner = as.integer(english_learner),
         calpads_upc_count = as.integer(calpads_upc_count),
         tribal_foster_youth = as.integer(tribal_foster_youth)) #d
str(cupc_2425_k12)

## Step 4.2: Creating dummy variables ####
### any variable that is has categorical options, we want to convert to a numeric dummy
### starting with an indicator for whether a school is a charter school or not
table(cupc_2425_k12$charter)
cupc_2425_k12 <- cupc_2425_k12 %>%
  mutate(
    charter = na_if(as.character(charter), "N/A"),
    charter_dummy = case_when(
      as.character(charter) == "Yes" ~ 1,
      as.character(charter) == "No" ~ 0,
      TRUE ~ NA_integer_
    )) %>% 
  mutate(charter_dummy = as.integer(charter_dummy))
table(cupc_2425_k12$charter, cupc_2425_k12$charter_dummy)
str(cupc_2425_k12$charter_dummy)

### now onto the school type variable
### the numbering label scheme is just alphabetical order
table(cupc_2425_k12$school_type)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(
    school_type = na_if(as.character(school_type), "N/A"),
    school_type_num = case_when(
      as.character(school_type) == "Alternative Schools of Choice" ~ 1,
      as.character(school_type) == "Continuation High Schools" ~ 2,
      as.character(school_type) == "County Community" ~ 3,
      as.character(school_type) == "District Community Day Schools" ~ 4,
      as.character(school_type) == "Elemen Schools In 1 School Dist. (Public)" ~ 5,
      as.character(school_type) == "Elementary Schools (Public)" ~ 6,
      as.character(school_type) == "High Schools (Public)" ~ 7,
      as.character(school_type) == "High Schools In 1 School Dist. (Public)" ~ 8,
      as.character(school_type) == "Intermediate/Middle Schools (Public)" ~ 9,
      as.character(school_type) == "Junior High Schools (Public)" ~ 10,
      as.character(school_type) == "Juvenile Court Schools" ~ 11,
      as.character(school_type) == "K-12 Schools (Public)" ~ 12,
      as.character(school_type) == "Opportunity Schools" ~ 13,
      as.character(school_type) == "Special Education Schools (Public)" ~ 14
    )
  ) %>% 
  mutate(school_type_num = as.integer(school_type_num))
table(cupc_2425_k12$school_type, cupc_2425_k12$school_type_num)
str(cupc_2425_k12$school_type_num)

## same process for ed_option_type variable 
table(cupc_2425_k12$ed_option_type)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(
    ed_option_type = na_if(as.character(ed_option_type), "N/A"),
    ed_option_type_num = case_when(
      as.character(ed_option_type) == "Alternative School of Choice" ~ 1,
      as.character(ed_option_type) == "Community Day School" ~ 2,
      as.character(ed_option_type) == "Continuation School" ~ 3,
      as.character(ed_option_type) == "County Community School" ~ 4,
      as.character(ed_option_type) == "District Special Education Consortia School" ~ 5,
      as.character(ed_option_type) == "Home and Hospital" ~ 6,
      as.character(ed_option_type) == "Juvenile Court School" ~ 7,
      as.character(ed_option_type) == "Opportunity School" ~ 8,
      as.character(ed_option_type) == "Special Education School" ~ 9,
      as.character(ed_option_type) == "Traditional" ~ 10
    )
  ) %>% 
  mutate(ed_option_type_num = as.integer(ed_option_type_num))
table(cupc_2425_k12$ed_option_type, cupc_2425_k12$ed_option_type_num)
str(cupc_2425_k12$ed_option_type_num)

## same process for charter_funding variable 
table(cupc_2425_k12$charter_funding)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(
    charter_funding = na_if(as.character(charter_funding), "N/A"),
    charter_funding_num = case_when(
      as.character(charter_funding) == "Directly funded" ~ 1,
      as.character(charter_funding) == "Locally funded" ~ 2
    )
  ) %>% 
  mutate(charter_funding_num = as.integer(charter_funding_num))
table(cupc_2425_k12$charter_funding, cupc_2425_k12$charter_funding_num)
str(cupc_2425_k12$charter_funding_num)

## same process for irc variable 
table(cupc_2425_k12$irc)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(
    irc = na_if(as.character(irc), "N/A"),
    irc_num = case_when(
      as.character(irc) == "N" ~ 0, 
      as.character(irc) == "Y" ~ 1
    )
  ) %>% 
  mutate(irc_num = as.integer(irc_num))
table(cupc_2425_k12$irc, cupc_2425_k12$irc_num)
str(cupc_2425_k12$irc_num)

## same process with low_grade variable 
table(cupc_2425_k12$low_grade)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(
    low_grade = na_if(as.character(low_grade), "N/A"),
    low_grade_num = case_when(
      as.character(low_grade) == "1" ~ 1,
      as.character(low_grade) == "2" ~ 2,
      as.character(low_grade) == "3" ~ 3,
      as.character(low_grade) == "4" ~ 4,
      as.character(low_grade) == "5" ~ 5,
      as.character(low_grade) == "6" ~ 6,
      as.character(low_grade) == "7" ~ 7,
      as.character(low_grade) == "8" ~ 8,
      as.character(low_grade) == "9" ~ 9,
      as.character(low_grade) == "10" ~ 10,
      as.character(low_grade) == "11" ~ 11,
      as.character(low_grade) == "12" ~ 12,
      as.character(low_grade) == "Adult" ~ 13,
      as.character(low_grade) == "K" ~ 14,
      as.character(low_grade) == "P" ~ 15,
      TRUE ~ NA_integer_
    )) %>% 
  mutate(low_grade_num = as.integer(low_grade_num))
table(cupc_2425_k12$low_grade, cupc_2425_k12$low_grade_num)
str(cupc_2425_k12$low_grade_num)

## same process with high_grade variable
table(cupc_2425_k12$high_grade)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(
    high_grade = na_if(as.character(high_grade), "N/A"),
    high_grade_num = case_when(
      as.character(high_grade) == "1" ~ 1,
      as.character(high_grade) == "2" ~ 2,
      as.character(high_grade) == "3" ~ 3,
      as.character(high_grade) == "4" ~ 4,
      as.character(high_grade) == "5" ~ 5,
      as.character(high_grade) == "6" ~ 6,
      as.character(high_grade) == "7" ~ 7,
      as.character(high_grade) == "8" ~ 8,
      as.character(high_grade) == "9" ~ 9,
      as.character(high_grade) == "10" ~ 10,
      as.character(high_grade) == "11" ~ 11,
      as.character(high_grade) == "12" ~ 12,
      as.character(high_grade) == "13" ~ 13,
      as.character(high_grade) == "Adult" ~ 14,
      as.character(high_grade) == "Post-Secondary" ~ 15,
      as.character(high_grade) == "K" ~ 16,
      as.character(high_grade) == "P" ~ 17,
      TRUE ~ NA_integer_
    )) %>% 
  mutate(high_grade_num = as.integer(high_grade_num))
table(cupc_2425_k12$high_grade, cupc_2425_k12$high_grade_num)
str(cupc_2425_k12$high_grade_num)

## last but not least, calpads_fall1_cert
table(cupc_2425_k12$calpads_fall1_cert)
cupc_2425_k12 <- cupc_2425_k12 %>% 
  mutate(
    calpads_fall1_cert_num = case_when(
      as.character(calpads_fall1_cert) == "In Expected List But We Do Not Have Data For This School/LEA" ~ 0, 
      as.character(calpads_fall1_cert) == "Y" ~ 1
    )
  ) %>% 
  mutate(calpads_fall1_cert_num = as.integer(calpads_fall1_cert_num))
table(cupc_2425_k12$calpads_fall1_cert, cupc_2425_k12$calpads_fall1_cert_num)
str(cupc_2425_k12$calpads_fall1_cert_num)

#changing year from character year to two digit numeric
cupc_2425_k12 <- cupc_2425_k12 %>%
  mutate(year = case_when(
    year == "2024-2025" ~ 25
  )) %>% 
  mutate(year = as.integer(year))

# Step 5: Pad CDS codes and create cds variable ####
### this custom function creates a CDS code variable and ensures that all CDS codes are the correct length
### CDS code is the unique identifier the state gives to each school. We have the three pieces of CDS 
### code in this file (county, district and school code) so we concatenate those to make cds
cupc_2425_k12 <- pad_cds_codes(cupc_2425_k12)
view(cupc_2425_k12)

# Step 6: Export Data ####
## Step 6.1: Flat csv file ####
## we want this version that is less messy for REDI staff and other folks who want to work with data 
## but are not going to be doing visualizations
#~ fwrite(cupc_2425_k12, "T:/CDE data releases/Enrollment Data/CALPADS UPC Grades K-12/2024-25/cupc_k12_LEA_25_clean.csv")
fwrite(cupc_2425_k12, "/Users/merarisantana/Desktop/OCDE/CALPADS_K-12/data/processed/cupc_k12_LEA_25_clean.csv") #@

## Step 6.2: Fact file and entities dim ####
### a fact file is a version of our original dataset that we will send to the OCDE server, so we need this to 
### only have numeric information - no text 
cupc_2425_k12_fact <- cupc_2425_k12 %>%
  select(-c(county_name, district_name, school_name, charter, district_type, school_type, ed_option_type,
            charter, charter_funding, irc, low_grade, high_grade, calpads_fall1_cert))

# exporting 2018-19 fact
## we now check the primary key which uniquely identifies each row
## more info on primary keys: https://www.geeksforgeeks.org/dbms/primary-key-in-dbms/
validate_primary_key(cupc_2425_k12_fact, c("cds"), 
                     full_run = T)

## we now export the fact table using our custom function "safe_fwrite"
## merari, just update this code for the current file, but do not run it as it exports to our server
#! safe_fwrite(cupc_2425_k12_fact, table_name = "cupc_k12_25",
#!            data_year = 2025, data_source = "cde",
#!            data_description = "2024-25 calpads upc k-12 file",
#!            data_type = "enrollment",
#!            user_note = "fact file.")

## Step 6.3: Creating other dim tables ####
### we now make dimension tables to link together key numerical and text info for data viz
### explainer here: https://www.dremio.com/wiki/dimension-table/
### more info on snowflake schema which is our data schema: https://www.geeksforgeeks.org/dbms/snowflake-schema-in-data-warehouse-model/
cupck12_25_entities_dim <- cupc_2425_k12 %>% 
  select(year, cds, county_code, district_code, school_code) %>% 
  distinct()

dim25_cupck12_districts <- cupc_2425_k12 %>%
  select(year, district_code, district_name) %>% 
  distinct() %>% 
  arrange(district_name) %>% 
  mutate(district_code = as.integer(district_code))
print(dim25_cupck12_districts)

dim25_cupck12_schools <- cupc_2425_k12 %>%
  select(year, school_code, school_name) %>%
  distinct() %>% 
  arrange(school_code) %>% 
  mutate(school_code = as.integer(school_code)) %>% 
  filter(school_code != 0)
print(dim25_cupck12_schools)

### we are going to make a dimension table for most categorical dummies we made 
dim25_cupck12_school_type <- cupc_2425_k12 %>% 
  select(year, school_type, school_type_num) %>% 
  distinct() %>% 
  arrange(school_type_num) %>% 
  filter(!is.na(school_type_num))
print(dim25_cupck12_school_type)

dim25_cupck12_ed_option_type <- cupc_2425_k12 %>% 
  select(year, ed_option_type, ed_option_type_num) %>% 
  distinct() %>% 
  arrange(ed_option_type_num) %>% 
  filter(!is.na(ed_option_type_num))
print(dim25_cupck12_ed_option_type)

dim25_cupck12_charter_funding <- cupc_2425_k12 %>% 
  select(year, charter_funding, charter_funding_num) %>% 
  distinct() %>% 
  arrange(charter_funding_num) %>% 
  filter(!is.na(charter_funding_num))
print(dim25_cupck12_charter_funding)

dim25_cupck12_charter <- cupc_2425_k12 %>% 
  select(year, charter, charter_dummy) %>% 
  distinct() %>% 
  arrange(charter_dummy) %>% 
  filter(!is.na(charter_dummy))
print(dim25_cupck12_charter)

dim25_cupck12_irc <- cupc_2425_k12 %>% 
  select(year, irc, irc_num) %>% 
  distinct() %>% 
  arrange(irc_num) %>% 
  filter(!is.na(irc_num))
print(dim25_cupck12_irc)

dim25_cupck12_low_grade <- cupc_2425_k12 %>% 
  select(year, low_grade, low_grade_num) %>% 
  distinct() %>% 
  arrange(low_grade_num) %>% 
  filter(!is.na(low_grade_num))
print(dim25_cupck12_low_grade)

dim25_cupck12_high_grade <- cupc_2425_k12 %>% 
  select(year, high_grade, high_grade_num) %>% 
  distinct() %>% 
  arrange(high_grade_num) %>% 
  filter(!is.na(high_grade_num))
print(dim25_cupck12_high_grade)

dim25_cupck12_calpads_fall1_cert <- cupc_2425_k12 %>% 
  select(year, calpads_fall1_cert, calpads_fall1_cert_num) %>% 
  distinct() %>% 
  arrange(calpads_fall1_cert_num) %>% 
  filter(!is.na(calpads_fall1_cert_num))
print(dim25_cupck12_calpads_fall1_cert)

## Step 6.4: Exporting dimension tables ####
### merari, also just update this code but do not run other than "validate_primary_key lines" to ensure those are fine
### green checkmark and text is good, red x and text is bad 
cupck12_25_dims <- get_dim_objects()

validate_primary_key(cupck12_25_entities_dim, "cds", full_run = T)
#! safe_fwrite(cupck12_25_entities_dim, table_name = "cupck12_25_entities",
#!             dimension_type = "annualized", data_source = "cde",
#!             data_year = 2025, data_type = "dim",
#!             data_description = "2024-25 calpads k-12 upc entities dim table",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_districts, "district_code", full_run = T)
#! safe_fwrite(dim25_cupck12_districts, table_name = "cupck12_25_districts",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "unique list of school districts calpads k-12 upc 2025.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_schools, "school_code", full_run = T)
#! safe_fwrite(dim25_cupck12_schools, table_name = "cupck12_25_schools",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "unique list of school schools calpads k-12 upc 2025.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_ed_option_type, "ed_option_type_num", full_run = T)
#! safe_fwrite(dim25_cupck12_ed_option_type, table_name = "dim25_cupck12_ed_option_type",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for education option type.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_charter_funding, "charter_funding_num", full_run = T)
#! safe_fwrite(dim25_cupck12_charter_funding, table_name = "dim25_cupck12_charter_funding",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for charter funding in calpads upc k-12.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_irc, "irc_num", full_run = T)
#! safe_fwrite(dim25_cupck12_irc, table_name = "dim25_cupck12_irc",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for whether an LEA is an independently reporting charter in calpads upc k-12.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_low_grade, "low_grade_num", full_run = T)
#! safe_fwrite(dim25_cupck12_low_grade, table_name = "dim25_cupck12_low_grade",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for an LEAs lowest grade in calpads upc k-12.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_high_grade, "high_grade_num", full_run = T)
#! safe_fwrite(dim25_cupck12_high_grade, table_name = "dim25_cupck12_high_grade",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for an LEAs highest grade in calpads upc k-12.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_calpads_fall1_cert, "calpads_fall1_cert_num", full_run = T)
#! safe_fwrite(dim25_cupck12_calpads_fall1_cert, table_name = "dim25_cupck12_calpads_fall1_cert",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for whether an LEA fall 1 is certified in calpads upc k-12.",
#!             user_note = "dim table.")

# NOW SCHOOL LEVEL ####
# Step 1. Save new CDE data to folder on T drive ####
## THIS IS AN EXCEL FILE WITH MULTIPLE TABS, SO DATA FILE IS SAVED MANUALLY

# Step 2. Read in latest CDS data #### 
## 2425 is the year, 912 is the grade span (9-12)
#~ setwd("T:/CDE data releases/Enrollment Data/CALPADS UPC Grades K-12/2024-25")
setwd("/Users/merarisantana/Desktop/OCDE/CALPADS_K-12/data/raw") 
cupc_2425_k12_school <- readxl::read_excel("cupc2425-k12.xlsx", sheet = "School-Level CALPADS UPC Data", skip = 1) #$

# this is a custom function we created to compare variable names across datasets
# variables between the two are the same 
compare_variable_names(cupc_2425_k12, cupc_2425_k12_school)

#checking data types for all variables and looking through data
str(cupc_2425_k12_school)
view(cupc_2425_k12_school)

# Step 3: Cleaning and renaming columns ####
##clean_names function just makes all names lowercase and removes spaces and replaces with underscore
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  clean_names()
names(cupc_2425_k12_school)

## Step 3.1: Simplifying column names that are too long #### 
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  rename(year = academic_year, ed_option_type = educational_option_type, frpm = free_reduced_meal_program,
         charter = charter_school_y_n, charter_num = charter_number, charter_funding = charter_funding_type,
         calpads_upc_count = calpads_unduplicated_pupil_count_upc, calpads_fall1_cert = calpads_fall_1_certification_status_y_n,
         frpm = free_reduced_meal_program, english_learner = english_learner_el)
names(cupc_2425_k12_school)

# Step 4: Make sure all number columns are numeric or integer ####
### we need numeric when it has a decimal point, integer when it's a whole number

## Step 4.1: Suppression ####
### some columns have suppressed data with an * which we want to remove, but we want to know if it was suppressed
### to differentiate between NA and suppressed, we create a dummy variable

### star_scan scans all columns for "*" and then with $columns on the end, creates a list of all columns in 
### our data with suppression. We then assign the list an object name suppression_cols to use later

#### this data has no suppression
suppression_cols <- star_scan(cupc_2425_k12_school)$columns

### changing columns to numeric or integer dependent on if it has a decimal point or not
### we change to numeric when possible because it takes up less storage

### IMPORTANT: Although school_code, district_code and county_code could be converted to numeric, 
### we don't want to do that because we are going to use a function later that adds leading or 
### trailing zeros
str(cupc_2425_k12_school)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(total_enrollment = as.integer(total_enrollment),
         frpm = as.integer(frpm),
         foster = as.integer(foster),
         homeless = as.integer(homeless),
         migrant_program = as.integer(migrant_program),
         direct_certification = as.integer(direct_certification),
         unduplicated_frpm_eligible_count = as.integer(unduplicated_frpm_eligible_count),
         english_learner = as.integer(english_learner),
         calpads_upc_count = as.integer(calpads_upc_count),
         tribal_foster_youth = as.integer(tribal_foster_youth)) #d
str(cupc_2425_k12_school)

## Step 4.2: Creating dummy variables ####
### any variable that is has categorical options, we want to convert to a numeric dummy
### starting with an indicator for whether a school is a charter school or not
table(cupc_2425_k12_school$charter)
cupc_2425_k12_school <- cupc_2425_k12_school %>%
  mutate(
    charter = na_if(as.character(charter), "N/A"),
    charter_dummy = case_when(
      as.character(charter) == "Yes" ~ 1,
      as.character(charter) == "No" ~ 0,
      TRUE ~ NA_integer_
    )) %>% 
  mutate(charter_dummy = as.integer(charter_dummy))
table(cupc_2425_k12_school$charter, cupc_2425_k12_school$charter_dummy)
str(cupc_2425_k12_school$charter_dummy)

### now onto the school type variable
### the numbering label scheme is just alphabetical order
table(cupc_2425_k12_school$school_type)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(
    school_type = na_if(as.character(school_type), "N/A"),
    school_type_num = case_when(
      as.character(school_type) == "Alternative Schools of Choice" ~ 1,
      as.character(school_type) == "Continuation High Schools" ~ 2,
      as.character(school_type) == "County Community" ~ 3,
      as.character(school_type) == "District Community Day Schools" ~ 4,
      as.character(school_type) == "Elemen Schools In 1 School Dist. (Public)" ~ 5,
      as.character(school_type) == "Elementary Schools (Public)" ~ 6,
      as.character(school_type) == "High Schools (Public)" ~ 7,
      as.character(school_type) == "High Schools In 1 School Dist. (Public)" ~ 8,
      as.character(school_type) == "Intermediate/Middle Schools (Public)" ~ 9,
      as.character(school_type) == "Junior High Schools (Public)" ~ 10,
      as.character(school_type) == "Juvenile Court Schools" ~ 11,
      as.character(school_type) == "K-12 Schools (Public)" ~ 12,
      as.character(school_type) == "Opportunity Schools" ~ 13,
      as.character(school_type) == "Special Education Schools (Public)" ~ 14,
      as.character(school_type) == "Preschool" ~ 15
    )
  ) %>% 
  mutate(school_type_num = as.integer(school_type_num))
table(cupc_2425_k12_school$school_type, cupc_2425_k12_school$school_type_num)
str(cupc_2425_k12_school$school_type_num)

## same process for ed_option_type variable 
table(cupc_2425_k12_school$ed_option_type)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(
    ed_option_type = na_if(as.character(ed_option_type), "N/A"),
    ed_option_type_num = case_when(
      as.character(ed_option_type) == "Alternative School of Choice" ~ 1,
      as.character(ed_option_type) == "Community Day School" ~ 2,
      as.character(ed_option_type) == "Continuation School" ~ 3,
      as.character(ed_option_type) == "County Community School" ~ 4,
      as.character(ed_option_type) == "District Special Education Consortia School" ~ 5,
      as.character(ed_option_type) == "Home and Hospital" ~ 6,
      as.character(ed_option_type) == "Juvenile Court School" ~ 7,
      as.character(ed_option_type) == "Opportunity School" ~ 8,
      as.character(ed_option_type) == "Special Education School" ~ 9,
      as.character(ed_option_type) == "Traditional" ~ 10
    )
  ) %>% 
  mutate(ed_option_type_num = as.integer(ed_option_type_num))
table(cupc_2425_k12_school$ed_option_type, cupc_2425_k12_school$ed_option_type_num)
str(cupc_2425_k12_school$ed_option_type_num)

## same process for charter_funding variable 
table(cupc_2425_k12_school$charter_funding)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(
    charter_funding = na_if(as.character(charter_funding), "N/A"),
    charter_funding_num = case_when(
      as.character(charter_funding) == "Directly funded" ~ 1,
      as.character(charter_funding) == "Locally funded" ~ 2
    )
  ) %>% 
  mutate(charter_funding_num = as.integer(charter_funding_num))
table(cupc_2425_k12_school$charter_funding, cupc_2425_k12_school$charter_funding_num)
str(cupc_2425_k12_school$charter_funding_num)

## same process for irc variable 
table(cupc_2425_k12_school$irc)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(
    irc = na_if(as.character(irc), "N/A"),
    irc_num = case_when(
      as.character(irc) == "N" ~ 0, 
      as.character(irc) == "Y" ~ 1
    )
  ) %>% 
  mutate(irc_num = as.integer(irc_num))
table(cupc_2425_k12_school$irc, cupc_2425_k12_school$irc_num)
str(cupc_2425_k12_school$irc_num)

## same process with low_grade variable 
table(cupc_2425_k12_school$low_grade)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(
    low_grade = na_if(as.character(low_grade), "N/A"),
    low_grade_num = case_when(
      as.character(low_grade) == "1" ~ 1,
      as.character(low_grade) == "2" ~ 2,
      as.character(low_grade) == "3" ~ 3,
      as.character(low_grade) == "4" ~ 4,
      as.character(low_grade) == "5" ~ 5,
      as.character(low_grade) == "6" ~ 6,
      as.character(low_grade) == "7" ~ 7,
      as.character(low_grade) == "8" ~ 8,
      as.character(low_grade) == "9" ~ 9,
      as.character(low_grade) == "10" ~ 10,
      as.character(low_grade) == "11" ~ 11,
      as.character(low_grade) == "12" ~ 12,
      as.character(low_grade) == "Adult" ~ 13,
      as.character(low_grade) == "K" ~ 14,
      as.character(low_grade) == "P" ~ 15,
      TRUE ~ NA_integer_
    )) %>% 
  mutate(low_grade_num = as.integer(low_grade_num))
table(cupc_2425_k12_school$low_grade, cupc_2425_k12_school$low_grade_num)
str(cupc_2425_k12_school$low_grade_num)

## same process with high_grade variable
table(cupc_2425_k12_school$high_grade)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(
    high_grade = na_if(as.character(high_grade), "N/A"),
    high_grade_num = case_when(
      as.character(high_grade) == "1" ~ 1,
      as.character(high_grade) == "2" ~ 2,
      as.character(high_grade) == "3" ~ 3,
      as.character(high_grade) == "4" ~ 4,
      as.character(high_grade) == "5" ~ 5,
      as.character(high_grade) == "6" ~ 6,
      as.character(high_grade) == "7" ~ 7,
      as.character(high_grade) == "8" ~ 8,
      as.character(high_grade) == "9" ~ 9,
      as.character(high_grade) == "10" ~ 10,
      as.character(high_grade) == "11" ~ 11,
      as.character(high_grade) == "12" ~ 12,
      as.character(high_grade) == "13" ~ 13,
      as.character(high_grade) == "Adult" ~ 14,
      as.character(high_grade) == "Post-Secondary" ~ 15,
      as.character(high_grade) == "K" ~ 16,
      as.character(high_grade) == "P" ~ 17,
      TRUE ~ NA_integer_
    )) %>% 
  mutate(high_grade_num = as.integer(high_grade_num))
table(cupc_2425_k12_school$high_grade, cupc_2425_k12_school$high_grade_num)
str(cupc_2425_k12_school$high_grade_num)

## last but not least, calpads_fall1_cert
table(cupc_2425_k12_school$calpads_fall1_cert)
cupc_2425_k12_school <- cupc_2425_k12_school %>% 
  mutate(
    calpads_fall1_cert_num = case_when(
      as.character(calpads_fall1_cert) == "In Expected List But We Do Not Have Data For This School/LEA" ~ 0, 
      as.character(calpads_fall1_cert) == "Y" ~ 1
    )
  ) %>% 
  mutate(calpads_fall1_cert_num = as.integer(calpads_fall1_cert_num))
table(cupc_2425_k12_school$calpads_fall1_cert, cupc_2425_k12_school$calpads_fall1_cert_num)
str(cupc_2425_k12_school$calpads_fall1_cert_num)

#changing year from character year to two digit numeric
cupc_2425_k12_school <- cupc_2425_k12_school %>%
  mutate(year = case_when(
    year == "2024-2025" ~ 25
  )) %>% 
  mutate(year = as.integer(year))

# Step 5: Pad CDS codes and create cds variable ####
### this custom function creates a CDS code variable and ensures that all CDS codes are the correct length
### CDS code is the unique identifier the state gives to each school. We have the three pieces of CDS 
### code in this file (county, district and school code) so we concatenate those to make cds
cupc_2425_k12_school <- pad_cds_codes(cupc_2425_k12_school)
view(cupc_2425_k12_school)

# Step 6: Export Data ####
## Step 6.1: Flat csv file ####
## we want this version that is less messy for REDI staff and other folks who want to work with data 
## but are not going to be doing visualizations
#~ fwrite(cupc_2425_k12_school, "T:/CDE data releases/Enrollment Data/CALPADS UPC Grades K-12/2024-25/cupc_k12_school_25_clean.csv")
fwrite(cupc_2425_k12_school, "/Users/merarisantana/Desktop/OCDE/CALPADS_K-12/data/processed/cupc_k12_school_25_clean.csv")

## Step 6.2: Fact file and entities dim ####
### a fact file is a version of our original dataset that we will send to the OCDE server, so we need this to 
### only have numeric information - no text 
cupc_2425_k12_school_fact <- cupc_2425_k12_school %>% #d
  select(-c(county_name, district_name, school_name, charter, district_type, school_type, ed_option_type,
            tribal_foster_youth, charter, charter_funding, irc, low_grade, high_grade, calpads_fall1_cert)) #d

# exporting 2024-25 fact
## we now check the primary key which uniquely identifies each row
## more info on primary keys: https://www.geeksforgeeks.org/dbms/primary-key-in-dbms/
validate_primary_key(cupc_2425_k12_school_fact, c("cds"), 
                     full_run = T)

## we now export the fact table using our custom function "safe_fwrite"
## merari, just update this code for the current file, but do not run it as it exports to our server
#! safe_fwrite(cupc_2425_k12_school_fact, table_name = "cupc_k12_25_school_fact",
#!             data_year = 2025, data_source = "cde",
#!             data_description = "2024-25 calpads school upc k-12 file",
#!             data_type = "enrollment",
#!             user_note = "fact file.")

## Step 6.3: Creating other dim tables ####
### we now make dimension tables to link together key numerical and text info for data viz
### explainer here: https://www.dremio.com/wiki/dimension-table/
### more info on snowflake schema which is our data schema: https://www.geeksforgeeks.org/dbms/snowflake-schema-in-data-warehouse-model/
cupck12_25_entities_dim <- cupc_2425_k12_school %>% 
  select(year, cds, county_code, district_code, school_code) %>% 
  distinct()

dim25_cupck12_districts <- cupc_2425_k12_school %>%
  select(year, district_code, district_name) %>% 
  distinct() %>% 
  arrange(district_name) %>% 
  mutate(district_code = as.integer(district_code))
print(dim25_cupck12_districts)

dim25_cupck12_schools <- cupc_2425_k12_school %>%
  select(year, school_code, school_name) %>%
  distinct() %>% 
  arrange(school_code) %>% 
  mutate(school_code = as.integer(school_code)) %>% 
  filter(school_code != 0)
print(dim25_cupck12_schools)

### we are going to make a dimension table for most categorical dummies we made 
dim25_cupck12_school_type <- cupc_2425_k12_school %>% 
  select(year, school_type, school_type_num) %>% 
  distinct() %>% 
  arrange(school_type_num) %>% 
  filter(!is.na(school_type_num))
print(dim25_cupck12_school_type)

dim25_cupck12_ed_option_type <- cupc_2425_k12_school %>% 
  select(year, ed_option_type, ed_option_type_num) %>% 
  distinct() %>% 
  arrange(ed_option_type_num) %>% 
  filter(!is.na(ed_option_type_num))
print(dim25_cupck12_ed_option_type)

dim25_cupck12_charter_funding <- cupc_2425_k12_school %>% 
  select(year, charter_funding, charter_funding_num) %>% 
  distinct() %>% 
  arrange(charter_funding_num) %>% 
  filter(!is.na(charter_funding_num))
print(dim25_cupck12_charter_funding)

dim25_cupck12_charter <- cupc_2425_k12_school %>% 
  select(year, charter, charter_dummy) %>% 
  distinct() %>% 
  arrange(charter_dummy) %>% 
  filter(!is.na(charter_dummy))
print(dim25_cupck12_charter)

dim25_cupck12_irc <- cupc_2425_k12_school %>% 
  select(year, irc, irc_num) %>% 
  distinct() %>% 
  arrange(irc_num) %>% 
  filter(!is.na(irc_num))
print(dim25_cupck12_irc)

dim25_cupck12_low_grade <- cupc_2425_k12_school %>% 
  select(year, low_grade, low_grade_num) %>% 
  distinct() %>% 
  arrange(low_grade_num) %>% 
  filter(!is.na(low_grade_num))
print(dim25_cupck12_low_grade)

dim25_cupck12_high_grade <- cupc_2425_k12_school %>% 
  select(year, high_grade, high_grade_num) %>% 
  distinct() %>% 
  arrange(high_grade_num) %>% 
  filter(!is.na(high_grade_num))
print(dim25_cupck12_high_grade)

dim25_cupck12_calpads_fall1_cert <- cupc_2425_k12_school %>% 
  select(year, calpads_fall1_cert, calpads_fall1_cert_num) %>% 
  distinct() %>% 
  arrange(calpads_fall1_cert_num) %>% 
  filter(!is.na(calpads_fall1_cert_num))
print(dim25_cupck12_calpads_fall1_cert)

## Step 6.4: Exporting dimension tables ####
### merari, also just update this code but do not run other than "validate_primary_key lines" to ensure those are fine
### green checkmark and text is good, red x and text is bad 
cupck12_25_dims <- get_dim_objects()

validate_primary_key(cupck12_25_entities_dim, "cds", full_run = T)
#! safe_fwrite(cupck12_25_entities_dim, table_name = "cupck12_school_25_entities",
#!             dimension_type = "annualized", data_source = "cde",
#!             data_year = 2025, data_type = "dim",
#!             data_description = "2024-25 calpads k-12 school-level upc entities dim table",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_districts, "district_code", full_run = T)
#! safe_fwrite(dim25_cupck12_districts, table_name = "cupck12_school_25_districts",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "unique list of school districts calpads k-12 school-level upc 2025.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_schools, "school_code", full_run = T)
#! safe_fwrite(dim25_cupck12_schools, table_name = "cupck12_school_25_schools",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "unique list of school schools calpads k-12 school-level upc 2025.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_ed_option_type, "ed_option_type_num", full_run = T)
#! safe_fwrite(dim25_cupck12_ed_option_type, table_name = "dim25_cupck12_school_ed_option_type",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for education option typein calpads k12 school-level.",
#!           user_note = "dim table.")

validate_primary_key(dim25_cupck12_charter_funding, "charter_funding_num", full_run = T)
#! safe_fwrite(dim25_cupck12_charter_funding, table_name = "dim25_cupck12_school_charter_funding",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for charter funding in calpads upc k-12 school-level.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_irc, "irc_num", full_run = T)
#! safe_fwrite(dim25_cupck12_irc, table_name = "dim25_cupck12_school_irc",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for whether an LEA is an independently reporting charter in calpads upc k-12 school-level.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_low_grade, "low_grade_num", full_run = T)
#! safe_fwrite(dim25_cupck12_low_grade, table_name = "dim25_cupck12_school_low_grade",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for an LEAs lowest grade in calpads upc k-12 school-level.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_high_grade, "high_grade_num", full_run = T)
#! safe_fwrite(dim25_cupck12_high_grade, table_name = "dim25_cupck12_school_high_grade",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for an LEAs highest grade in calpads upc k-12 school-level.",
#!             user_note = "dim table.")

validate_primary_key(dim25_cupck12_calpads_fall1_cert, "calpads_fall1_cert_num", full_run = T)
#! safe_fwrite(dim25_cupck12_calpads_fall1_cert, table_name = "dim25_cupck12_school_calpads_fall1_cert",
#!             data_year = 2025, data_source = "cde",
#!             dimension_type = "annualized", data_type = "dim",
#!             data_description = "dimension table for whether an LEA fall 1 is certified in calpads upc k-12 school-level.",
#!             user_note = "dim table.")

