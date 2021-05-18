#### Packages

library(tidyverse)
library(lubridate)
library(data.table)
library(openair)


#### Helper function `coalesce_join`

# **Description**
#   
# `coalesce_join` performs a majority of the data aggregation work within the `compile_atm_data` and `compile_climate_data`. 
# The function combines two datasets containing identical non-key variables in varying states of completeness. 
# This allows the well data compilation to be updated/appended to without worry of overlap with the existing time series in the existing compilation.
# 
# **Arguments:**
#   
# * `x`, `y` tbls to join
# * `by` a character vector of variables to join by. If NULL, the default, *_join() defined by the `join` argument will do a natural join, using all variables with common names across the two tables. A message lists the variables so that you can check they're right (to suppress the message, simply explicitly list the variables that you want to join).
# * `suffix` If there are non-joined duplicate variables in x and y, these suffixes will be added to the output to disambiguate them. Should be a character vector of length 2
# * `join` type of mutating join supported by `dplyr`
# * ... other parameters passed onto methods, for instance, `na_matches` to control how NA values are matched. This is mostly included for robustness.
# 
# **Value**
# 
# A data frame `tbl_df`

coalesce_join <- function(x, y, 
                          by = NULL, suffix = c(".x", ".y"), 
                          join = dplyr::full_join, ...) {
  
  joined <- join(x, y, by = by, suffix = suffix, ...)
  
  # names of desired output
  cols <- dplyr::union(names(x), names(y))
  
  to_coalesce <- names(joined)[!names(joined) %in% cols]
  
  suffix_used <- suffix[ifelse(endsWith(to_coalesce, suffix[1]), 1, 2)]
  
  # remove suffixes and reduplicate
  to_coalesce <- unique(substr(
    to_coalesce, 
    1, 
    nchar(to_coalesce) - nchar(suffix_used)
  ))
  
  coalesced <- purrr::map_dfc(to_coalesce, ~dplyr::coalesce(
    joined[[paste0(.x, suffix[1])]], 
    joined[[paste0(.x, suffix[2])]]
  ))
  
  names(coalesced) <- to_coalesce
  
  dplyr::bind_cols(joined, coalesced)[cols]
}

#### Helper function `read_atm_hobo_u20`

# **Description**
#   
# `read_atm_hobo_u20` reads in the Hobo U20 atmospheric `.csv` 
# 
# **Arguments:**
#   
# * `path` path to the Hobo U20 atmospheric data file
# 
# **Value**
# 
# A data frame `tbl_df`

read_atm_hobo_u20 <- function(path){
  
  # Avoid problem of not knowing the number of cols coming from Hobo file (i.e. avoid warning)
  no_of_cols <- readr::read_csv(path, skip = 1, n_max = 1, col_types = cols(.default = col_guess())) %>% 
    ncol()
  
  no_of_cols_drop_at_end <- no_of_cols - 4
  
  data <- readr::read_csv(path, skip = 2, 
                          col_types = paste0("_cdd", stringi::stri_dup("_", no_of_cols_drop_at_end)), 
                          col_names = c("Date", "baro.pressure_psi", "baro.temp_F")) %>% 
    dplyr::mutate(Date = lubridate::mdy_hms(Date)) %>% 
    dplyr::filter(!is.na(baro.pressure_psi))
  
  return(data)
  
}

#### Helper function `read_climate`

# **Description**
#   
# `read_climate` reads in the Hobo climate station `.csv` 
# 
# **Arguments:**
#   
# * `path` path to the Hobo climate station data file
# 
# **Value**
# 
# A data frame `tbl_df`

read_climate <- function(path){
  
  col_names <- readr::read_lines(path, skip = 1, n_max = 1) %>% 
    stringr::str_remove_all(pattern = '\"') %>% 
    stringr::str_split(pattern = ",") %>% 
    purrr::pluck(1) %>% 
    str_to_lower() %>% 
    str_detect("snow")
  
  if(TRUE %in% col_names){
    
    data <- readr::read_csv(path, skip = 2, 
                            col_types = "_cddddddddd", col_names = c("Date", "solar.rad_1", "solar.rad_2", "wind_speed", "gust_speed",
                                                                     "temp_F", "RH", "rain_in", "snowdepth_in", "current")) %>% 
      dplyr::mutate(Date = lubridate::mdy_hms(Date))
    
    return(data)
    
  } else {
    
    data <- readr::read_csv(path, skip = 2, 
                            col_types = "_cddddd", col_names = c("Date", "solar.rad_1", "wind_speed", "gust_speed", 
                                                                 "temp_F", "RH")) %>% 
      dplyr::mutate(Date = lubridate::mdy_hms(Date))
    
    return(data)
    
  }
  
}

#### Main Function `compile_atm_data`

# **Description**
#   
# `compile_atm_data` takes a folder containing raw .csv files from one meadow site and performs atmospheric data compilation, appending to an 
# existing compilation for that meadow site if prompted. Outputs a written .csv file.
# 
# **Arguments**
#   
# * `path` path to folder containing raw .csv atmospheric data files. This provided path is where the compilation .csv will be written to.
# * `prev_compile` previous data compilation. By default this is set to `NULL` 
# * `written_file_name` name of produced compilation file, must be quoted and end with .csv
# 
# **Value**
#   
# A .csv file written to `path`
# 

compile_atm_data <- function(path, prev_compile = NULL, written_file_name){
  
  if(!stringr::str_detect(written_file_name, pattern = ".csv")){
    stop("provided written file name must end with .csv")
  }
  
  ## Create char. vector of file names w/ directory path prepended contained in provided path
  
  files <- list.files(path, full.names = TRUE)
  
  ## Creates list of clean/standardized data (data frames) for all those provided in the path using `read_atm_hobo_u20` function
  
  listed_files <- purrr::map(files, read_atm_hobo_u20)
  
  # Keeps files grouped by instrument and orders data chronologically. Duplicate observations are removed.
  compiled <- dplyr::bind_rows(listed_files) %>% 
    dplyr::distinct() %>%
    # `dplyr::arrange` would not function properly here
    do(data.frame(with(data = .,.[order(Date),])))
  
  ## Compiles well data using only those files provided in the path
  
  if(is.null(prev_compile)){
    
    compiled <- compiled %>% 
      dplyr::mutate(Date = as.character(Date))
    
    # Write out compiled file from provided files only 
    readr::write_csv(compiled, path = paste0(path, "/", written_file_name))
    
    ## When prev_compile file is provided, the files provided in the path are appended to the previously compiled file
    # Performs coalescing join- user does not have to worry about feeding in data that has been previously compiled
  } else {
    
    appended_compile <- prev_compile %>%
      coalesce_join(compiled, by = "Date") %>% 
      do(data.frame(with(data = .,.[order(Date),]))) %>%
      dplyr::distinct() %>% 
      purrr::discard(~all(is.na(.))) %>% 
      dplyr::mutate(Date = as.character(Date))
    
    # Write out appended compile file
    readr::write_csv(appended_compile, path = paste0(path, "/", written_file_name))
    
  }
  
}

#### Main Function `compile_climate_data`

# **Description**
#   
# `compile_climate_data` takes a folder containing raw climate .csv files from one meadow site and performs atmospheric data compilation, appending to an 
# existing compilation for that meadow site if prompted. Outputs a written .csv file.
# 
# **Arguments**
#   
# * `path` path to folder containing raw .csv climate data files. This provided path is where the compilation .csv will be written to.
# * `prev_compile` previous data compilation. By default this is set to `NULL` 
# * `written_file_name` name of produced compilation file, must be quoted and end with .csv
# 
# **Value**
#   
# A .csv file written to `path`
# 


compile_climate_data <- function(path, prev_compile = NULL, written_file_name){
  
  if(!stringr::str_detect(written_file_name, pattern = ".csv")){
    stop("provided written file name must end with .csv")
  }
  
  ## Create char. vector of file names w/ directory path prepended contained in provided path
  
  files <- list.files(path, full.names = TRUE)
  
  ## Creates list of clean/standardized data (data frames) for all those provided in the path using `read_climate` function
  
  listed_files <- purrr::map(files, read_climate)
  
  # Keeps files grouped by instrument and orders data chronologically. Duplicate observations are removed.
  compiled <- dplyr::bind_rows(listed_files) %>% 
    dplyr::distinct() %>%
    # `dplyr::arrange` would not function properly here
    do(data.frame(with(data = .,.[order(Date),])))
  
  ## Compiles well data using only those files provided in the path
  
  if(is.null(prev_compile)){
    
    compiled <- compiled %>% 
      dplyr::mutate(Date = as.character(Date))
    
    # Write out compiled file from provided files only 
    readr::write_csv(compiled, path = paste0(path, "/", written_file_name))
    
    ## When prev_compile file is provided, the files provided in the path are appended to the previously compiled file
    # Performs coalescing join- user does not have to worry about feeding in data that has been previously compiled
  } else {
    
    appended_compile <- prev_compile %>%
      coalesce_join(compiled, by = "Date") %>% 
      do(data.frame(with(data = .,.[order(Date),]))) %>%
      dplyr::distinct() %>% 
      purrr::discard(~all(is.na(.))) %>% 
      dplyr::mutate(Date = as.character(Date))
    
    # Write out appended compile file
    readr::write_csv(appended_compile, path = paste0(path, "/", written_file_name))
    
  }
  
}

## Temporal Aggregation Function

# **Description**
#   
# `temp_agg_meadow_dat` takes a data.frame of a meadow data compilation (e.g. soil moisture, well, climate, sap flow data) and 
# performs a temporal aggregation. 
# 
# **Arguments**
#   
# * `data` dataframe with data to aggregate, **must** contain a variable called "Date" of class `POSIXct` 
# * `start_day_of_week` an integer specifying the start day of the week for the temporal aggregation. 1 corresponds to Sunday and so on.
# * `interval` the function tries to determine the interval of the original time series (e.g. hourly) by calculating the most common interval between time steps. The interval is needed for calculations where the `data.thresh > 0` as is defaulted. For example, a time step of 30 minutes would be specified as `interval = "30 min"`
# * `avg.time` This defines the time period to average to. Can be “sec”, “min”, “hour”, “day”, “DSTday”, “week”, “month”, “quarter” or “year”. For much increased flexibility a number can precede these options followed by a space. For example, a 7 day aggregation would be `avg.time = "7 day"`. In addition, avg.time can equal “season”, in which case 3-month seasonal values are calculated with spring defined as March, April, May and so on.
# * `data.thresh` The data capture threshold to use (%). A value of zero means that all available data will be used in a particular period regardless if of the number of values available. Conversely, a value of 100 will mean that all data will need to be present for the average to be calculated, else it is recorded as NA. 
# * `statistic` The statistic to apply when aggregating the data; default is the mean. Can be one of “mean”, “max”, “min”, “median”, “sd”. "sd" is standard deviation.
# 
# **Value**
#   
# A data.frame with Date in class `POSIXct` and WY in class `numeric`. WY indicates the water year that the aggregation belongs to.
# 
# **Other notes**
#   
# Make sure there is a variable in the supplied data.frame called "Date"

temp_agg_meadow_dat <- function(data, start_day_of_week, interval = "30 min", avg.time = "7 day", data.thresh = 50, statistic = "mean"){
  
  # data <- readr::read_csv(path, col_types = cols(Date = col_character(), .default = col_double())) %>% 
  #   dplyr::mutate(Date = as.POSIXct(Date, tz = "UTC"))
  
  # path_write_out <- paste0(stringr::str_replace(path, "[^/]+$", replacement = ""), file_name_write)
  
  # Determine the dates corresponding to the first and last of chosen weekday (start date for averaging) present in data
  start_date <- data %>% 
    dplyr::mutate(is.chosen_wday = ifelse(lubridate::wday(Date) == start_day_of_week, T, F)) %>%
    dplyr::filter(is.chosen_wday == TRUE) %>% 
    dplyr::summarise(min(Date)) %>% 
    dplyr::pull()
  
  end_date <- data %>% 
    dplyr::mutate(is.chosen_wday = ifelse(lubridate::wday(Date) == start_day_of_week - 1, T, F)) %>%
    dplyr::filter(is.chosen_wday == TRUE) %>% 
    dplyr::summarise(max(Date)) %>% 
    dplyr::pull()
  
  data <- data %>% 
    # rename Date variable to "date" to play nicely w/ openair::timeAverage
    dplyr::rename(date = Date) %>% 
    dplyr::filter(date >= start_date & date <= end_date)
  
  aggregation <- openair::timeAverage(data, avg.time = avg.time, 
                                      data.thresh = data.thresh, statistic = statistic,
                                      start.date = start_date, end.date = end_date, interval = interval) %>% 
    # determine water year membership of the time avg- this might need to be tweaked
    dplyr::mutate(WY = dplyr::case_when(
      date %within% lubridate::interval(ymd("2017-10-01", tz = "UTC"), 
                                        ymd("2018-09-30", tz = "UTC")) ~ 2018,
      date %within% lubridate::interval(ymd("2018-10-01", tz = "UTC"), 
                                        ymd("2019-09-30", tz = "UTC")) ~ 2019,
      date %within% lubridate::interval(ymd("2019-10-01", tz = "UTC"), 
                                        ymd("2020-09-30", tz = "UTC")) ~ 2020,
      date %within% lubridate::interval(ymd("2020-10-01", tz = "UTC"), 
                                        ymd("2021-09-30", tz = "UTC")) ~ 2021,
      date %within% lubridate::interval(ymd("2021-10-01", tz = "UTC"), 
                                        ymd("2022-09-30", tz = "UTC")) ~ 2022
    )) 
  # %>% 
  #   mutate(date = as.character(date)) %>% 
  #   rename(Date = date) %>%  
  #   # Write to .csv
  #   readr::write_csv(path = path_write_out)
  
}