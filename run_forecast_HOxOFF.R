forecast_HOx_off  <- function(config_set_name = "glm_aed_flare_v3",
                              configure_run_file = "configure_run.yml",
                              inflow_s3_bucket = ,
                              inflow_s3_endpoint = "https://amnh1.osn.mghpcc.org") {

  library(tidyverse)
  library(lubridate)
  set.seed(100)

  Sys.setenv('GLM_PATH'='GLM3r')

  options(future.globals.maxSize= 891289600)
  lake_directory <- here::here()

  source(file.path(lake_directory, "R/convert_vera4cast_inflow.R"))

  config <- FLAREr::set_up_simulation(configure_run_file = configure_run_file,
                                      lake_directory = lake_directory,
                                      config_set_name = config_set_name)

  noaa_ready <- FLAREr::check_noaa_present(lake_directory,
                                           configure_run_file,
                                           config_set_name = config_set_name)

  reference_date <- lubridate::as_date(config$run_config$forecast_start_datetime)
  s3 <- arrow::s3_bucket(bucket = inflow_s3_bucket,
                         endpoint_override = inflow_s3_endpoint,
                         anonymous = TRUE)
  avail_dates <- gsub("reference_date=", "", s3$ls())

  if(reference_date %in% lubridate::as_date(avail_dates)) {
    inflow_ready <- TRUE
  }else{
    inflow_ready <- FALSE
  }


  while (noaa_ready & inflow_ready) {

# put flare code in here!

  }


}
