library(tidyverse)
library(neon4cast)
library(lubridate)
library(rMR)
library(arrow)


date <- Sys.Date() - 1
bucket <- "bio230014-bucket01"
path <- "neon4cast-drivers/noaa/gefs-v12/stage1"
endpoint <- "https://sdsc.osn.xsede.org"

stage1 <- 
  glue::glue("{bucket}/{path}/reference_datetime={date}") |>
  arrow::s3_bucket(endpoint_override = endpoint, anonymous = TRUE) |>
  arrow::open_dataset()

site_data <- readr::read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv") |> 
  dplyr::filter(aquatics == 1)

sites <- c(site_data$field_site_id)

for( i in 1:length(sites)){
  
  precip <- stage1 |> 
    filter(variable == "PWAT",
           site_id == sites[i]) |>
    #group_by(datetime) |>
    #summarise(temp = mean(prediction)) |>
    arrange(datetime)
  
  precip_df <- precip |> collect()
  
  ggplot(precip_df, aes(x = datetime, y = prediction, group = ensemble))+
    geom_line()+
    theme_classic()
  
  ggsave(paste0("./figs/daily_precip/precip_forecast_",sites[i],"_",Sys.Date(),".jpg"), width=4, height=3, units='in')
  
  temp <- stage1 |> 
    filter(variable == "TMP",
           site_id == sites[i]) |>
    #group_by(datetime) |>
    #summarise(temp = mean(prediction)) |>
    arrange(datetime)
  temp_df <- temp |> collect()
  
  ggplot(temp_df, aes(x = datetime, y = prediction, group = ensemble))+
    geom_line()+
    theme_classic()
  
  ggsave(paste0("./figs/daily_temp/temp_forecast_",sites[i],"_",Sys.Date(),".jpg"), width=4, height=3, units='in')
  
}
