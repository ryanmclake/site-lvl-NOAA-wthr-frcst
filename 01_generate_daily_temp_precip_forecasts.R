
# remotes::install_github("eco4cast/neon4cast")
# devtools::install_github("eco4cast/read4cast")

library(tidyverse)
library(neon4cast)
library(lubridate)
library(rMR)
library(arrow)

date <- Sys.Date() - 1
bucket <- "bio230014-bucket01"
path <- "neon4cast-drivers/noaa/gefs-v12/stage1"
path2 <- "neon4cast-drivers/noaa/gefs-v12/stage2"
endpoint <- "https://sdsc.osn.xsede.org"

stage1 <- 
  glue::glue("{bucket}/{path}/reference_datetime={date}") |>
  arrow::s3_bucket(endpoint_override = endpoint, anonymous = TRUE) |>
  arrow::open_dataset()

stage2 <- 
  glue::glue("{bucket}/{path2}/reference_datetime={date}") |>
  arrow::s3_bucket(endpoint_override = endpoint, anonymous = TRUE) |>
  arrow::open_dataset()

site_data <- readr::read_csv("https://raw.githubusercontent.com/eco4cast/neon4cast-targets/main/NEON_Field_Site_Metadata_20220412.csv")

# remove previous forecasts
mydir1 <- "C:/Users/mcclurer/OneDrive - National Ecological Observatory Network/Documents/R/site-lvl-NOAA-wthr-frcst/figs/daily_precip"
files_to_delete <- dir(path=mydir1 ,pattern="*.jpg")
file.remove(file.path(mydir1, files_to_delete))

mydir2 <- "C:/Users/mcclurer/OneDrive - National Ecological Observatory Network/Documents/R/site-lvl-NOAA-wthr-frcst/figs/daily_temp"
files_to_delete <- dir(path=mydir2 ,pattern="*.jpg")
file.remove(file.path(mydir2, files_to_delete))

sites <- c(site_data$field_site_id)

for( i in 1:length(sites)){
  
  precip <- stage1 |> 
    filter(variable == "APCP",
           site_id == sites[i]) |>
    arrange(datetime)
  
  precip_df <- precip |> collect() |>
    group_by(datetime) |>
    mutate(daily_mean = mean(prediction))
  
  ggplot(precip_df, aes(x = datetime, y = prediction, group = ensemble))+
    geom_line(alpha = 0.5)+
    geom_line(aes(x = datetime, y = daily_mean), color = "blue", lwd = 1) +
    ylab("precipitation (kg/m2)") +
    geom_vline(xintercept = temp_df$datetime[256], color = "red", lwd = 1) +
    theme_classic()
  
  ggsave(paste0("./figs/daily_precip/precip_forecast_",sites[i],"_",Sys.Date(),".jpg"), width=4, height=3, units='in')
  
  temp <- stage1 |> 
    filter(variable == "TMP",
           site_id == sites[i]) |>
    arrange(datetime)
  temp_df <- temp |> collect()|>
    group_by(datetime) |>
    mutate(daily_mean = mean(prediction))
  
  ggplot(temp_df, aes(x = datetime, y = prediction, group = ensemble))+
    geom_line(alpha = 0.5)+
    geom_line(aes(x = datetime, y = daily_mean), color = "blue", lwd = 1) +
    ylab("temperature (C)") +
    geom_vline(xintercept = temp_df$datetime[256], color = "red", lwd = 1) +
    theme_classic()
  
  ggsave(paste0("./figs/daily_temp/temp_forecast_",sites[i],"_",Sys.Date(),".jpg"), width=4, height=3, units='in')
  
}

