library(tidyverse)

  # Connect to openflow database
  con<-DBI::dbConnect(
    RPostgres::Postgres(),
    dbname = 'openflow',
    host = 'nonprod-commondb.gcp.neoninternal.org',
    port = '5432',
    user = 'shiny_openflow_rw',
    password = "ZVhWwoJopLi35P56GrZNBKlC1cIo3vrX"
  )
  
  # Read in metadata table from openflow database 
  productList <- DBI::dbReadTable(con,"sitelist")
  
  # Get data
  startDateFormat <- format(as.POSIXct("2020-01-01"),"%Y-%m-%d 00:00:00")
  endDateFormat <- format(as.POSIXct(Sys.Date())+86400,"%Y-%m-%d 00:00:00")
  dbquery <- sprintf("SELECT * FROM contqsum WHERE \"siteID\" = '%s' AND \"date\" > timestamp '%s' AND \"date\" < timestamp '%s'","WLOU",startDateFormat,endDateFormat)
  contqsum <- DBI::dbSendQuery(con,dbquery)
  contqsum <- DBI::dbFetch(contqsum)
  if(sum(!is.na(contqsum$priPrecipBulk))){
    isPrimaryPtp <- T
  }else{
    isPrimaryPtp <- F
  }
  precipSiteID <- productList$ptpSite[productList$siteID=="WLOU"]
  dbquery <- sprintf("SELECT * FROM histmedq WHERE \"siteID\" = '%s'","WLOU")
  histmedq <- DBI::dbSendQuery(con,dbquery)
  histmedq <- DBI::dbFetch(histmedq)
  contqsum <- dplyr::left_join(contqsum,histmedq,by="monthDay")


continuousDischarge_model <- contqsum |>
  select(date, meanQ, secPrecipBulk) |>
  mutate(date = as.Date(date)) |>
  group_by(date) |>
  summarise(sdQ = sd(meanQ, na.rm = T),
            meanQ = mean(meanQ, na.rm = T),
            lag_meanQ = mean(meanQ, na.rm = T),
            secPrecipBulk = sum(secPrecipBulk, na.rm = T)) |>
  mutate(lag_meanQ = lag(meanQ)) |>
  na.omit()

model <- lm(log10(meanQ+1) ~ log10(lag_meanQ+1) + secPrecipBulk, data = continuousDischarge_model)
summary(model)

int <- as.data.frame(rnorm(1000, model$coefficients[[1]], sqrt(diag(vcov(model)))[[1]]))
phi <- as.data.frame(rnorm(1000, model$coefficients[[2]], sqrt(diag(vcov(model)))[[2]]))
omega <- as.data.frame(rnorm(1000, model$coefficients[[3]], sqrt(diag(vcov(model)))[[3]]))

params <- bind_cols(int, phi, omega)
names(params)[1] <- "intercept"
names(params)[2] <- "lag"
names(params)[3] <- "precip"

date <- as.Date(max(continuousDischarge_model$date))
bucket <- "bio230014-bucket01"
path <- "neon4cast-drivers/noaa/gefs-v12/stage1"
path2 <- "neon4cast-drivers/noaa/gefs-v12/stage2"
endpoint <- "https://sdsc.osn.xsede.org"

stage1 <- 
  glue::glue("{bucket}/{path}/reference_datetime={date}") |>
  arrow::s3_bucket(endpoint_override = endpoint, anonymous = TRUE) |>
  arrow::open_dataset()


precip <- stage1 |> 
  filter(variable == "APCP",
         site_id == "WLOU") |>
  arrange(datetime) |>
  collect() |>
  group_by(datetime, ensemble) |>
  mutate(datetime = as.Date(datetime)) |>
  summarize(mean_daily_precip = sum(prediction),
            ensemble_daily_uncertainty_precip = ifelse(sum(prediction) == 0, 0.01, sd(prediction)))


ggplot(precip, aes(datetime, mean_daily_precip, group = ensemble))+
  geom_line()

noaa_ensembles <- c(unique(precip$ensemble))
forecast_dates <- c(unique(precip$datetime))

out2 <- list()
out1 <- list()

for(g in 1:length(forecast_dates)){
  for(i in 1:length(noaa_ensembles)){
    daily_mean = precip |> filter(datetime == forecast_dates[g]) |> filter(ensemble == noaa_ensembles[i])
    daily_enseble_forecast = as.data.frame(abs(rnorm(10, daily_mean$mean_daily_precip, daily_mean$ensemble_daily_uncertainty_precip)))|>
      mutate(datetime = forecast_dates[g],
             ensemble = noaa_ensembles[i]) |>
      rename(precip_forecast_ensemble = `abs(rnorm(10, daily_mean$mean_daily_precip, daily_mean$ensemble_daily_uncertainty_precip))`)
    
    out1[[i]] <- daily_enseble_forecast
    data <- do.call(rbind, out1)
  }
  out2[[g]] <- data
}

precip_forecast <- do.call(rbind, out2)

initial_precip_forecast <- precip_forecast |>
  filter(datetime == min(datetime))

forecast_function <- function(int, lag, phi, omega, precip, resid){
  est <- int + (phi * lag) + (omega * precip) + rnorm(310, 0, sd = resid)
  return(est)
}


params <- sample_n(params, 310, replace=TRUE)

initial_condition <- continuousDischarge_model %>% filter(date == max(.$date - 1)) %>%
  select(meanQ, sdQ)

initial_condition = rnorm(310, initial_condition$meanQ, initial_condition$sdQ) 

precip_forecast_go = precip_forecast |> filter(datetime == forecast_dates[g])


Q_forecast_1day <- forecast_function(lag = log(initial_condition + 1), ## sample IC
                                     precip = precip_forecast_go$precip_forecast_ensemble,
                                     int = params$intercept,
                                     phi = params$lag,
                                     omega = params$precip,
                                     resid = summary(model)$sigma) |>
  na.omit(.)

Q_forecast_1day <- as.data.frame(Q_forecast_1day)%>%
  mutate(datetime = max(continuousDischarge_model$date))

out3 <- Q_forecast_1day |> group_by(datetime) |> summarize(meanQ = mean(exp(Q_forecast_1day)+1), sdQ = sd(exp(Q_forecast_1day)+1))


### APPEND THE FORECASTS TO EACH OTHER
for(g in 2:length(forecast_dates)){
    
    params <- sample_n(params, 310, replace=TRUE)
    
    for_lag <- out3 |> filter(datetime == forecast_dates[g]-1)
    
    lag = abs(rnorm(310, for_lag$meanQ, for_lag$sdQ))
    
    precip_forecast_next = precip_forecast |> filter(datetime == forecast_dates[g])
    
    
    Q_forecast <- forecast_function(lag = log(lag+1), ## sample IC
                                         precip = precip_forecast_next$precip_forecast_ensemble,
                                         int = params$intercept,
                                         phi = params$lag,
                                         omega = params$precip,
                                         resid = summary(model)$sigma)
    
    Q_forecast <- as.data.frame(Q_forecast)|>
      mutate(datetime = forecast_dates[g])
    
    append <- Q_forecast |> group_by(forecast_dates[g]) |> summarize(meanQ = mean(exp(Q_forecast)+1), sdQ = sd(exp(Q_forecast)+1)) |>
      rename(datetime = `forecast_dates[g]`)
    
    out3 <- out3 %>% bind_rows(.,append)

}

out3 <- out3 %>% filter(datetime <= min(.$datetime + 8))
 

observed <- continuousDischarge_model %>%
  filter(date > max(.$date - 15))

ggplot(out3, aes(datetime, meanQ))+
  geom_ribbon(aes(ymin = meanQ-sdQ, ymax = meanQ+sdQ), fill = "blue", alpha = 0.2)+
  geom_line()+
  geom_point(data = observed, aes(date, meanQ), color = "black", pch = 21, fill = "red")

  
  

  hist(exp(data$Q_forecast_1day)+1)  
  