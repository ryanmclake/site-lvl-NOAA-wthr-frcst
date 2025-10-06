
devtools::install_github(repo = "NEONScience/NEON-stream-discharge/neonStageQPlot", dependencies = TRUE, force = TRUE)

library(neonStageQplot)

discharge <- get.cont.Q.NEON.API(site.id = "HOPB", start.date = "2015-09-04", end.date = Sys.Date() - 2)

list2env(discharge,envir=.GlobalEnv)

weather_stage3 <- neon4cast::noaa_stage3()

precip_old <- weather_stage3 |> 
  dplyr::filter(site_id == "MAYF") |>
  dplyr::collect() |>
  dplyr::filter(variable == "precipitation_flux") |>
  mutate(prediction = 86400 * prediction) |>
  select(site_id, parameter, datetime, prediction)

daily_precip <- precip_old |>
  mutate(date = lubridate::date(datetime)) |>
  group_by(date, parameter, site_id) |>
  summarize(daily_sum_rain = sum(prediction))

daily_precip_mean <- daily_precip |>
  mutate(date = lubridate::date(date)) |>
  group_by(date, site_id) |>
  summarize(parameter_mean = mean(daily_sum_rain))

zSummary <- continuousDischarge_sum %>% select(meanQ, dischargeFinalQFSciRvw) %>% 
  na.omit(.) %>%
  mutate(scaledDischarge = scale(meanQ)) %>%
  summarize(globalMedian = median(scaledDischarge),
            Q1 = quantile(scaledDischarge, 0.25),
            Q3 = quantile(scaledDischarge, 0.75),
            IQR = IQR(scaledDischarge))

csd_raw_flagged <- continuousDischarge_sum %>%
  arrange(date) %>%
  mutate(scaledDischarge = scale(meanQ),
         floodFlag = ifelse(scaledDischarge>(zSummary$Q1-10*zSummary$IQR)&scaledDischarge<(zSummary$Q3+10*zSummary$IQR),0,1),
         date = lubridate::date(date)) %>%
  select(date, meanQ, floodFlag) %>%
  group_by(date) %>%
  summarise_all(funs(median), na.rm = T) %>%
  mutate(month = lubridate::month(date))

ggplot(csd_raw_flagged, aes(date, meanQ)) +
  geom_line(data = daily_precip_mean, aes(date, parameter_mean), color = "red", inherit.aes = F)+
  geom_point(size = 0.5)


model <- left_join(daily_precip_mean, csd_raw_flagged, by = "date") |>
  ungroup() %>%
  mutate(ar_1 = lag(meanQ)) %>%
  tsibble::as_tsibble(index = date)

mod <- glm(meanQ ~ parameter_mean + ar_1, data = model, na.action = "na.omit")
summary(mod)

mod_fits <- model %>%  
  tsibble::fill_gaps() %>%
  fabletools::model(
    mod_mean = fable::MEAN(meanQ))

fc_null <- mod_fits %>%
  fabletools::forecast(h = "30 days") 

target_view <- model %>%
  filter(date >= "2024-01-01")

fc_null %>% 
  autoplot(target_view) +
  facet_grid(.model ~ ., scales = "free_y") +
  theme_bw()

met_past <- model %>%
  filter(date < "2024-08-06")
  
  
met_future <- 

precip <- stage1 |> 
  filter(variable == "PWAT",
         site_id == "MAYF") |>
  arrange(datetime)

precip_df <- precip |> collect()

forecasted_precip <- precip_df %>%
  filter(ensemble != "gec00") %>%
  filter(site_id == "MAYF") %>%
  mutate(ensemble = as.numeric(gsub('gep', '', ensemble))) %>%
  rename(parameter = ensemble)%>%
  mutate(date = lubridate::date(datetime))%>%
  group_by(date, parameter)%>%
  summarize(daily_precip_mean = mean(prediction))


met_past <- model %>%
  filter(date < "2024-08-06")





mod_fit_candidates <- met_past %>%
  fabletools::model(
    mod_precip = fable::TSLM(meanQ ~ parameter_mean + ar_1))

fabletools::report(mod_fit_candidates)

fabletools::augment(mod_fit_candidates) %>%
  ggplot(aes(x = date)) +
  geom_line(aes(y = meanQ, lty = "Obs"), color = "black") +
  geom_line(aes(y = .fitted, color = .model, lty = "Model")) +
  theme_bw()


met_future <- forecasted_precip %>%
  mutate(ar_1 = NA) %>%
  rename(parameter_mean = daily_precip_mean)

model


parameters <- seq(from = 1, to = 30, by = 1)
dates <- c(unique(forecasted_precip$date))
out <- list()

for(i in 1:length(parameters)) {
  
ens <- met_future %>%
  filter(parameter == parameters[i])

ens[1,4] <- 29.09900


  for(s in 1:length(dates))
    
    predict <- ens %>% filter(date == dates[s])

    step <- predict %>%
      dplyr::mutate(forecasted_discharge = mod$coefficients[1] + 
                      parameter_mean*mod$coefficients[2] +
                      ar_1*mod$coefficients[3])
    
      
    
  for(s in 1:length(horizon)){
    
    IC <- forecast %>%
      filter(parameter == parameters[1]) %>%
      filter(horizon == (horizons[s]))
    
    p_Q <- left_join(precip_old, Q, by = "datetime") |>
      filter(datetime >= "2021-01-01 01:00:00") %>%
      filter(parameter == parameters[i]) %>%
      mutate(ar_1 = lag(maxpostDischarge)) %>%
      mutate(predicted_discharge = coeff$estimate[1] + prediction*coeff$estimate[2] + ar_1*coeff$estimate[3]) %>%
      select(-site_id.y) %>%
      rename(siteID = site_id.x)
    
    
    
  }

  
  out[[i]] <- p_Q
  
  
  
}

predictions <- do.call("rbind", out) 

ggplot(predictions, aes(datetime, predicted_discharge, group = parameter))+
  geom_line(alpha = 0.5)+
  geom_point(data = predictions, aes(datetime, maxpostDischarge), color = "red")


