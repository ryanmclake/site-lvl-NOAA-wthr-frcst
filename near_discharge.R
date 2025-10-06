install.packages("keras")
remotes::install_github("rstudio/tensorflow")

Sys.setenv(RETICULATE_PYTHON="/usr/local/bin/python")

library(keras)
library(tensorflow)
library(tidyverse)

install_tensorflow(envname = "r-tensorflow")


discharge <- get.cont.Q.NEON.API(site.id = "MAYF", start.date = "2022-01-01", end.date = Sys.Date() - 2)

list2env(discharge,envir=.GlobalEnv)

plot(continuousDischarge_sum$meanQ)


scaledQ <- scale(continuousDischarge_sum$meanQ)

train_data <- scaledQ[1:5000,]
test_data <- scaledQ[5001:83160,]

train_data <- array(train_data, dim = c(nrow(train_data), 1, ncol(train_data)))
test_data <- array(test_data, dim = c(nrow(test_data), 1, ncol(test_data)))


model <- keras_model_sequential() %>%
  layer_lstm(units = 50, input_shape = c(1, ncol(train_data))) %>%
  layer_dense(units = 1)

model %>% compile(
  loss = 'mean_squared_error',
  optimizer = 'adam'
)

  

