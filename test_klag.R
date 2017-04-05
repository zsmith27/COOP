#==============================================================================
#==============================================================================
# Author: Zachary M. Smith
# Created: 1-24-2017
# Updated: 1-25-2017
# Maintained: Zachary M. Smith
# Purpose:
# Output: 
#==============================================================================
#==============================================================================
# Load necessary packages.
library(lubridate)
library(ggplot2)
library(dplyr)
library(RPostgreSQL)
library(zoo)
#==============================================================================
# Set working directory.
setwd("C:/Users/zsmith/Desktop/COOP/Data_COOP")
#==============================================================================
# Import the K-Lag table.
k.lag <- read.csv("k_lag.csv")
#==============================================================================
# Connect to the PostgreSQL database "COOP".
con = dbConnect("PostgreSQL", user = "postgres", password = "Hexapoda27",
                dbname = "COOP", host = "localhost", port = 5432)
#==============================================================================
# Import site information.
site.info <- dbReadTable(con, "Sites")
#==============================================================================
#x <- import.df
hour_agg <- function(x, name_col){
  
  #final.df <- aggregate(x[ , "discharge_cfs"], 
  #                      list(cut(as.POSIXct(x[ , "dateTime"]) - 1, "hour")),
  #                      mean)
  
  x$dateTime <- as.POSIXct(round(x$dateTime, units = "hours"))
  final.df <- aggregate(discharge_cfs ~ dateTime, data = x, FUN = mean)
  names(final.df) <- c("DATE_TIME", paste(name_col, "FLOW", sep = "_"))
  final.df$DATE_TIME <- as.POSIXct(final.df$DATE_TIME) + 3600
  return(final.df)
}
#==============================================================================
import_gage <- function(gage) {
  import.df <- dbReadTable(con, gage)
  #import.df$dateTime <- as.POSIXct(import.df$dateTime)
  import.df$MONTH <- format(import.df$dateTime, "%m")
  import.df <- import.df[import.df$MONTH %in% c("06", "07", "08"),]#, "09", "10"), ]
  final.df <- hour_agg(import.df, gage)
  return(final.df)
}
#==============================================================================
por <- import_gage("POR")
mon_jug <- import_gage("MON_JUG")
goose <- import_gage("GOOSE")
seneca <- import_gage("SENECA")
lfall <- import_gage("LFALL")
#==============================================================================
merged <- plyr::join_all(list(por, mon_jug, goose, seneca, lfall), by = "DATE_TIME")
merged$YEAR <- format(merged$DATE_TIME, "%Y")
#==============================================================================
# Function that returns Root Mean Squared Error.
rmse <- function(error) sqrt(mean(error ^ 2, na.rm = TRUE))
# Function that returns Mean Absolute Error.
mae <- function(error) mean(abs(error), na.rm = TRUE)
# Function that returns hours.
hours <- function(x) x * 3600
#==============================================================================
int_lag <- function(k.tbl, lag1, lag2, gage.col) {
 return (k.tbl$LAG[lag1] + 
    ((k.tbl$LAG[lag2] - k.tbl$LAG[lag1]) * 
       (gage.col - k.tbl$FLOW[lag1]) / 
       (k.tbl$FLOW[lag2] - k.tbl$FLOW[lag1])))
}

#==============================================================================
#org <- org.df
#flow <- flow.df
#gage.name <- gage
#k.tbl <- k.pot
lag_k <- function(org, flow, gage.name, k.tbl) {
  #gage.name <- sub("^(.*)[_].*", "\\1", gage.name)
  flow$LAG <- ifelse(flow[, gage.name] <= k.tbl$FLOW[1], k.tbl$LAG[1], NA)
  
  for (i in 2:nrow(k.tbl)) {
    if (is.na(k.tbl$FLOW[i]) == FALSE) {
      flow$LAG <- ifelse(flow[, gage.name] > k.tbl$FLOW[i - 1] & flow[, gage.name] <= k.tbl$FLOW[i],
                         int_lag(k.tbl, i - 1, i, flow[, gage.name]), flow$LAG)
    }
  }
  
  max.klag <- max(k.tbl$FLOW, na.rm = TRUE)
  flow$LAG <- ifelse(flow[, gage.name] >= max.klag,
                     k.tbl[k.tbl$FLOW == max.klag, "LAG"],
                     flow$LAG)
  flow$LAG <- flow$DATE_TIME + flow$LAG * 3600
  
  lag.name <- paste(gage.name, "LAG", sep = "_")
  
  lag.df <- flow[, c("LAG", gage.name)]
  names(lag.df) <- c("DATE_TIME", lag.name)
  lag.df <- lag.df[complete.cases(lag.df), ]
  lag.df$DATE_TIME <- as.POSIXct(round(lag.df$DATE_TIME, units = "hours")) 
  #lag.df$DATE_TIME <- as.POSIXct(ceiling_date(lag.df$DATE_TIME, unit = "hours")) 
  lag.df <- aggregate(. ~ DATE_TIME, data = lag.df, FUN = mean)
  merged1 <- plyr::join_all(list(org, lag.df), by = "DATE_TIME")
  mt <- merged1[duplicated(merged1$DATE_TIME), ]
  # Need to solve Daylight savings time issue to incorporate Nov.
  interp.df <- data.frame(na.approx(zoo(x = merged1[, paste(gage.name, "LAG", sep = "_")],
                                        order.by = merged1$DATE_TIME)))
  interp.df$DATE_TIME <- row.names(interp.df)
  names(interp.df) <- c(lag.name, "DATE_TIME")
  interp.df$DATE_TIME <- as.POSIXct(interp.df$DATE_TIME)
  return(interp.df)
}
#==============================================================================
#org.df <- merged
#gage <- "POR_FLOW"
#k.table <- k.lag2
#trib.gage <- "MON_JUG_FLOW"
#pot.lag <- "POR_1"
calc_lag <- function(org.df, gage, k.table, trib.gage, pot.lag) {
  #============================================================================
  # Copy the orginal data frame to make edits.
  flow.df <- org.df
  #============================================================================
  # Calculate the lag of the instream Potomac River.
  k.pot <- k.table[k.table$GAGE %in% pot.lag, ]
  flow.pot <- lag_k(org.df, flow.df, gage, k.pot)
  # Calculate the lag of a tributary to the Potomac River.
  trib.gage.name <- sub("^(.*)[_].*", "\\1", trib.gage)
  k.trib <- k.table[k.table$GAGE %in% trib.gage.name, ]
  flow.trib <- lag_k(org.df, flow.df, trib.gage, k.trib)
  #============================================================================
  confluence.df <- merge(flow.pot, flow.trib, by = "DATE_TIME", all = TRUE)
  confluence.df$SIM_FLOW <- rowSums(confluence.df[, 2:3], na.rm = TRUE)
  names(confluence.df)[4] <- pot.lag
  #============================================================================
  final.df <- merge(org.df, confluence.df, by = "DATE_TIME", all = TRUE)
  
  #names(org.df)[names(org.df) %in% "LAG"] <- paste(gage.name, "LAG", sep = "_")
  return(final.df)
}
#==============================================================================
por1.lag <- calc_lag(merged, "POR_FLOW", k.lag, "MON_JUG_FLOW", "POR_1")
por2.lag <- calc_lag(por1.lag, "POR_1", k.lag, "GOOSE_FLOW", "POR_2")
por3.lag <- calc_lag(por2.lag, "POR_2", k.lag, "SENECA_FLOW", "POR_3")
k.4 <- k.lag[k.lag$GAGE %in% "POR_4", ]
por4.lag <- lag_k(por3.lag, por3.lag, "POR_3", k.4)
names(por4.lag) <- c("SIM_FLOW", "DATE_TIME")
sim.lag <- merge(merged, por4.lag, by = "DATE_TIME", all = TRUE)
#==============================================================================
for (i in unique(sim.lag$YEAR)) {
  plot.me <- sim.lag[as.character(sim.lag$YEAR) %in% i, ]
  #plot.me <- plot.me[plot.me$DATE_TIME < "2008-07-10", ]
  print( ggplot(plot.me, aes(x = DATE_TIME, y = LFALL_FLOW)) + 
           labs(title = i) +
           geom_line(color = "green", size = 2) +
           geom_line(aes(y = SIM_FLOW), color = "blue", size = 2, alpha = 0.5))
}
#==============================================================================
head(import.df$dateTime, 11)
round(head(import.df$dateTime, 11), units ="hours")
#==============================================================================
test.this <- function(k.lag){
  por1.lag <- calc_lag(merged, "POR_FLOW", k.lag, "MON_JUG_FLOW", "POR_1")
  por2.lag <- calc_lag(por1.lag, "POR_1", k.lag, "GOOSE_FLOW", "POR_2")
  por3.lag <- calc_lag(por2.lag, "POR_2", k.lag, "SENECA_FLOW", "POR_3")
  k.4 <- k.lag[k.lag$GAGE %in% "POR_4", ]
  por4.lag <- lag_k(por3.lag, por3.lag, "POR_3", k.4)
  names(por4.lag) <- c("SIM_FLOW", "DATE_TIME")
  sim.lag <- merge(merged, por4.lag, by = "DATE_TIME", all = TRUE)
  plot.me <- sim.lag[as.character(sim.lag$YEAR) %in% 2016, ]
  ggplot(plot.me, aes(x = DATE_TIME, y = LFALL_FLOW)) + 
    labs(title = "2016") +
    geom_line(color = "green", size = 2) +
    geom_line(aes(y = SIM_FLOW), color = "blue", size = 2, alpha = 0.5)
}
#==============================================================================

k.lag1 <- k.lag
k.lag2 <- k.lag
k.lag2$LAG <- k.lag2$LAG * 0.7
test.this(k.lag1)
test.this(k.lag2)
#==============================================================================







mon.lag <- calc_lag(por.lag, "MON_JUG_FLOW", k.lag, "GOOSE_FLOW")

goo.lag <- calc_lag(mon.lag, "GOOSE_FLOW", k.lag, "SENECA_FLOW")
#goo.lag <- calc_lag(mon.lag, "SENECA_FLOW", k.lag, "LFALL_FLOW")
goo.lag$LFALL_SIM <- goo.lag$SENECA_FLOW



for (i in unique(goo.lag$YEAR)) {
  plot.me <- goo.lag[as.character(goo.lag$YEAR) %in% i, ]
  #plot.me <- plot.me[plot.me$DATE_TIME < "2008-07-10", ]
 print( ggplot(plot.me, aes(x = DATE_TIME, y = LFALL_FLOW)) + 
          labs(title = i) +
    geom_line(color = "green", size = 2) +
    geom_line(aes(y = LFALL_SIM), color = "blue", size = 2, alpha = 0.5))
}




plot <- plot.me[duplicated(plot.me$DATE_TIME), ]


