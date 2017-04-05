#==============================================================================
#==============================================================================
# Author: Zachary M. Smith
# Created: 1-23-2017
# Updated: 1-24-2017
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
por <- dbReadTable(con, "POR")
por$MONTH <- format(por$dateTime, "%m")
por <- por[por$MONTH %in% c("07", "08", "09"), ]
lfall <- dbReadTable(con, "LFALL")
lfall$MONTH <- format(lfall$dateTime, "%m")
lfall <- lfall[lfall$MONTH %in% c("07", "08", "09"), ]
#==============================================================================
hour_agg <- function(x, name_col){
  final.df <- aggregate(x[ , "discharge_cfs"], 
                   list(cut(as.POSIXct(x[ , "dateTime"]) - 1, "hour")),
                   mean)
  names(final.df) <- c("DATE_TIME", paste(name_col, "FLOW", sep = "_"))
  final.df$DATE_TIME <- as.POSIXct(final.df$DATE_TIME) + 3600
  return(final.df)
}
#==============================================================================
por.df <- hour_agg(por, "POR")
lfall.df <- hour_agg(lfall, "LFALL")
merged <- merge(lfall.df, por.df, by = "DATE_TIME", all = TRUE)
merged$YEAR <- format(merged$DATE_TIME, "%Y")
#==============================================================================
# Function that returns Root Mean Squared Error.
rmse <- function(error) sqrt(mean(error^2, na.rm = TRUE))
# Function that returns Mean Absolute Error.
mae <- function(error) mean(abs(error), na.rm = TRUE)
# Function that returns hours.
hours <- function(x) x * 3600
#==============================================================================
j <- "2008"
i <- 5
final.lag <- list()
for (j in unique(merged$YEAR)) {
  sub.merged <- merged[merged$YEAR %in% j, ]
  list.lag <- list()
  for (i in c(1:1000)) {
    sub.merged$SIM <- c(rep(NA, i), head(sub.merged$POR_FLOW, nrow(sub.merged) - i))
    sub.merged$RES <- sub.merged$LFALL_FLOW - sub.merged$SIM
    sim.df <- sub.merged[sub.merged$LFALL_FLOW < 1000000, ]
    if(nrow(sim.df[!is.na(sim.df$SIM), ]) < 30) next
    final.df <- data.frame(LAG = i)
    final.df$RMSE <- rmse(sim.df$RES)
    final.df$MAE <- mae(sim.df$RES)
    list.lag[[i]] <- final.df
  }

  final.lag[[j]] <- do.call(rbind, list.lag)
}

bind <- do.call(rbind, final.lag)
bind <- bind[order(bind$RMSE), ]
bind <- bind[order(bind$MAE), ]

new.df$SIM <- c(rep(NA, bind[1, "LAG"]), head(new.df$POR, nrow(new.df) - bind[1, "LAG"]))
sim.df <- new.df[new.df$LFALLS.OBSERVED < 100000000, ]

ggplot(sim.df, aes(x = DATE_TIME, y = LFALLS.OBSERVED)) + 
  geom_line(color = "green", size = 2) +
  geom_line(aes(x = DATE_TIME, y = SIM), color = "red", size = 2, alpha = 0.5)




k.table <- data.frame(FLOW = as.numeric(c("500", "1000", "5000", "10000", "100000")), 
                      LAG = as.numeric(c("60", "48", "36", "24", "18")))


test$NEW <- ifelse(test$POINT_OF_ROCKS <= k.table$FLOW[1], k.table$LAG[1],
                   ifelse(test$POINT_OF_ROCKS >= k.table$FLOW[4], k.table$LAG[4],
                          ifelse(test$POINT_OF_ROCKS == k.table$FLOW[2], k.table$LAG[2],
                                 ifelse(test$POINT_OF_ROCKS == k.table$FLOW[3], k.table$LAG[3],
                                        ifelse(test$POINT_OF_ROCKS > k.table$FLOW[1] & test$POINT_OF_ROCKS < k.table$FLOW[2],
                                               k.table$LAG[1] + ((k.table$LAG[2] - k.table$LAG[1]) * (test$POINT_OF_ROCKS - k.table$FLOW[1]) / (k.table$FLOW[2] - k.table$FLOW[1])),
                                               ifelse(test$POINT_OF_ROCKS > k.table$FLOW[2] & test$POINT_OF_ROCKS < k.table$FLOW[3],
                                                      k.table$LAG[2] + ((k.table$LAG[3] - k.table$LAG[2]) * (test$POINT_OF_ROCKS - k.table$FLOW[2]) / (k.table$FLOW[3] - k.table$FLOW[2])),
                                                      ifelse(test$POINT_OF_ROCKS > k.table$FLOW[3] & test$POINT_OF_ROCKS < k.table$FLOW[4],
                                                             k.table$LAG[3] + ((k.table$LAG[4] - k.table$LAG[3]) * (test$POINT_OF_ROCKS - k.table$FLOW[3]) / (k.table$FLOW[4] - k.table$FLOW[3])), "FAIL")))))))



test$NEW_TIME <- test$DATE_TIME + as.numeric(test$NEW) * 3600

new.df <- test[, c("NEW_TIME", "POINT_OF_ROCKS")]
new.df <- new.df[order(new.df$NEW_TIME), ]






names(new.df2) <- c("NEW_TIME2", "POR")
new.df3 <- cbind(new.df, new.df2)

new.df3$TEST <- round(new.df3$NEW_TIME2, units="hours")


plot(new.df$NEW_TIME, new.df$POINT_OF_ROCKS)

L1 + ((L2 - L1) * Flow / (F2 - F1))

diff(k.table$FLOW)
test <- lag_func(new.df, sen.lag = 12, goo.lag = 12, mon.lag = 24, por.lag = 24)

ggplot(new.df3, aes(x = NEW_TIME2, y = POINT_OF_ROCKS)) + 
  geom_line(color = "green", size = 2) +
  geom_line(aes(x = TEST, y = POR), color = "blue", size = 2, alpha = 0.5)

tt <- new.df3[diff(new.df3$NEW_TIME) < 0, ]
