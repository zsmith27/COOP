
library(lubridate)
library(ggplot2)

setwd("C:/Users/zsmith/Desktop/COOP/Data_COOP")
k.lag <- read.csv("k_lag.csv")
org.df <- read.csv("test_data_01_12_17.csv")
#==============================================================================
names(org.df) <- toupper(names(org.df))
#==============================================================================
names(org.df)[names(org.df) %in% "DATETIME"] <- "DATE_TIME"
org.df$DATE_TIME <- as.character(org.df$DATE_TIME)



org.df$DATE_TIME <- as.POSIXct( org.df$DATE_TIME, format = "%m/%d/%Y %H:%M")
#==============================================================================
new.df <- org.df[, 1:6]
names(new.df)[names(new.df) %in% "POR"] <- "POINT_OF_ROCKS"
new.df$SEN <- c(NA, diff(new.df$SENECA))
new.df$GOO <- c(NA, diff(new.df$GOOSE))
new.df$MON <- c(NA, diff(new.df$MONOCACY.AT.JUG.BR))
new.df$POR <- c(NA, diff(new.df$POINT_OF_ROCKS))
new.df$DELTA <- rowSums(new.df[, c("LFALLS.OBSERVED", "SEN", "GOO", "MON", "POR")])


ggplot(new.df, aes(x = DATE_TIME, y = LFALLS.OBSERVED)) + 
  geom_line(color = "green", size = 2) +
  geom_line(aes(x = DATE_TIME, y = DELTA), color = "blue", size = 2, alpha = 0.5)

diff(head(new.df$SENECA), lag = 1)

nl <- 4
new.df$TEST <- head(c(rep(NA, nl), diff(new.df$SENECA, lag = 1)), nrow(new.df))

lag_func <- function(my.data , sen.lag, goo.lag = 12, mon.lag = 24, por.lag = 24){
  my.data$SEN_LAG <- head(c(rep(NA, sen.lag), my.data$SENECA), nrow(my.data)) * 1.05
  my.data$GOO_LAG <- head(c(rep(NA, goo.lag), my.data$GOOSE), nrow(my.data)) * 1.05
  my.data$MON_LAG <- head(c(rep(NA, mon.lag), my.data$MONOCACY.AT.JUG.BR), nrow(my.data)) * 1.4
  my.data$POR_LAG <- head(c(rep(NA, por.lag), my.data$POINT_OF_ROCKS), nrow(my.data)) * 1.02
  my.data$PER <- rowSums(my.data[, c("SEN_LAG", "GOO_LAG", "MON_LAG", "POR_LAG")], na.rm = TRUE) - 600
  return(my.data)
}

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
