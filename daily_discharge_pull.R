#==============================================================================
# Author: Zachary M. Smith
# Created: 1-17-2017
# Updated: 1-19-2017
# Maintained: Zachary M. Smith
# Purpose: The script was written to pull information from the Potomac River
#          USGS NWIS gages daily and import the data into a PostgreSQL database.
# URL: https://waterdata.usgs.gov/md/nwis/current/?type=flow&group_key=basin_cd
# Output: Eventually, the script will pull information from the USGS website
# daily and the information will be stored in an PostGreSQL database.
#==============================================================================
#==============================================================================
# Laod the dataRetrieval package created to pull USGS and EPA data into R.
library(dataRetrieval)
# dplyr and RPostgreSQL aid in the communication between R and Postgresql.
library(dplyr)
library(RPostgreSQL)
#==============================================================================
# Import a list of USGS gages for which we want to pull flow data.
setwd("C:/Users/zsmith/Desktop/COOP/Data_COOP/Site_Info")
#site.info <- read.csv("coop_site_info.csv", colClasses = rep("character", 3))
# A list of site codes.
#ite.vec <- as.character(site.info$site_no)
#==============================================================================
# Connect to the PostgreSQL database "COOP".
con = dbConnect("PostgreSQL", user = "postgres", password = "Hexapoda27",
                dbname = "COOP", host = "localhost", port = 5432)
#==============================================================================
site.info <- dbReadTable(con, "Sites")
# A list of site codes.
site.vec <- as.character(site.info$site_no)
#==============================================================================
# This loop sequences through each site and imports new data.
# The last input date in the COOP database table is used as the startDate input
# for the readNWISdata function.  Any duplicated rows are then removed and the
# new table overwrites the old table in the COOP database.
# This method should ensure that no data is excluded from the table because
# any disruption in the daily import schedual will allow the script to pick up
# from the last import date.
system.time(
for (i in seq_along(site.vec)) {
  print("================================================================================")
  print(paste("Job: ", i,"/", length(site.vec), sep = ""))
  print(paste("Start:", site.vec[i]))
  # Read in the table from the postgreSQL database "COOP."
  #old.df <- dbReadTable(con, site.vec[i])

  # Find the database code name for the site_no.
  name.string <- site.info[site.info$site_no %in% site.vec[i], 2]
  # Find the last date entered into the database.
  last.date <- dbGetQuery(con, paste('SELECT "dateTime" FROM "', name.string,
                                     '" ORDER BY "dateTime" DESC LIMIT 1;', sep = ""))
  # NWIS data is reported every 15 minutes or 900 seconds.
  # 900 seconds is added to the dateTime to pull the next sequential dateTime.
  # The format is changed to meet NWIS standards.
  last.date <- gsub(" ", "T", as.character(last.date$dateTime + 900))
  
  # Use this fuction from the dataRetrieval package to pull the latest 
  # dischrage data.
  final.df <- readNWISdata(service = "iv",
                        site = site.vec[i],
                        startDate = last.date,
                        endDate = Sys.Date(),
                        tz = "America/New_York", 
                        parameterCd = "00060") # Dischrage Code.
  
  # Append the new dataframe to the old dataframe.
  # Convert to data.table for faster sorting and removal of duplicates in 
  # next step.
  #final.df <- data.table::data.table(rbind(old.df, site.df))
  
  # Order the dataframe by dateTime and remove any duplicated rows.
  #final.df <- unique(final.df[order(dateTime)])
  
  final.df <- data.table::data.table(final.df)
  final.df <- unique(final.df)
  
  names(final.df) <- c("agency_cd", "site_no", "dateTime",
                       "discharge_cfs", "qual_code", "timezone")

  # Export the table to the COOP database and overwrite the old table within
  # the database.
  
  dbWriteTable(con, name.string, final.df, append = TRUE,  row.names = FALSE)
  #============================================================================
  # Check for duplicate dateTime in the COOP database.
  dups.check <- dbGetQuery(con, paste('SELECT "dateTime", COUNT(*) FROM  "', name.string, 
                                '" GROUP BY  "dateTime" HAVING COUNT(*) > 1; ', sep = ""))
  # If duplicate rows exist, import the table from the COOP database, remove
  # the duplicates, and export to the COOP database overwriting the old table.
  if (nrow(dups.check) > 0) {
    new.final <- dbReadTable(con, name.string) %>% 
      data.table::data.table() %>%
      unique()
    
    dbWriteTable(con, name.string, new.final, overwrite = TRUE,  row.names = FALSE)
  }
  #============================================================================
 
  print(paste("End:", site.vec[i]))
}
)


#dbWriteTable(con, "Sites", site.info, overwrite = TRUE , row.names = FALSE)
#dbGetQuery(con,  "ALTER TABLE Sites ADD PRIMARY KEY(site_no);")




test <- new.final[duplicated(new.final$dateTime), ]
new.final <- data.frame(new.final)
test <- new.final[diff(new.final$dateTime)!=15, ]
test$new <- test$dateTime +900
