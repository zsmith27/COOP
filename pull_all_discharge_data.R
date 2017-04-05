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
#setwd("C:/Users/zsmith/Desktop/COOP/Data_COOP/Site_Info")

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

    # Use this fuction from the dataRetrieval package to pull the latest 
    # dischrage data.
    final.df <- readNWISdata(service = "iv",
                            site = site.vec[i],
                            startDate = "1950-10-30",
                            endDate = Sys.Date(),
                            asDateTime = FALSE,
                            #tz = "America/New_York", 
                            parameterCd = "00060") # Dischrage Code.
    
  
    final.df$dateTime <- as.POSIXct(final.df$dateTime, format = "%Y-%m-%dT%H:%M")
    names(final.df) <- c("agency_cd", "site_no", "dateTime",
                         "discharge_cfs", "qual_code", "timezone")
    
    # Export the table to the COOP database and overwrite the old table within
    # the database.
    name.string <- site.info[site.info$site_no %in% site.vec[i], 2]
    dbWriteTable(con, name.string, final.df, overwrite = TRUE , row.names = FALSE)
    
    print(paste("End:", site.vec[i]))
  }
)