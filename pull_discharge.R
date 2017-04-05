#==============================================================================
#==============================================================================
# Author: Zachary M. Smith
# Created: 1-17-2017
# Updated: 1-17-2017
# Maintained: Zachary M. Smith
# Purpose: The script was written to pull information (web scraping) from the
# USGS NWIS gages.
# URL: https://waterdata.usgs.gov/md/nwis/current/?type=flow&group_key=basin_cd
# Output: Eventually, the script will pull information from the USGS website
# daily and the information will be stored in an PostGreSQL database.
#==============================================================================
#==============================================================================
library(dplyr)
library(RPostgreSQL)
setwd("C:/Users/zsmith/Desktop/COOP/R_COOP/R_Output")
#==============================================================================
drive <- dbDriver("PostgreSQL")
con = dbConnect(drive, user = "postgres", password = "Hexapoda27",
                dbname = "COOP", host = "localhost", port = 5432)
#==============================================================================
pull_discharge <- function(site_no, begin_date, end_date, todays_date = TRUE) {
  if (todays_date == TRUE) {
    begin_date <- Sys.Date()
    end_date <- Sys.Date()
  }
  
  if (as.Date(begin_date) < "2016-10-01") {
    site.url <- paste("https://nwis.waterdata.usgs.gov/md/nwis/uv/?cb_00060=on&format=rdb&site_no=",
                      site_no, "&period=&begin_date=", begin_date, "&end_date=", end_date, sep = "")
  } else {
    site.url <- paste("https://waterdata.usgs.gov/md/nwis/uv?cb_00060=on&format=rdb&site_no=",
                      site_no, "&period=&begin_date=", begin_date, "&end_date=", end_date, sep = "")
  }
  
  
  site.df <- read.table(site.url, 
                       header = FALSE, skip = 26, stringsAsFactors = FALSE,
                       col.names = c(scan(site.url, what = "", n = 6)),
                       sep = "\t")
  
  names(site.df) <- as.character(site.df[1, ])
  names(site.df)[5] <- "discharge"
  names(site.df)[6] <- "q_code"
  final.df <- site.df[3:nrow(site.df), ]
  
  return(final.df)
}
#==============================================================================
# Test
#por.df <- pull_discharge("01639000", begin_date = "2015-01-01", end_date = "2017-01-17", todays_date = FALSE)
#==============================================================================
# POTOMAC RIVER NEAR WASH, DC LITTLE FALLS PUMP STA 
lf.df <- pull_discharge("01646500")
dbWriteTable(con, "lf_pmp_sta", lf.df, row.names = 0,  append = T)
#==============================================================================
# POTOMAC RIVER AT POINT OF ROCKS, MD 
por.df <- pull_discharge("01638500")
#==============================================================================
# MONOCACY RIVER AT JUG BRIDGE NEAR FREDERICK, MD 
# Hancock????
mon_jug.df <- pull_discharge("01643000")
#==============================================================================
# SHENANDOAH RIVER AT MILLVILLE, WV 
# SENECA????
shen_mill.df <- pull_discharge("01636500")
#==============================================================================
# POTOMAC RIVER AT HANCOCK, MD
# Goose????
hancock.df <- pull_discharge("01613000")
#==============================================================================
# SENECA CREEK AT DAWSONVILLE, MD 
# Monacacy; Jug Bridge
seneca_dawson.df <- pull_discharge("01645000")
#==============================================================================
# GOOSE CREEK NEAR LEESBURG, VA 
# POR-recess & adj done w/o Luke ????
goose_lee.df <- pull_discharge("01644000")
#==============================================================================
# OPEQUON CREEK NEAR MARTINSBURG, WV 
# Shenandoad at Millville????
opequon_martin.df <- pull_discharge("01616500")
#==============================================================================
# NORTH BRANCH POTOMAC RIVER NEAR CUMBERLAND, MD 
# Antietam????
nbp_cumber.df <- pull_discharge("01603000")
#==============================================================================
# POTOMAC RIVER AT PAW PAW, WV 
# Opequon????
paw.df <- pull_discharge("01610000")
#==============================================================================
# CONOCOCHEAGUE CREEK AT FAIRVIEW, MD
# Hancock????
conoco_fair.df <- pull_discharge("01614500")
#==============================================================================




