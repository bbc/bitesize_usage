## How does frequency group change over 13 weeks?
options(java.parameters = "-Xmx64g")
library(stringr)
library(RJDBC)
library(tidyverse)
library(dplyr)
library(lubridate)
library(scales)
setwd("~/Documents/Projects/DS/small_tickets/bitesize_usage")

#dir.create('~/.redshiftTools')
#download.file('http://s3.amazonaws.com/redshift-downloads/drivers/RedshiftJDBC41-1.1.9.1009.jar','~/.redshiftTools/redshift-driver.jar')

######### Get Redshift creds #########
driver <- JDBC("com.amazon.redshift.jdbc41.Driver","~/.redshiftTools/redshift-driver.jar",identifier.quote="`")
my_aws_creds <- read.csv("~/Documents/Projects/DS/redshift_creds.csv", header=TRUE, stringsAsFactors = FALSE)
url <- paste0("jdbc:redshift://localhost:5439/redshiftdb?user=",my_aws_creds$user, "&password=", my_aws_creds$password)
conn <- dbConnect(driver, url)
# test that it works:
dbGetQuery(conn, "select distinct version_id, bbc_st_pips, bbc_st_sch, brand_id  from prez.scv_vmb limit 10")

############  Add daily web usage data to table ############
# dt = ymd("2020-09-19")
# while (dt < "2021-12-01") {
#   date = gsub("-", "", dt)
#   print(date)
#   sql <- paste0(
#     "
#     INSERT INTO central_insights_sandbox.vb_bitesize_web_daily
#   SELECT is_signed_in,
#        dt::date as date_of_event,
#        count(distinct audience_id)
#   FROM s3_audience.audience_activity_daily_summary
#   WHERE dt = '",
#     date,
#     "' AND destination = 'PS_BITESIZE' GROUP BY 1, 2;
#   "
#   )
#   
#   dbSendUpdate(conn, sql)
#   
#   dt = dt + 1
# }


############  Make graph of web data ############
daily_bitesize<- dbGetQuery(conn, "SELECT * FROM central_insights_sandbox.vb_bitesize_web_daily")
daily_bitesize%>%arrange(dt, is_signed_in)%>%head()
daily_bitesize$dt<- ymd(daily_bitesize$dt)

##list dates when schools were closed
school_closure_dates<-data.frame(
  dt = daily_bitesize$dt%>%unique(),
  closure = 'no'
  
)
school_closure_dates%>%head()

school_closure_dates <- school_closure_dates %>%
  mutate(
    closure = case_when(
      dt >= '2020-03-20' & dt < '2020-06-15' ~ 'yes - lockdown 1',
      dt >= '2020-06-15' & dt < '2020-07-20' ~ 'partial',
      dt >= '2021-01-06' & dt < '2021-03-08' ~ 'yes - lockdown 3'
    )
  ) %>%
  mutate(closure = replace_na(closure, 'no'))
school_closure_dates%>%head()

daily_bitesize<-
  daily_bitesize %>%left_join(school_closure_dates, by = 'dt')
daily_bitesize%>%head()


ggplot(data = daily_bitesize,
       aes(x = dt, y = num_users, colour = closure)) +
  geom_line() +
  scale_x_date(breaks = "3 month",
               date_labels = "%Y %b") +
  scale_y_continuous(label=comma )+
  ylab("Number of Users")+
  xlab("")+
  ggtitle("Daily Users to BBC Bitesize Web")+
  theme_classic()+
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 


## dates chosen
getwd()
sample_dates<- read_csv("school_period_dates.csv")
sample_dates$date <- dmy(sample_dates$date)
sample_dates %>%head()

daily_bitesize<-
  daily_bitesize %>%left_join(sample_dates, by = c('dt'= 'date'))

daily_bitesize%>%head()

ggplot(data = daily_bitesize,
       aes(x = dt, y = num_users, colour = school_period)) +
  geom_line() +
  scale_x_date(breaks = "3 month",
               date_labels = "%Y %b") +
  scale_y_continuous(label=comma )+
  ylab("Number of Users")+
  xlab("")+
  ggtitle("Daily Users to BBC Bitesize Web")+
  theme_classic()+
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 


############  Add daily IPLAYER usage data to table ############
# dt = ymd("2019-05-01")
# while (dt < "2021-12-01") {
#   date = gsub("-", "", dt)
#   print(date)
#   sql <- paste0(
#     "
#    INSERT INTO central_insights_sandbox.vb_bitesize_iplayer_daily
# SELECT date_of_event,
#        top_level_editorial_object,
#        count(distinct audience_id) as num_users,
#        sum(playback_time_total)    as playback_time_total
# FROM audience.audience_activity_daily_summary_enriched
# WHERE date_of_event = '",
#     dt,
#     "'
#   AND destination = 'PS_IPLAYER'
#   AND brand_title ILIKE '%bitesize%'
# GROUP BY 1, 2;
#     "
#   )
#   
#   dbSendUpdate(conn, sql)
#   
#   dt = dt + 1
# }


##make graph
iplayer<-dbGetQuery(conn, "SELECT * FROM central_insights_sandbox.vb_bitesize_iplayer_daily;")

iplayer<-iplayer%>%
  mutate(playback_time_mins = round(playback_time_total/(60),0) )

iplayer$dt = ymd(iplayer$dt)
iplayer <- iplayer %>% left_join(school_closure_dates, by = 'dt')

iplayer%>%head()

##users
ggplot(data = iplayer,
       aes(x = dt, y = num_users,colour = closure )) +
  geom_line() +
  scale_x_date(breaks = "2 month",
               date_labels = "%Y %b") +
  scale_y_continuous(label=comma )+
  ylab("Number of Users")+
  xlab("")+
  ggtitle("Daily Users to BBC iPlayer (Bitesize Content)")+
  theme_classic()+
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 

## time spent viewing content
ggplot(data = iplayer,
       aes(x = dt, y = playback_time_mins, colour = closure )) +
  geom_line() +
  scale_x_date(breaks = "2 month",
               date_labels = "%Y %b") +
  scale_y_continuous(label=comma )+
  ylab("Number of Users")+
  xlab("")+
  ggtitle("Daily Users to BBC iPlayer (Bitesize Content)")+
  theme_classic()+
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) 






