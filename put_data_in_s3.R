options(java.parameters = "-Xmx64g")
library(stringr)
library(RJDBC)
library(tidyverse)
library(lubridate)


######### Get Redshift creds #########
driver <-
  JDBC(
    "com.amazon.redshift.jdbc41.Driver",
    "~/.redshiftTools/redshift-driver.jar",
    identifier.quote = "`"
  )
my_aws_creds <-
  read.csv(
    "~/Documents/Projects/DS/redshift_creds.csv",
    header = TRUE,
    stringsAsFactors = FALSE
  )
url <-
  paste0(
    "jdbc:redshift://localhost:5439/redshiftdb?user=",
    my_aws_creds$user,
    "&password=",
    my_aws_creds$password
  )
conn <- dbConnect(driver, url)
# test that it works:
dbGetQuery(
  conn,
  "select distinct version_id, bbc_st_pips, bbc_st_sch, brand_id  from prez.scv_vmb limit 10"
)


######### dates_required #########

dates <- c(
  '2019-05-22',
  '2019-09-25',
  '2019-10-30',
  '2020-04-22',
  '2020-05-20',
  '2020-09-23',
  '2020-10-28',
  '2021-01-20',
  '2021-05-19',
  '2021-09-22',
  '2021-10-27'
)
################## Create the tables ##################
dt = ymd(dates[1])
gsub('-', '', dt)

## store tbl_names
tbl_names_list<-c()

tbl_name <- paste0("bbc_bitesize_", dt)

for (date in 1:length(dates)) {
  dt = ymd(dates[date])
  tbl_name <- paste0("bbc_bitesize_", gsub('-', '', dt))
  tbl_names_list<-tbl_names_list%>%append(tbl_name)
  print(dt)
  print(tbl_name)
  dbSendUpdate(
    conn,
  
    paste0("
           CREATE TABLE central_insights_sandbox.",
           tbl_name,
           " AS
    SELECT DISTINCT audience_id,
    dt::date                     as date_of_event,
    page_name,
    content_id,
    app_name,
    app_type,
    device_type,
    browser_brand,
    destination,
    page_views_total,
    event_datetime_min::datetime as event_datetime_min,
    event_datetime_max::datetime as event_datetime_max,
    NULL                         as location,
    NULL                         as town,
    NULL                         as nation,
    NULL                         as barb_region,
    NULL                         as acorn_category_description
    FROM s3_audience.audience_activity_daily_summary
    WHERE dt = '",
      gsub('-', '', dt),
      "'
      AND destination = 'PS_BITESIZE'
      AND length(audience_id) != 43

    UNION
    SELECT DISTINCT audience_id,
                    date_of_event::date          as date_of_event,
                    page_name,
                    content_id,
                    app_name,
                    app_type,
                    device_type,
                    browser_brand,
                    destination,
                    page_views_total,
                    event_datetime_min::datetime as event_datetime_min,
                    event_datetime_max::datetime as event_datetime_max,
                    location,
                    town,
                    nation,
                    barb_region,
                    acorn_category_description
    FROM audience.audience_activity_daily_summary_enriched
    WHERE date_of_event = '",
      dt,
      "'
      AND destination = 'PS_BITESIZE';
                      "
    )
  )
}


#### iPlayer viewing from 2020 onwards
for (date in 4:length(dates)) {
  dt = ymd(dates[date])
  tbl_name <- paste0("bbc_iplayer_bitesize_", gsub('-', '', dt))
  tbl_names_list<-tbl_names_list%>%append(tbl_name)
  print(dt)
  print(tbl_name)
  dbSendUpdate(
    conn,
    
    paste0("
           CREATE TABLE central_insights_sandbox.",
           tbl_name,
           " AS
    SELECT DISTINCT audience_id,
                date_of_event::date          as date_of_event,
                page_name,
                app_name,
                app_type,
                version_id,
                device_type,
                browser_brand,
                av_content_type,
                destination,
                geo_country_site_visited,
                playback_time_total,
                play_event_count,
                pause_event_count,
                end_event_count,
                page_views_total,
                event_datetime_min::datetime as event_datetime_min,
                event_datetime_max::datetime as event_datetime_max,
                location,
                town,
                nation,
                barb_region,
                acorn_category_description,
                on_air_version_id,
                top_level_editorial_object,
                programme_title,
                master_brand_name,
                brand_id,
                brand_title,
                series_id,
                series_title,
                episode_id,
                episode_title,
                clip_id,
                clip_title,
                pips_genre_level_1_names,
                bbc_st_pips,
                programme_duration
FROM audience.audience_activity_daily_summary_enriched
    WHERE date_of_event = '",
           dt,
           "'
  AND destination = 'PS_IPLAYER'
  AND (brand_title ILIKE '%bitesize%' OR top_level_editorial_object ILIKE '%bitesize%');
                      "
    )
  )
}

################## Upload to s3 ##################

tbl_names_list









