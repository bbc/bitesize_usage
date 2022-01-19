options(java.parameters = "-Xmx64g")
library(stringr)
library(RJDBC)
library(tidyverse)
library(lubridate)
library(rjson)
library(httr)
library(aws.ec2metadata)
library(aws.s3)


# ######### Get Redshift creds (local R) #########
# driver <-
#   JDBC(
#     "com.amazon.redshift.jdbc41.Driver",
#     "~/.redshiftTools/redshift-driver.jar",
#     identifier.quote = "`"
#   )
# my_aws_creds <-
#   read.csv(
#     "~/Documents/Projects/DS/redshift_creds.csv",
#     header = TRUE,
#     stringsAsFactors = FALSE
#   )
# url <-
#   paste0(
#     "jdbc:redshift://localhost:5439/redshiftdb?user=",
#     my_aws_creds$user,
#     "&password=",
#     my_aws_creds$password
#   )
# conn <- dbConnect(driver, url)

######### Get Redshift creds  MAP #########
get_redshift_connection <- function() {
  driver <-
    JDBC(
      driverClass = "com.amazon.redshift.jdbc.Driver",
      classPath = "/usr/lib/drivers/RedshiftJDBC42-no-awssdk-1.2.41.1065.jar",
      identifier.quote = "`"
    )
  url <-
    str_glue(
      "jdbc:redshift://live-idl-prod-redshift-component-redshiftcluster-1q6vyltqf8lth.ctm1v7db0ubd.eu-west-1.redshift.amazonaws.com:5439/redshiftdb?user={Sys.getenv('REDSHIFT_USERNAME')}&password={Sys.getenv('REDSHIFT_PASSWORD')}"
    )
  conn <- dbConnect(driver, url)
  return(conn)
}
# Variable to hold the connection info
conn <- get_redshift_connection()
# test that it works:
dbGetQuery(conn,"select distinct brand_title, series_title  from prez.scv_vmb limit 10"
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
  # print(dt)
  # print(tbl_name)
  # dbSendUpdate(conn, paste0("DROP TABLE IF EXISTS central_insights_sandbox.", tbl_name,";"))
  # dbSendUpdate(
  #   conn,
  # 
  #   paste0("
  #        CREATE TABLE central_insights_sandbox.",
  #        tbl_name,
  #        " AS
  # SELECT DISTINCT FNV_HASH(audience_id) as hashed_id,
  # dt::date                     as date_of_event,
  # page_name,
  # content_id,
  # app_name,
  # app_type,
  # device_type,
  # browser_brand,
  # destination,
  # page_views_total,
  # event_datetime_min::datetime as event_datetime_min,
  # event_datetime_max::datetime as event_datetime_max,
  # NULL                         as location,
  # NULL                         as town,
  # NULL                         as nation,
  # NULL                         as barb_region,
  # NULL                         as acorn_category_description
  # FROM s3_audience.audience_activity_daily_summary
  # WHERE dt = '",
  #   gsub('-', '', dt),
  #   "'
  #   AND destination = 'PS_BITESIZE'
  #   AND length(audience_id) != 43
  # 
  # UNION
  # SELECT DISTINCT FNV_HASH(audience_id) as hashed_id,
  #                 date_of_event::date          as date_of_event,
  #                 page_name,
  #                 content_id,
  #                 app_name,
  #                 app_type,
  #                 device_type,
  #                 browser_brand,
  #                 destination,
  #                 page_views_total,
  #                 event_datetime_min::datetime as event_datetime_min,
  #                 event_datetime_max::datetime as event_datetime_max,
  #                 location,
  #                 town,
  #                 nation,
  #                 barb_region,
  #                 acorn_category_description
  # FROM audience.audience_activity_daily_summary_enriched
  # WHERE date_of_event = '",
  #   dt,
  #   "'
  #   AND destination = 'PS_BITESIZE';
  #                   "
  #   )
  # )
}


#### iPlayer viewing from 2020 onwards
for (date in 4:length(dates)) {
  dt = ymd(dates[date])
  tbl_name <- paste0("bbc_iplayer_bitesize_", gsub('-', '', dt))
  tbl_names_list<-tbl_names_list%>%append(tbl_name)
  # print(dt)
  # print(tbl_name)
  # dbSendUpdate(conn, paste0("DROP TABLE IF EXISTS central_insights_sandbox.", tbl_name,";"))
  # sql<-paste0("CREATE TABLE  central_insights_sandbox.",
  #             tbl_name,
  #             " AS
  #             SELECT DISTINCT FNV_HASH(audience_id) as hashed_id,
  #             date_of_event::date          as date_of_event,
  #             page_name,
  #             app_name,
  #             app_type,
  #             version_id,
  #             device_type,
  #             browser_brand,
  #             av_content_type,
  #             destination,
  #             geo_country_site_visited,
  #             playback_time_total,
  #             play_event_count,
  #             pause_event_count,
  #             end_event_count,
  #             page_views_total,
  #             event_datetime_min::datetime as event_datetime_min,
  #             event_datetime_max::datetime as event_datetime_max,
  #             location,
  #             town,
  #             nation,
  #             barb_region,
  #             acorn_category_description,
  #             on_air_version_id,
  #             top_level_editorial_object,
  #             programme_title,
  #             master_brand_name,
  #             brand_id,
  #             brand_title,
  #             series_id,
  #             series_title,
  #             episode_id,
  #             episode_title,
  #             clip_id,
  #             clip_title,
  #             pips_genre_level_1_names,
  #             bbc_st_pips,
  #             programme_duration
  #             FROM audience.audience_activity_daily_summary_enriched
  #             WHERE date_of_event = '",
  #          dt,
  #          "'
  #          AND destination = 'PS_IPLAYER'
  #          AND (brand_title ILIKE '%bitesize%' OR top_level_editorial_object ILIKE '%bitesize%');
  #          "
  #   )
  # 
  # dbSendUpdate(
  #   conn,sql
  # )
}

################## file sizes ##################
tbl_size<-data.frame(tbl_name = as.character(), num_rows = as.numeric(), pull_into_r = as.character())

for(tbl in 1:length(tbl_names_list)){
  size <-dbGetQuery(conn, paste0(" SELECT count(*) FROM central_insights_sandbox.",tbl_names_list[tbl],";"))
  tbl_size<-tbl_size %>% rbind(data.frame(tbl_name = tbl_names_list[tbl], 
                                          num_rows = size,
                                          pull_into_r = case_when(size <=2000000 ~ 'Y',
                                                                  size >2000000 ~ 'N')
                                          ))
}
tbl_size

################## Upload to s3 ##################

get_s3_credentials <- function() {
  role_name <-httr::content(httr::GET("http://169.254.169.254/latest/meta-data/iam/security-credentials/"))
  s3credentials <-jsonlite::fromJSON(httr::content(httr::GET(paste0("http://169.254.169.254/latest/meta-data/iam/security-credentials/", role_name))))
  
  return(s3credentials)
}

s3credentials<- get_s3_credentials()

### this uploads it as an unspecified file, you will need to download and then rename for it to become a .csv file
## but this doesn't pull the data into R so is good for large files/tables.
tbl_size %>%filter(pull_into_r == 'N')
for(table in 1:nrow(tbl_size%>%filter(pull_into_r == 'N'))) {
  tbl_size$tbl_name[table]
  if (tbl_size$pull_into_r[table] == "N") {
    sql <- paste0(
      "
      UNLOAD ('select * from central_insights_sandbox.",
      tbl_names_list[table],
      "')
      to 's3://map-input-output/vicky/",
      tbl_names_list[table] ,
      "'
      CREDENTIALS 'aws_access_key_id=<AWS_ACCESS_KEY_ID>;aws_secret_access_key=<AWS_SECRET_ACCESS_KEY>;token=<TOKEN>'
      parallel off
      CSV

      ALLOWOVERWRITE
      ;"
      )

    sql <-
      stringr::str_replace(sql, '<AWS_ACCESS_KEY_ID>', s3credentials$AccessKeyId)
    sql <-
      stringr::str_replace(sql,
                           '<AWS_SECRET_ACCESS_KEY>',
                           s3credentials$SecretAccessKey)
    sql <- stringr::str_replace(sql, '<TOKEN>', s3credentials$Token)

    dbSendUpdate(conn, sql)
  }

}




## this pulls in the table from Redshift to an R object and saves to s3 as a .csv, 
##but pulling in a large table can be difficult and generally this can be a bit slow
extractDataToS3 <- function(data, table_name = "table-name", bucket = "bucket-name") {
  filename_final <- paste0(table_name,".csv")# Generate Filename
  write.csv(data, filename_final, row.names = FALSE)# Write to csv
  
  # Put into s3
  put_object(paste0(getwd(),"/",filename_final) , bucket=bucket)                              # CHANGE bucket= to your s3 folder location
  message(paste0(filename_final," into S3 complete"))
}

tbl_size %>%filter(pull_into_r == 'Y')
for(table in 6:nrow(tbl_size%>%filter(pull_into_r == 'Y'))) {
  if (tbl_size$pull_into_r[table] == "Y") {
    sql_query <- paste0("select * from central_insights_sandbox.", tbl_names_list[table])
    
    redshift_extract <- dbGetQuery(conn, sql_query)
    print(redshift_extract %>% nrow())
    extractDataToS3(redshift_extract,
                    table_name = tbl_names_list[table], # CHANGE this to a reference to the table you are extracting, it will form part of the csv file name
                    bucket = 'map-input-output/vicky') # CHANGE this to s3 bucket filepath

    }
}
