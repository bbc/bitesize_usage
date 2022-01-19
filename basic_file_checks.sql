set search_path TO 'central_insights_sandbox';

SELECT * FROM bbc_bitesize_20200422 LIMIT 10;


-- rows
SELECT count(*) FROM bbc_bitesize_20200422;--12,262,133

-- audience_id
SELECT count(distinct hashed_id) FROM bbc_bitesize_20200422;--2,521,736

--signed in
 SELECT count(distinct hashed_id) FROM bbc_bitesize_20200422 WHERE acorn_category_description IS NOT NULL;
-- scorn value given (signed in + personalisation on)  = 296,388
-- other (signed out or personalisation off) = 2,225,348

with users as ( SELECT distinct hashed_id,nation FROM bbc_bitesize_20200422 )
SELECT nation, count(distinct hashed_id) from users GROUP BY 1 ORDER BY 2 desc ;

/*
nation,     count
null,       2,224,778
England,    264,135
Scotland,   17,556
Wales,      10,462
Northern Ireland,   4,805

 */


--hours in the day
SELECT date_part(hour,event_datetime_min) as hour, count(*)
FROM bbc_bitesize_20200422
GROUP BY 1 ORDER BY 1;


-- location
with users as ( SELECT distinct hashed_id,location FROM bbc_bitesize_20200422 )
SELECT location, count(distinct hashed_id) from users GROUP BY 1 ORDER BY 2 desc ;
/*
location,               count
null,                   2,147,316
gb (Great Britain),       369,526
je (Jersey),                  455
im (Isle of Man),             412

 */
DROP TABLE sample_users;
CREATE TEMP TABLE sample_users as
    SELECT distinct audience_id FROM bbc_bitesize_20200520 LIMIT 1000;


select DISTINCT length(audience_id),
       audience_id,
       FNV_HASH(audience_id) as hashed_id,
       length(hashed_id)
FROM bbc_bitesize_20200520
WHERE audience_id IN (SELECT audience_id FROM sample_users)
ORDER BY 2
LIMIT 1000;


SELECT * FROM bbc_iplayer_bitesize_20200520 LIMIT 10;

DROP TABLE IF EXISTS bbc_bitesize_20200422;

CREATE TABLE bbc_bitesize_20200422 AS
    SELECT DISTINCT FNV_HASH(audience_id) as hashed_id,
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
    WHERE dt = '20200422'
      AND destination = 'PS_BITESIZE'
      AND length(audience_id) != 43

    UNION
    SELECT DISTINCT FNV_HASH(audience_id) as hashed_id,
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
    WHERE date_of_event = '2020-04-22'
      AND destination = 'PS_BITESIZE';

SELECT * FROM bbc_bitesize_20190522 LIMIT 10;